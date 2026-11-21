# 4. Worker simulado (`internal/simworker`)

El proyecto arranca sin GPU. `simworker.Worker` imita lo suficiente el comportamiento de un servidor de inferencia como para probar el control plane (admisión, streaming, cancelación, concurrencia) con miles de peticiones locales.

Desde la Fase 6a, la contabilidad (presupuesto, leases, TTL, `CapacityChanged`) vive en `internal/ledger`, compartida con el adaptador de vLLM; este paquete solo simula cómo se ejecuta un stream. El simulador ignora `Messages` y `Params` (sus salidas son palabras falsas) y no informa `Usage`: la API cuenta chunks, como antes. Para probar el camino HTTP sin GPU está el **vLLM falso** (`internal/vllm/fakevllm`, [13](13-fase-6a-decisiones.md#decisión-9-cómo-se-prueba)), que es otra cosa: imita la API de vLLM, no una GPU.

## Qué simula y qué no

| Aspecto de una GPU con vLLM | Simulación | Fidelidad |
| :--- | :--- | :--- |
| Memoria de KV-Cache limitada | `TokenBudget` en tokens | Alta en concepto; ver diferencias en [06](06-relacion-con-vllm.md). |
| Prefill del prompt (TTFT) | `BaseTTFT + PrefillPerToken × tokens de prompt no cacheados` | Aproximada: lineal en el tamaño del prompt. |
| *Prefix caching* (Fase 3) | LRU de bloques de 16 tokens identificados por `PrefixHashes`; solo se reutilizan los bloques iniciales consecutivos | Media: la caché es aparte del presupuesto (no compite con los leases) y no protege los bloques en uso. |
| Decode (un token por paso) | `InterTokenLatency × (1 + ITLLoadFactor × (leases activos − 1))` | Media: la ITL crece con la carga, de forma lineal e inventada (se calibra en la Fase 6). |
| Abortar una petición y liberar sus bloques | `ctx.Done()` → retorno inmediato → liberación | Alta. |
| Longitud de salida | Exponencial con media `MeanOutputTokens`, acotada por `MaxNewTokens` y determinista por ID; `finish_reason: stop` si para antes | Media: la forma de la distribución es una suposición. |
| Contenido | Palabras falsas deterministas | Irrelevante para el control plane. |
| Fallos (Fase 4) | `Faults`: errores al empezar, caída, cuelgue, corte o parada a mitad de stream, lentitud y reinicio | Media: cubre los fallos típicos de un servidor de inferencia, no su frecuencia real. |
| *Continuous batching*, *preemption* | No se simulan | — |

## Configuración

```go
type Config struct {
    ID                string
    Models            []string
    TokenBudget       int           // capacidad de KV-Cache simulada, en tokens
    BaseTTFT          time.Duration // latencia fija antes del primer token
    PrefillPerToken   time.Duration // coste de prefill por token de prompt
    InterTokenLatency time.Duration // pausa entre tokens
    LeaseTTL          time.Duration // 0 = sin expiración
    MeanOutputTokens  int           // media de la salida; 0 = siempre MaxNewTokens
    ITLLoadFactor     float64       // crecimiento de la ITL por lease activo extra; 0 = constante
    PrefixCacheBlocks int           // caché de prefijos, en bloques de 16 tokens; 0 = sin caché
    Faults            Faults        // fallos inyectados desde el arranque (Fase 4); también SetFaults
}
```

`New` valida que haya `ID`, al menos un modelo, `TokenBudget > 0`, duraciones no negativas y que `MeanOutputTokens`, `ITLLoadFactor` y `PrefixCacheBlocks` no sean negativos. Los tres valen 0 por defecto (el comportamiento de la Fase 1, que usan los tests de contrato); el binario los activa con `-mean-output-tokens 200`, `-itl-load-factor 0.05` y `-prefix-cache-blocks` (por defecto `token-budget/16`, es decir, 1.024 bloques).

Con los valores por defecto del binario (`ttft=50ms`, `prefill-per-token=20µs`, `itl=20ms`), un solo lease activo, sin prefijo cacheado y contando los tokens realmente generados:

| Petición | TTFT | Duración total aproximada |
| :--- | :--- | :--- |
| Prompt de 100 tokens, 100 de salida | 50 ms + 2 ms = 52 ms | 52 ms + 99 × 20 ms ≈ 2,0 s |
| Prompt de 4000 tokens, 100 de salida | 50 ms + 80 ms = 130 ms | ≈ 2,1 s |
| Prompt de 100 tokens, 500 de salida | 52 ms | ≈ 10 s |

Esto refleja una realidad de los LLMs: el **prompt** encarece sobre todo la latencia inicial (prefill, paralelizable), mientras que la **salida** domina la duración total (decode, secuencial, un token por paso). Y como la reserva se mantiene durante toda la generación, `max_tokens` es lo que más tiempo bloquea capacidad.

## Generación (`ExecuteStream`)

```text
begin()                         → valida el estado del lease, para el TTL
defer Release()                 → libera tokens al salir, pase lo que pase
cached = min(bloques iniciales de PrefixHashes ya en caché × 16, PromptTokens)
          → y todos los bloques de la petición pasan a ser los más recientes
ttft = BaseTTFT + (PromptTokens − cached) × PrefillPerToken
n = outputTokens(semilla, MaxNewTokens)    → MaxNewTokens si MeanOutputTokens = 0
para i en 0 .. n-1:
    espera (i == 0 ? ttft : ITL según la carga) ┐ ambos con select sobre ctx.Done():
    envía TokenChunk{i, texto} a out            ┘ si se cancela → return ctx.Err()
    (el último lleva FinishReason = "length", o "stop" si n < MaxNewTokens)
return nil
```

Detalles:

- Se usa **un único `time.Timer`** reutilizado con `Reset` para todas las pausas, en lugar de `time.Sleep` o `time.After` por token. `Sleep` no se puede interrumpir con `ctx`, y `time.After` crearía un temporizador nuevo por token. Con la semántica de temporizadores de Go ≥ 1.23 (versión mínima del módulo), `Reset` sobre un temporizador parado es seguro y no entrega valores antiguos.
- Si una pausa es 0 (por ejemplo en tests), no se crea espera, pero se sigue comprobando `ctx.Err()` en cada token.
- El texto sale de un vocabulario de 16 palabras, indexado a partir de un hash FNV del ID de la petición: la misma petición produce siempre el mismo texto, lo que facilita depurar. La longitud de salida usa la misma semilla, así que también es reproducible.
- La ITL se recalcula en cada token leyendo los leases activos bajo el mutex del worker.
- La caché de prefijos tiene su propio mutex, separado del del ledger: consultarla no bloquea `AcquireLease`.
- Al liberar la reserva (`free`), el worker emite `CapacityChanged()` con un envío no bloqueante sobre un canal de buffer 1: muchas liberaciones seguidas producen una sola señal.

## Caché de prefijos (Fase 3)

Imita el *automatic prefix caching* de vLLM, lo justo para medir el router por prefijos ([09 · Decisión 7](09-fase-3-routing-por-prefijos.md#decisión-7-caché-de-prefijos-en-el-simulador)):

- **Unidad:** bloques de 16 tokens estimados, identificados por los hashes encadenados que calcula el control plane (`InferenceRequest.PrefixHashes`). El worker no hashea nada.
- **Solo prefijos:** cuenta los bloques cacheados **desde el principio** hasta el primer fallo. Un bloque cacheado después de un fallo no sirve, porque su KV depende de todos los anteriores.
- **LRU con el prefijo protegido:** la petición toca sus bloques del último al primero. Así los primeros, los que comparten otras peticiones, quedan como los más recientes y se expulsan los últimos (vLLM libera los bloques de una petición en orden inverso por la misma razón).
- **Fuera del presupuesto:** la caché no descuenta tokens del ledger ni compite con los leases. En vLLM la caché vive en la KV libre, así que el simulador es optimista en ese punto (FI-1).
- **Contadores** en `Snapshot()` y `/debug/workers`: `prefix_cache_blocks` (capacidad), `prefix_cache_used`, `prefix_blocks_queried` y `prefix_blocks_hit`. Su cociente es el acierto **real**, que se compara con el que predice el router.

Con `-prefill-per-token 100us`, un *system prompt* de 1.500 tokens cuesta 150 ms de prefill la primera vez en cada worker y 0 las siguientes.

## Fallos simulados (Fase 4)

Para que los healthchecks, el *circuit breaker* y los reintentos tengan algo que detectar ([10 · Decisión 6](10-fase-4-decisiones.md#decisión-6-fallos-simulados-en-simworker)). Se configuran en `Config.Faults` o en caliente con `SetFaults` (y desde `POST /debug/workers/{id}/faults`). Cada stream lee los fallos **al empezar**: cambiarlos no afecta a los streams en curso.

| Campo (`json`) | Efecto | Imita |
| :--- | :--- | :--- |
| `ErrorRate` (`error_rate`) | Esa fracción de streams falla al empezar con `ErrWorkerFailure`. El sorteo usa la semilla del ID de la petición: es reproducible. | Conexión rechazada, `5xx` |
| `Down` (`down`) | Todo stream y todo `Health` fallan. `AcquireLease` sigue concediendo, porque con vLLM el ledger vive en el control plane. | Proceso caído |
| `Hang` (`hang`) | El stream no emite nada hasta que se cancela. `Health` **sigue respondiendo bien**: solo lo detecta el timeout de TTFT del scheduler. | GPU colgada |
| `StallAfterTokens` (`stall_after_tokens`) | Tras N tokens, deja de emitir sin fallar. | Cuelgue a mitad de decode |
| `FailAfterTokens` (`fail_after_tokens`) | Tras N tokens, falla con `ErrWorkerFailure`. | Conexión cortada a mitad de stream |
| `Slowdown` (`slowdown`) | Multiplica el TTFT y la ITL. | *Throttling*, vecino ruidoso |

`Restart()` simula un reinicio: los streams en curso fallan con `ErrWorkerFailure`, la caché de prefijos se vacía y el `InstanceID` que devuelve `Health` cambia (`sim-worker-1#0` → `sim-worker-1#1`). Los fallos inyectados se mantienen.

## Lo que el simulador permite validar hoy

- **Admisión y cola:** con presupuestos pequeños (`-token-budget 600`) las peticiones esperan en la cola; `/debug/scheduler` muestra la espera y los `503` por cola llena o timeout. `task loadgen` genera tráfico realista para comparar configuraciones.
- **Reparto:** `/debug/workers` muestra cómo la colocación reparte leases (`leases_granted`, `leases_rejected`).
- **Routing por prefijos:** `task loadgen -- -prefixes 24` con `-placement prefix` y con `-placement round-robin` compara TTFT y acierto de caché ([09 · Mediciones](09-fase-3-routing-por-prefijos.md#mediciones)).
- **Cancelación:** con `-itl 1s` y un `curl` interrumpido se ve cómo `tokens_reserved` vuelve a 0 al instante.
- **Resiliencia:** `task loadgen -- -chaos down` (o `hang`, `errors`, `stall`, `slow`, `restart`) rompe un worker durante la prueba; `/debug/workers` muestra su salud y su breaker, y `/debug/scheduler` los reintentos ([10 · Mediciones](10-fase-4-decisiones.md#mediciones-con-loadgen--chaos)).
- **Concurrencia:** los tests lanzan cientos de goroutines contra un worker y verifican que no se pierde ni se sobrepasa capacidad (ver [07](07-concurrencia-y-tests.md)).

## Cómo hacerlo más realista (si hiciera falta)

Las dos primeras se implementaron en la Fase 2 ([08 · Decisión 1](08-fase-2-decisiones.md#decisión-1-realismo-del-worker-simulado)): sin ellas, ninguna política de cola se podía evaluar. La tercera se implementó en la Fase 3 (ver [arriba](#caché-de-prefijos-fase-3)). Las tres están descritas en este documento.

- **Salida de longitud variable** (parar antes de `MaxNewTokens` con `finish_reason: stop`): haría visible la infrautilización de la reserva pesimista (FI-1), hoy oculta porque reserva y uso coinciden.
- **ITL dependiente de la carga** (por ejemplo `ITL × (1 + α·leases_activos)`): imitaría el efecto del *continuous batching*, donde más peticiones simultáneas suben el throughput total pero empeoran la latencia de cada una.
- **Caché de prefijos simulada:** reducir el TTFT si el prefijo ya se vio en ese worker. Hacía falta para medir el router por prefijos de la Fase 3 sin GPU.
- **Pendiente, si hiciera falta:** que la caché comparta memoria con el presupuesto (los bloques cacheados ocupan sitio y se expulsan cuando una petición nueva lo necesita), como en vLLM. Va ligado a FI-1.
