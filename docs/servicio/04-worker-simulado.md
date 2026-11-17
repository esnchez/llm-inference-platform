# 4. Worker simulado (`internal/simworker`)

El proyecto arranca sin GPU. `simworker.Worker` imita lo suficiente el comportamiento de un servidor de inferencia como para probar el control plane (admisión, streaming, cancelación, concurrencia) con miles de peticiones locales.

## Qué simula y qué no

| Aspecto de una GPU con vLLM | Simulación | Fidelidad |
| :--- | :--- | :--- |
| Memoria de KV-Cache limitada | `TokenBudget` en tokens | Alta en concepto; ver diferencias en [06](06-relacion-con-vllm.md). |
| Prefill del prompt (TTFT) | `BaseTTFT + PromptTokens × PrefillPerToken` | Aproximada: lineal en el tamaño del prompt. |
| Decode (un token por paso) | `InterTokenLatency × (1 + ITLLoadFactor × (leases activos − 1))` | Media: la ITL crece con la carga, de forma lineal e inventada (se calibra en la Fase 6). |
| Abortar una petición y liberar sus bloques | `ctx.Done()` → retorno inmediato → liberación | Alta. |
| Longitud de salida | Exponencial con media `MeanOutputTokens`, acotada por `MaxNewTokens` y determinista por ID; `finish_reason: stop` si para antes | Media: la forma de la distribución es una suposición. |
| Contenido | Palabras falsas deterministas | Irrelevante para el control plane. |
| *Continuous batching*, *preemption*, *prefix caching* | No se simulan | — |

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
}
```

`New` valida que haya `ID`, al menos un modelo, `TokenBudget > 0`, duraciones no negativas y que `MeanOutputTokens` e `ITLLoadFactor` no sean negativos. Estos dos valen 0 por defecto (el comportamiento de la Fase 1, que usan los tests de contrato); el binario los activa con `-mean-output-tokens 200` e `-itl-load-factor 0.05`.

Con los valores por defecto del binario (`ttft=50ms`, `prefill-per-token=20µs`, `itl=20ms`), un solo lease activo y contando los tokens realmente generados:

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
ttft = BaseTTFT + PromptTokens × PrefillPerToken
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
- Al liberar la reserva (`free`), el worker emite `CapacityChanged()` con un envío no bloqueante sobre un canal de buffer 1: muchas liberaciones seguidas producen una sola señal.

## Lo que el simulador permite validar hoy

- **Admisión y cola:** con presupuestos pequeños (`-token-budget 600`) las peticiones esperan en la cola; `/debug/scheduler` muestra la espera y los `503` por cola llena o timeout. `task loadgen` genera tráfico realista para comparar configuraciones.
- **Reparto:** `/debug/workers` muestra cómo el round-robin reparte leases (`leases_granted`, `leases_rejected`).
- **Cancelación:** con `-itl 1s` y un `curl` interrumpido se ve cómo `tokens_reserved` vuelve a 0 al instante.
- **Concurrencia:** los tests lanzan cientos de goroutines contra un worker y verifican que no se pierde ni se sobrepasa capacidad (ver [07](07-concurrencia-y-tests.md)).

## Cómo hacerlo más realista (si hiciera falta)

Las dos primeras se implementaron en la Fase 2 ([08 · Decisión 1](08-fase-2-decisiones.md#decisión-1-realismo-del-worker-simulado)) y están descritas arriba: sin ellas, ninguna política de cola se podía evaluar. La tercera será necesaria para la Fase 3.

- **Salida de longitud variable** (parar antes de `MaxNewTokens` con `finish_reason: stop`): haría visible la infrautilización de la reserva pesimista (FI-1), hoy oculta porque reserva y uso coinciden.
- **ITL dependiente de la carga** (por ejemplo `ITL × (1 + α·leases_activos)`): imitaría el efecto del *continuous batching*, donde más peticiones simultáneas suben el throughput total pero empeoran la latencia de cada una.
- **Caché de prefijos simulada:** reducir el TTFT si el prefijo ya se vio en ese worker. Será necesario para medir el router por prefijos de la Fase 3 sin GPU.
