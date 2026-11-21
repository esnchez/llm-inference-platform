# 6. Relación con los workers vLLM

En producción, cada worker sería un servidor **vLLM** con una GPU. Este documento explica qué representa cada pieza simulada, qué hace vLLM por su cuenta, cómo encaja el `VLLMAdapter` (Fase 6a, verificado contra vLLM real en CPU en la 6b-CPU) y qué huecos del contrato de la Fase 1 hubo que cerrar.

> Los nombres de flags y métricas de vLLM cambian entre versiones (motor V0 frente a V1). La versión fijada es la **0.34**, y los nombres que usa el adaptador están comprobados contra ella (cierra **FI-4**).

## Reparto de responsabilidades

| Responsabilidad | Plataforma (control plane, Go) | vLLM (data plane, por GPU) |
| :--- | :--- | :--- |
| Elegir a qué GPU va cada petición | ✅ `scheduler` (afinidad de prefijo desde la Fase 3; round-robin opcional) | ❌ solo conoce su propia GPU |
| Rechazar pronto cuando el clúster está lleno (`503`) | ✅ leases + admisión | ⚠️ encola internamente sin límite útil para el cliente |
| Agrupar peticiones en batches (*continuous batching*) | ❌ | ✅ |
| Gestionar la memoria KV-Cache por bloques (*PagedAttention*) | ❌ solo la contabiliza (aproximadamente) | ✅ |
| Reutilizar prefijos ya calculados (*prefix caching*) | ✅ Fase 3: enviar la petición al worker que ya los tiene (radix tree aproximado) | ✅ dentro de su GPU |
| Tokenizar y aplicar la *chat template* del modelo | ❌ solo estima para admitir (FI-3); la respuesta usa los tokens reales del motor | ✅ |
| Abortar y liberar memoria al cancelar | ✅ propaga la cancelación | ✅ libera los bloques |
| Detectar GPUs caídas o colgadas y desviar el tráfico | ✅ Fase 4: healthchecks, *circuit breaker*, timeouts de cuelgue, reintento antes del primer token | ⚠️ solo informa (`/health`) de su propio proceso |
| Kernels CUDA, ejecución del modelo | ❌ (no objetivo) | ✅ |

En una frase: **vLLM optimiza una GPU; la plataforma optimiza el clúster de GPUs.** vLLM no sabe que existen otras réplicas, así que no puede decidir que una petición iría mejor a otra GPU que ya tiene su prefijo cacheado, ni rechazar pronto porque todo el clúster está saturado.

## Correspondencia entre conceptos

| Simulación | Equivalente en vLLM |
| :--- | :--- |
| `simworker.Worker` | Un proceso `vllm serve <modelo>` en una GPU (en K8s, un pod). |
| `Config.Models` | El modelo cargado por ese servidor (normalmente uno por proceso). |
| `TokenBudget` | Capacidad total de KV-Cache: nº de bloques × tamaño de bloque (`--block-size`, 16 tokens por defecto). Depende de la VRAM libre tras cargar los pesos (`--gpu-memory-utilization`). El adaptador la **descubre** de `vllm:cache_config_info` (`num_gpu_blocks × block_size`); en CPU el bloque es de 128 tokens. |
| `TokensReserved / TokenBudget` | Métrica de ocupación de KV-Cache (`vllm:kv_cache_usage_perc` en V1, `vllm:gpu_cache_usage_perc` en V0). |
| `ActiveLeases` | `vllm:num_requests_running` (+ `vllm:num_requests_waiting`). |
| TTFT simulado (`BaseTTFT + prompt × PrefillPerToken`) | Fase de *prefill*: el modelo procesa todo el prompt en paralelo y llena la KV-Cache. |
| ITL simulada | Fase de *decode*: un paso del modelo por token, compartido con todo el batch. |
| `PrefixHashes` + caché de `simworker` (Fase 3) | *Automatic prefix caching* (`--enable-prefix-caching`, activo por defecto en V1): bloques de `--block-size` tokens identificados por un hash del bloque y de su prefijo. vLLM hashea IDs de token tras la *chat template*; nosotros, texto aplanado. |
| `prefix.Tree` en el router | No existe en vLLM: es la vista del clúster que ninguna GPU tiene. |
| `ctx.Done()` → liberación | Al cerrar la conexión HTTP, el servidor de vLLM aborta la petición y libera sus bloques. |
| `AcquireLease` atómico | **No existe en vLLM.** Ver abajo. |
| `Worker.Health` / `HealthStatus.InstanceID` (Fase 4) | `GET /health` (200 si el motor está vivo). vLLM no expone un identificador de instancia: el adaptador lo deriva de `process_start_time_seconds` de `/metrics` o, si falta, de un contador que baja (Fase 6a). |
| `ErrWorkerFailure` (Fase 4) | Lo que el adaptador envuelve: conexión rechazada, `5xx`, stream SSE cortado. **No** un `400` (p. ej. `--max-model-len`), que es un error de la petición. |
| `ErrAtCapacity` | No hay equivalente directo: vLLM acepta y encola, o expulsa (*preempt*) peticiones si se queda sin bloques. |

## Lo que la simulación no captura (y por qué importa)

1. **vLLM no reserva el peor caso.** Asigna bloques a medida que la secuencia crece. Si se queda sin memoria, expulsa peticiones en curso (las recalcula más tarde), lo que dispara la latencia. Nuestra reserva pesimista evita llegar a ese punto, a costa de admitir menos de lo que cabría (**FI-1**).
2. **El prefix caching hace que el coste real sea menor que el contabilizado.** Si 50 peticiones comparten un *system prompt* de 2000 tokens, vLLM guarda esos bloques una sola vez; nosotros los contamos 50 veces (**FI-1**). Con el routing por afinidad de la Fase 3 esas peticiones se concentran en el mismo worker, así que la sobrestimación es mayor ([10 · Interacción con FI-1](10-fase-3-decisiones.md#interacción-con-fi-1-la-reserva-pesimista)).
3. **La ITL depende del batch.** Con más peticiones simultáneas, cada paso de decode sirve a más secuencias: sube el throughput total pero cada petición va más lenta. El simulador lo imita con una ITL que crece linealmente con los leases activos (`-itl-load-factor`, Fase 2), una forma inventada y sin calibrar (ver [04](04-worker-simulado.md)).
4. **Los tokens reales salen del tokenizer del modelo**, no de "4 caracteres por token" (**FI-3**).

## El `VLLMAdapter` (Fase 6a)

**Implementado en la Fase 6a** (`internal/vllm`, decisiones en [13](13-fase-6a-decisiones.md)) y **verificado contra vLLM 0.34 real en CPU** en la Fase 6b-CPU ([14](14-fase-6b-cpu-decisiones.md)): los formatos que suponía el falso (SSE, `usage`, errores, `/v1/models`, nombres de métricas) coinciden, y las respuestas reales están en `internal/vllm/testdata/vllm-0.34.0/` como *fixtures* de los tests. Diferencias encontradas y cómo quedaron: vLLM recorta `max_tokens` en vez de rechazar (la plataforma ahora hace lo mismo); en CPU el bloque de KV-Cache es de 128 tokens (el router no lo tiene en cuenta: limitación documentada); la *chat template* añade tokens que la estimación no cuenta (FI-3); y `request_success_total{finished_reason="abort"}` no cuenta las desconexiones del cliente. Implementa `inference.Worker` sobre el ledger compartido:

```mermaid
sequenceDiagram
    participant API as httpapi
    participant A as VLLMAdapter (Go)
    participant V as vLLM (GPU)

    API->>A: AcquireLease(req)
    Note over A: contabilidad local en el control plane:<br/>reserved + coste ≤ presupuesto (FI-2)
    A-->>API: Lease
    API->>A: ExecuteStream(ctx, out)
    A->>V: POST /v1/chat/completions {stream:true}<br/>(petición HTTP ligada a ctx)
    loop chunks SSE de vLLM
        V-->>A: data: {...delta...}
        A-->>API: TokenChunk
    end
    alt el cliente se desconecta
        API->>A: ctx cancelado
        A--xV: cierra la conexión HTTP
        Note over V: vLLM aborta la petición<br/>y libera sus bloques KV
    end
    A->>A: libera la reserva local
```

| Método | Implementación |
| :--- | :--- |
| Descubrimiento (`Connector`) | Antes de registrarse: `GET /v1/models` (modelos y `max_model_len`) y `GET /metrics` (`vllm:cache_config_info` → presupuesto = `num_gpu_blocks × block_size`). Reintenta con *backoff* mientras vLLM carga el modelo y redescubre cada 30 s. |
| `AcquireLease` | El ledger compartido (`internal/ledger`): vLLM no ofrece una API para reservar. La atomicidad depende de que esta réplica sea la única puerta de entrada a ese vLLM (**FI-2**). Límite por petición = `max_model_len`. |
| `ExecuteStream` | `POST /v1/chat/completions` con los `messages`, los parámetros reenviados y los campos controlados (`max_tokens`, `stream`, `stream_options.include_usage`), ligado a `ctx`: cancelar cierra la conexión y vLLM aborta. El SSE se traduce a `TokenChunk`; el último lleva `finish_reason` y el `usage` del motor. |
| Errores | Red y `5xx`/`404`/`401` → `ErrWorkerFailure`; `400` de longitud → `ErrRequestTooLarge`; otro `400` → `ErrInvalidRequest` ([13 · Decisión 6](13-fase-6a-decisiones.md#decisión-6-traducción-de-errores)). |
| `Health` | `GET /health` y, en la misma consulta, `GET /metrics`: reinicios (`process_start_time_seconds`, o un contador que baja), acierto real de la caché de prefijos y ocupación real de la KV-Cache (`WorkerSnapshot.Engine`). |
| `Release`, `CapacityChanged` | Los del ledger, idénticos a los del simulador. |

`httpapi` y `scheduler` no cambiaron para hablar con vLLM: `main` crea un `Connector` por cada URL de `-vllm`, y desde la Fase 7 un `DNSWatcher` por cada nombre de `-vllm-dns`, que arranca un `Connector` por IP. Simulados y vLLM pueden convivir en el mismo pool.

### Dónde corre el adaptador y qué cruza la red

El `VLLMAdapter` es un **objeto Go dentro del proceso del control plane**, no un proceso en el nodo de la GPU. El [ledger](03-leases-y-capacidad.md#el-ledger-de-capacidad) de cada vLLM vive en él.

| Pieza | Dónde corre | Qué cruza la red |
| :--- | :--- | :--- |
| Scheduler | Proceso del control plane | Nada |
| `VLLMAdapter` (ledger, señal de capacidad) | Mismo proceso | Nada: las señales son canales Go |
| vLLM | Nodo GPU | La petición HTTP, su stream SSE y, en cada healthcheck, `/metrics` y `/health` |

El adaptador sabe que una petición terminó **porque se cierra el stream SSE que está reenviando**; no necesita ningún mensaje adicional de vLLM para liberar su reserva. Lo que vLLM haga con sus bloques de KV-Cache (asignar, liberar, expulsar del LRU de *prefix caching*) no llega al adaptador ni afecta a la admisión. En vLLM, la expulsión LRU ocurre *al asignar* memoria a otra petición, así que no indica capacidad nueva.

Para la información que vive en vLLM, la regla es **leer estado agregado periódicamente** o consumir eventos agrupados fuera del camino crítico, nunca un evento por bloque. El adaptador lee `/metrics` en cada healthcheck (cada 2 s): ocupación real de la KV-Cache, acierto real de su caché de prefijos y reinicios. La ocupación real se muestra pero **no se usa para admitir**: relajar FI-1 con ella sigue pendiente. Los eventos de KV-Cache no se usan: el árbol del router predijo el acierto real a menos de un punto (6b-CPU). Ver [09 · Decisión 2](09-fase-2-decisiones.md#decisión-2-cómo-se-entera-el-scheduler-de-que-hay-capacidad-libre).

## Huecos del contrato que el adaptador tenía que cerrar

Eran consecuencias del contrato de la Fase 1. **Los tres primeros están cerrados** (FI-7 y FI-8 en la Fase 6a, FI-9 en la Fase 4 y 6a); el texto original se conserva como registro.

1. ~~**`InferenceRequest` solo lleva el prompt aplanado.**~~ **Resuelto en la 6a:** `Messages` y `Params` (con campos controlados y rechazados). `renderPrompt` convierte los mensajes en `role: content\n` únicamente para estimar el coste. vLLM necesita los `messages` originales para aplicar la *chat template* del modelo, y también los parámetros de muestreo que hoy se ignoran (`temperature`, `top_p`, `stop`, …). El contrato tendrá que transportar los mensajes y esos parámetros, o el cuerpo original de la petición (**FI-7**).
2. ~~**Un `TokenChunk` no siempre es exactamente un token.**~~ **Resuelto en la 6a:** `TokenChunk.Usage` con los tokens del motor; `Index` es índice de chunk. vLLM normalmente emite un token por evento, pero puede agrupar varios (por ejemplo, al retener bytes de un carácter UTF-8 incompleto, o con *speculative decoding*). Hoy `completion_tokens` se calcula contando chunks; con vLLM debería tomarse del `usage` que devuelve el propio motor (**FI-8**).
3. ~~**Las cabeceras `200` se envían antes de que el worker responda.**~~ **Resuelto en la Fase 4:** el handler espera al primer chunk para enviar el `200`, así que un rechazo de vLLM al empezar llega al cliente con su código (**FI-9**). Para que sea un `400` y no un `503`, el adaptador debe traducir el `400` de vLLM a `ErrRequestTooLarge` o `ErrInvalidRequest`, nunca a `ErrWorkerFailure`.
4. **`PrefixHashes` no le sirve a vLLM.** El adaptador no lo envía: vLLM calcula sus propios hashes sobre los tokens reales. El router los sigue usando para su árbol. Si la *chat template* del modelo insertara texto variable al principio del prompt (fechas, IDs), rompería en vLLM prefijos que el router cree compartidos ([10 · Limitaciones](10-fase-3-decisiones.md#limitaciones-y-trabajo-futuro)). Con el modelo de la 6b-CPU (Qwen2.5-0.5B) no ocurrió: el acierto previsto y el real coincidieron (75,7 % y 75,0 %). Hay que volver a comprobarlo con cada modelo.
5. **El `finish_reason` real** puede ser `stop` (fin natural, *stop sequence*) además de `length`; el tipo `FinishReason` ya lo contempla.

## Por qué el diseño se mantuvo con vLLM

- **Streaming-first:** vLLM responde en SSE con el mismo formato OpenAI; el adaptador traduce evento a evento sin cambiar de modelo mental.
- **Cancelación propagada:** el camino `ctx.Done()` que libera los tokens simulados es el mismo que cierra la conexión con vLLM; verificado en la 6b-CPU: cancelar detiene la generación en el motor.
- **Admisión antes de responder:** aunque vLLM no reserve, el control plane sigue evitando enviar trabajo a GPUs saturadas, que es lo que provoca las expulsiones (*preemption*) y el *thrashing* de la KV-Cache descritos en §4.2 del README raíz.
- **Interfaces:** el simulador y el adaptador son intercambiables, así que los tests del control plane siguen siendo válidos y se pueden comparar ambos con el mismo benchmark.
