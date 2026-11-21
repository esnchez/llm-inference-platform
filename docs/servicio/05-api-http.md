# 5. API HTTP (`internal/httpapi`)

## Por qué compatible con OpenAI

vLLM expone una API compatible con OpenAI, y herramientas como `llmperf` (benchmark previsto en la Fase 6), los SDK de OpenAI y la mayoría de clientes LLM hablan ese formato. Usarlo desde el día uno permite:

- Apuntar cualquier cliente existente a la plataforma sin adaptadores.
- Comparar en la Fase 6 "router inteligente" contra "vLLM directo" con la misma herramienta y el mismo tráfico.
- Que el futuro `VLLMAdapter` sea casi un *passthrough* del mismo formato.

Solo se implementa el subconjunto que el control plane necesita hoy.

## Endpoints

| Método y ruta | Respuesta |
| :--- | :--- |
| `POST /v1/chat/completions` | Inferencia. SSE si `stream: true`; JSON si no. |
| `GET /v1/models` | `{"object":"list","data":[{"id":"sim-llama-3-8b","object":"model","owned_by":"llm-inference-platform"}]}` |
| `GET /debug/workers` | Array de `WorkerSnapshot` (presupuesto, reservado, leases activos, concedidos, rechazados y, con caché de prefijos, `prefix_cache_blocks`, `prefix_cache_used`, `prefix_blocks_queried` y `prefix_blocks_hit`). |
| `GET /debug/scheduler` | Estado de la admisión: peticiones y tokens en cola por modelo, límite de la cola, edad de la cabeza, admitidas (directas y desde la cola), rechazos por motivo, despertares con barrera y tiempo en cola p50/p99. Con `-placement prefix`, un bloque `placement` con los contadores de afinidad y el tamaño del radix tree ([09 · Métricas](09-fase-3-routing-por-prefijos.md#métricas)). |
| `GET /healthz` | `200 ok` (liveness). |
| `GET /metrics` | Métricas en formato Prometheus (Fase 5). Lo sirve `cmd/platform`; catálogo en [12 · Observabilidad](12-observabilidad.md#catálogo-de-métricas). |

`/debug/workers` incluye desde la Fase 4 un objeto `status` por worker: `state` (`active`/`draining`), `healthy`, `breaker` (`closed`/`open`/`half-open`), `degraded`, `itl_ms`, `in_flight`, `failures`, `breaker_opens` e `instance_id`. `/debug/scheduler` añade `worker_failures`, `ttft_timeouts`, `stall_timeouts`, `retries`, `retry_successes`, `breaker_opens`, `restarts_detected` y `rejected_no_healthy_workers`.

Las rutas usan los patrones con método de `net/http` (Go ≥ 1.22): un `GET` a `/v1/chat/completions` devuelve `405` automáticamente.

## Petición

```json
{
  "model": "sim-llama-3-8b",
  "messages": [
    {"role": "system", "content": "Eres un asistente."},
    {"role": "user", "content": "Hola"}
  ],
  "max_tokens": 64,
  "stream": true,
  "stream_options": {"include_usage": true}
}
```

| Campo | Uso |
| :--- | :--- |
| `model` | Obligatorio. Debe servirlo algún worker, si no `404`. |
| `messages` | Obligatorio, no vacío. Cada mensaje con `role` y `content` **de texto** (un `content` en partes, multimodal, → `400`); `name` opcional. Se reenvían al motor tal cual y, aplanados, sirven para estimar tokens y para los hashes de prefijo. |
| `max_completion_tokens` / `max_tokens` | Por este orden de preferencia; si no vienen, 256. Rango 1..`max-tokens-limit`. |
| `stream` | `true` → SSE. |
| `stream_options.include_usage` | Añade un chunk final con `usage`. Con un motor real, `usage` lleva los tokens que contó el motor (FI-8); con el simulador, la estimación y los chunks. |
| `n`, `best_of` (≠ 1), `logprobs` (`true`), `top_logprobs`, `prompt_logprobs`, `tools`, `tool_choice`, `functions`, `function_call` | **`400`** con la lista de campos y el motivo: la plataforma no sabría reservar capacidad para varias respuestas ni devolver logprobs o llamadas a herramientas (Fase 6a). `null` equivale a ausente. |
| Cualquier otro (`temperature`, `top_p`, `stop`, `seed`, `presence_penalty`, `response_format`, `top_k`, `min_p`…) | Se **reenvía al motor sin interpretar** (Fase 6a). El simulador los ignora; vLLM los aplica o responde `400` si no los conoce. |

Campos aceptados por OpenAI pero **ignorados** hoy: `temperature`, `top_p`, `stop`, `n`, `tools`, etc. El decodificador JSON no es estricto, así que no provocan error. Cuando exista el adaptador de vLLM habrá que reenviarlos (ver [06](06-relacion-con-vllm.md)).

## Respuesta en streaming (SSE)

El `200` y las cabeceras SSE se envían **con el primer token**, no al admitir la petición (Fase 4). Si el worker falla antes, la petición se reintenta en otro sin que el cliente lo note; si no hay alternativa, el cliente recibe un error normal (`503 worker_unavailable` con `Retry-After`) en lugar de un stream roto.

```text
data: {"id":"chatcmpl-…","object":"chat.completion.chunk","created":1794154380,"model":"sim-llama-3-8b","choices":[{"index":0,"delta":{"role":"assistant","content":"token"},"finish_reason":null}]}

data: {"id":"chatcmpl-…","object":"chat.completion.chunk",…,"choices":[{"index":0,"delta":{"content":" budget"},"finish_reason":null}]}

data: {"id":"chatcmpl-…","object":"chat.completion.chunk",…,"choices":[{"index":0,"delta":{"content":" the"},"finish_reason":"length"}]}

data: {"id":"chatcmpl-…","object":"chat.completion.chunk",…,"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":3,"total_tokens":6}}

data: [DONE]
```

- Un evento por token, con *flush* inmediato.
- El primer chunk incluye `delta.role`.
- El último chunk de contenido incluye `finish_reason`. OpenAI y vLLM suelen enviarlo en un chunk aparte con `delta` vacío; ambas formas son válidas para los clientes.
- `usage.prompt_tokens` es la **estimación** (FI-3) y `completion_tokens` es el número de chunks emitidos.
- Cabeceras: `Content-Type: text/event-stream`, `Cache-Control: no-cache`, `Connection: keep-alive`, `X-Accel-Buffering: no`, `X-Request-Id`, `X-Worker-Id`.

### Errores a mitad de stream

Con el `200` ya enviado (es decir, después del primer token), un fallo del worker se notifica dentro del stream y no se reintenta, como hacen los servidores OpenAI-compatibles, seguido de `[DONE]`:

```text
data: {"error":{"message":"…","type":"server_error"}}

data: [DONE]
```

Si el motivo es que el cliente se fue, no se escribe nada más.

## Respuesta sin streaming

```json
{
  "id": "chatcmpl-…",
  "object": "chat.completion",
  "created": 1794154380,
  "model": "sim-llama-3-8b",
  "choices": [{"index": 0, "message": {"role": "assistant", "content": "token budget the"}, "finish_reason": "length"}],
  "usage": {"prompt_tokens": 3, "completion_tokens": 3, "total_tokens": 6}
}
```

Lleva la cabecera `X-Worker-Id`. Internamente consume el mismo stream que la variante SSE.

## Errores HTTP

Salvo el `405`, todos usan el formato de OpenAI:

```json
{"error": {"message": "no worker has free capacity, retry later", "type": "server_overloaded", "code": "at_capacity"}}
```

| HTTP | `type` / `code` | Causa |
| :--- | :--- | :--- |
| `400` | `invalid_request_error` | JSON inválido, cuerpo mayor de 4 MB, sin `model`/`messages`, `max_tokens` fuera de rango. |
| `400` | `invalid_request_error` / `context_length_exceeded` | `prompt + max_tokens` supera el presupuesto de **todos** los workers del modelo: no cabrá nunca. |
| `404` | `invalid_request_error` / `model_not_found` | Ningún worker sirve el modelo. |
| `405` | — (texto plano de `net/http`) | Método incorrecto en una ruta conocida. |
| `503` | `server_overloaded` / `queue_full` + `Retry-After` | La cola del modelo está llena (límite por tokens) o se alcanzó el tope global de peticiones en espera. |
| `503` | `server_overloaded` / `queue_timeout` + `Retry-After` | La petición esperó en la cola el máximo (5 s o el deadline del cliente) sin que se liberase capacidad. |
| `503` | `server_overloaded` / `at_capacity` + `Retry-After` | Se mantiene en el mapa, pero el scheduler de la Fase 2 ya no lo devuelve. |
| `503` | `server_error` / `no_healthy_workers` + `Retry-After` | Hay workers que sirven el modelo, pero ninguno está sano ni con el breaker cerrado (Fase 4). No se encola. |
| `503` | `server_error` / `worker_unavailable` + `Retry-After` | El worker falló antes del primer token y el reintento también falló o no había otro worker (Fase 4). |
| `500` | `server_error` | Error inesperado antes del primer token. |

**`503` frente a `400`:** `503` significa "ahora no, reintenta" y `400` "nunca". La diferencia está en el scheduler: `ErrRequestTooLarge` solo se devuelve si la petición no cabe en ninguno de los workers del modelo ni vacíos; si cabe pero ahora no hay hueco, espera en la cola y solo recibe `503` si la cola está llena o no entra a tiempo.

**`Retry-After`:** lo calcula el scheduler a partir de la espera reciente en cola (media exponencial), acotada entre 1 s y 30 s, más hasta 1 s de *jitter* para que los clientes no reintenten sincronizados. Llega en un `inference.RetryAfterError`; `Config.RetryAfter` (1 s) solo se usa si el error no trae valor.

## Administración de workers simulados (Fase 4)

Los sirve `cmd/platform` (no `httpapi`), porque crean y rompen workers **simulados**:

| Método y ruta | Efecto | Respuesta |
| :--- | :--- | :--- |
| `POST /debug/workers` | Crea y registra un worker nuevo (`sim-worker-N`) con la configuración de los flags. | `201 {"worker_id": …}` |
| `DELETE /debug/workers/{id}` | Baja con drenaje: no recibe más peticiones y desaparece de `/debug/workers` cuando termina su último stream. | `202 {"state": "draining"}`; `404` si no existe |
| `POST /debug/workers/{id}/faults` | Sustituye sus fallos inyectados por el cuerpo JSON (campos de [04 · Fallos simulados](04-worker-simulado.md#fallos-simulados-fase-4)); `{}` los quita. Campos desconocidos → `400`. | `200` con los fallos vigentes |
| `POST /debug/workers/{id}/restart` | Simula un reinicio: corta sus streams, vacía su caché y cambia su `InstanceID`. | `200` |

## Configuración (`httpapi.Config`)

| Campo | Defecto | Efecto |
| :--- | :--- | :--- |
| `DefaultMaxTokens` | 256 | `max_tokens` si el cliente no lo envía. |
| `MaxTokensLimit` | 4096 | Máximo aceptado (flag `-max-tokens-limit`). |
| `RetryAfter` | 1 s | Valor de reserva de la cabecera si el scheduler no aporta uno. |
| `StreamBuffer` | 16 | Buffer del canal worker → handler. |
| `MaxBodyBytes` | 4 MB | Tamaño máximo del cuerpo. |
| `Observer` | ninguno | Recibe un registro por petición (`outcome`, TTFT, tokens) para las métricas (Fase 5). |

**`X-Request-Id`** (Fase 5): si el cliente lo envía con 1–64 caracteres `[A-Za-z0-9._-]`, se usa como ID de la petición (logs, cabecera de respuesta y `id` de los chunks); si no, se genera uno (`chatcmpl-…`). Se devuelve en todas las respuestas, también en las de error.

## Ejemplos

```bash
# Streaming
curl -N localhost:8080/v1/chat/completions -H 'Content-Type: application/json' -d '{
  "model":"sim-llama-3-8b","stream":true,"max_tokens":16,
  "messages":[{"role":"user","content":"Hola"}]}'

# Forzar 503: servidor con poco presupuesto y varias peticiones largas en paralelo
task run -- -workers 1 -token-budget 600
for i in 1 2 3; do curl -s -o /dev/null -w "%{http_code}\n" localhost:8080/v1/chat/completions \
  -d '{"model":"sim-llama-3-8b","max_tokens":500,"messages":[{"role":"user","content":"hi"}]}' & done; wait

# Estado de los workers
curl -s localhost:8080/debug/workers

# Estado de la cola
curl -s localhost:8080/debug/scheduler

# Carga realista (Poisson, chat + RAG, cancelaciones) y resumen de resultados
task loadgen -- -rate 30 -duration 10s

# Routing por prefijos: 24 system prompts compartidos (añadir -prefix-skew 1.5 para unos pocos calientes).
# Comparar con el servidor arrancado con -placement prefix y con -placement round-robin.
task loadgen -- -rate 4 -duration 60s -prefixes 24 -rag-fraction 0 -cancel-fraction 0

# Resiliencia: tumbar un worker, ver cómo lo aíslan y recuperarlo
curl -s -X POST localhost:8080/debug/workers/sim-worker-1/faults -d '{"down": true}'
curl -s localhost:8080/debug/workers        # status.healthy=false, status.breaker=open (~6 s después)
curl -s -X POST localhost:8080/debug/workers/sim-worker-1/faults -d '{}'
task loadgen -- -rate 4 -duration 45s -chaos hang   # lo mismo, automatizado, durante una prueba de carga
```

Los mensajes `system` cuentan para el prefijo igual que el resto: el prompt se aplana en orden (`role: content\n`) y se hashea por bloques de 64 caracteres. Dos peticiones con el mismo `system` (y el mismo modelo) comparten todos los bloques completos de ese mensaje.
