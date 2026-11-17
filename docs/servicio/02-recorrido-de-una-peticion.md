# 2. Recorrido de una petición

Este documento sigue una petición de chat desde que llega hasta que se libera su capacidad. Todo el código está en `internal/httpapi/server.go` (`handleChatCompletions`, `stream`, `collect`) salvo donde se indica.

## Vista general

```mermaid
sequenceDiagram
    autonumber
    participant C as Cliente
    participant API as httpapi.Server
    participant P as scheduler.Scheduler
    participant W as simworker.Worker
    participant L as Lease
    participant G as goroutine<br/>(StartStream)

    C->>API: POST /v1/chat/completions {stream:true}
    API->>API: decodificar JSON (máx. 4 MB) y validar
    API->>API: renderPrompt + tokens.Estimate + ID
    API->>P: Acquire(ctx, req)
    Note over P: si no cabe: espera en la cola del modelo<br/>hasta que el dispatcher le entregue un lease<br/>(o timeout → 503)
    loop workers desde el desplazamiento round-robin
        P->>W: AcquireLease(ctx, req)
        W-->>P: Lease | ErrAtCapacity | ...
    end
    P-->>API: Lease (o error → 4xx/503, fin)
    Note over API: defer lease.Release()
    API->>C: 200 + cabeceras SSE
    API->>G: StartStream(ctx, lease, 16)
    G->>L: ExecuteStream(ctx, out)
    L->>L: espera TTFT (prefill)
    loop cada token
        L-->>G: TokenChunk (canal out)
        G-->>API: chunk
        API->>C: data: {chat.completion.chunk}
        L->>L: espera ITL (decode)
    end
    L->>W: libera tokens (al retornar)
    L-->>G: nil
    G-->>API: canal cerrado + wait() = nil
    API->>C: data: {usage} (opcional)
    API->>C: data: [DONE]
    API->>API: log "request completed"
```

## Fase A — Antes de escribir nada al cliente

### 1. Decodificación y validación

- El cuerpo se limita a `MaxBodyBytes` (4 MB) con `http.MaxBytesReader`.
- JSON inválido → `400`.
- `toInferenceRequest` valida:
  - `model` no vacío y `messages` con al menos un mensaje.
  - `max_tokens`: se toma `max_completion_tokens` si viene, si no `max_tokens`, si no `DefaultMaxTokens` (256). Debe estar entre 1 y `MaxTokensLimit` (4096). Fuera de rango → `400`.

### 2. Construcción del `InferenceRequest`

| Campo | Origen |
| :--- | :--- |
| `ID` | `chatcmpl-` + 24 caracteres hex aleatorios (`crypto/rand`). Se usa en logs, en la cabecera `X-Request-Id` y en cada chunk. |
| `Model` | `model` del cuerpo. |
| `Prompt` | `renderPrompt`: cada mensaje como `role: content\n`. |
| `PromptTokens` | `tokens.Estimate(Prompt)` (~4 caracteres/token, FI-3). |
| `MaxNewTokens` | El `max_tokens` resuelto en el paso 1. |

`renderPrompt` **no** es una *chat template* real: solo sirve para estimar el coste. Ver [06 · Relación con vLLM](06-relacion-con-vllm.md) para lo que implica.

### 3. Admisión: obtener un lease

`s.sched.Acquire(r.Context(), req)` intenta un lease y, si no hay capacidad, espera en la cola del modelo hasta que el dispatcher se lo entregue o venza el timeout (ver [01 · Arquitectura](01-arquitectura.md#internalscheduler--admisión-y-colocación-fase-2)). Durante la espera el cliente no recibe ningún byte. Si falla, `writeAdmissionError` elige la respuesta:

| Error | Respuesta |
| :--- | :--- |
| El cliente ya se fue (`r.Context()` cancelado) | Nada (no hay a quién escribir). |
| `ErrModelNotFound` | `404`, `code: model_not_found` |
| `ErrRequestTooLarge` | `400`, `code: context_length_exceeded` |
| `ErrInvalidRequest` | `400` |
| `ErrQueueFull` | `503`, `code: queue_full`, `Retry-After` calculado por el scheduler |
| `ErrQueueTimeout` | `503`, `code: queue_timeout`, `Retry-After` calculado por el scheduler |
| `ErrAtCapacity` | `503`, `code: at_capacity` (el scheduler de la Fase 2 ya no lo devuelve; se mantiene en el mapa) |
| Otro | `500` |

**Por qué la admisión va antes de las cabeceras:** una vez enviado `200 OK` ya no se puede cambiar el código de estado. Reservar primero permite rechazar con un `503` limpio que clientes, balanceadores y herramientas de benchmark entienden y reintentan. Es la decisión "Leases atómicos" de la tabla §2 del README raíz.

Con el lease concedido:

```go
defer lease.Release()                         // red de seguridad: siempre se libera
ctx, cancel := context.WithCancel(r.Context()) // se cancela si el cliente se va o si falla una escritura
defer cancel()
```

## Fase B — Streaming (`stream: true`)

### 4. Cabeceras

Se envían `200` y:

| Cabecera | Motivo |
| :--- | :--- |
| `Content-Type: text/event-stream` | Formato SSE. |
| `Cache-Control: no-cache` | Que ningún intermediario cachee el stream. |
| `Connection: keep-alive` | Conexión larga. |
| `X-Accel-Buffering: no` | Desactiva el buffering de proxies tipo nginx, que si no retendrían los tokens. |
| `X-Request-Id`, `X-Worker-Id` | Trazabilidad: qué petición y qué worker la sirvió. |

### 5. Bucle de tokens

`inference.StartStream(ctx, lease, 16)` lanza `lease.ExecuteStream(ctx, out)` en una goroutine y devuelve el canal `out` (buffer de 16 chunks). El handler hace `for c := range chunks`:

- Marca el instante del primer token (para el log `ttft_ms`) y cuenta tokens.
- Construye un `chat.completion.chunk`. El primero lleva `delta.role: "assistant"`; el último lleva `finish_reason: "length"` (o `"stop"` si el simulador termina antes de `max_tokens`).
- Lo escribe como `data: <json>\n\n` y hace **flush** inmediatamente (`http.ResponseController.Flush`), para que el token llegue al cliente en cuanto se genera.

El buffer de 16 desacopla un poco al generador del cliente: si el cliente lee algo más lento, el worker puede adelantarse hasta 16 tokens antes de bloquearse. Más allá de eso hay *backpressure* natural: el worker se bloquea en el `send` hasta que el cliente consuma (o se cancele `ctx`).

### 6. Final

Cuando el canal se cierra, `wait()` devuelve el error de `ExecuteStream`:

| Resultado | Qué se envía después de los tokens |
| :--- | :--- |
| Éxito y `stream_options.include_usage: true` | Un chunk con `choices: []` y `usage` (`prompt_tokens` estimados, `completion_tokens` contados). |
| Éxito | — |
| Error con `ctx` vivo (fallo del worker) | `data: {"error": {...}}` *en banda*, porque el `200` ya se envió. |
| Cancelación | Nada más (el cliente ya no está). |

Si `ctx` sigue vivo, se termina con `data: [DONE]`, la señal estándar de fin de stream para clientes OpenAI.

### 7. Liberación

La capacidad se libera **cuando `ExecuteStream` retorna** (lo hace el propio lease con un `defer` interno), no al salir del handler. El `defer lease.Release()` del handler no hace nada en el camino feliz (`Release` es idempotente); existe para los caminos en los que `ExecuteStream` nunca llega a ejecutarse (por ejemplo, un *panic* entre la admisión y el stream).

### 8. Log

Un único log por petición, con `request_id`, `model`, `worker_id`, `prompt_tokens`, `max_new_tokens`, `stream`, `completion_tokens`, `duration_ms` y `ttft_ms`:

| Mensaje | Cuándo |
| :--- | :--- |
| `request rejected` | Falló la admisión (incluye `error`). |
| `request completed` | Stream completo. |
| `request cancelled by client` | El cliente se desconectó. |
| `request failed` | Error del worker (nivel `ERROR`). |

## Fase B' — Sin streaming (`stream: false`)

`collect` usa exactamente el mismo mecanismo (`StartStream`), pero en lugar de escribir cada chunk lo acumula en un `strings.Builder`. Al terminar responde un único JSON `chat.completion` con el texto completo, `finish_reason` y `usage`. Como no se ha escrito nada antes, un error del worker sí puede devolverse como `500`.

**Por qué reutilizar el stream:** es la decisión *streaming-first*. Hay un único camino de ejecución en el data plane; la respuesta no-streaming es solo una forma distinta de presentar el mismo stream.

## Cancelación: el cliente se va a mitad de respuesta

Es el caso que más importa en LLMs: cada petición abandonada que siguiera generando ocuparía VRAM para nada.

```mermaid
sequenceDiagram
    participant C as Cliente
    participant API as httpapi
    participant L as Lease
    participant W as Worker

    C--xAPI: cierra la conexión
    Note over API: net/http cancela r.Context()<br/>→ ctx (derivado) se cancela
    L->>L: select { case <-ctx.Done() } despierta<br/>(en la espera ITL o en el send)
    L->>W: defer Release() → reserved -= coste
    L-->>API: ExecuteStream devuelve context.Canceled
    API->>API: el range termina, wait() = Canceled
    API->>API: no escribe [DONE]; log "cancelled by client"
```

Hay dos formas de detectar la desconexión, y ambas acaban en `cancel()`:

1. **`net/http` detecta el cierre** de la conexión y cancela `r.Context()`; `ctx` deriva de él.
2. **Falla una escritura** (`writeSSE` devuelve error): el handler marca `writeFailed`, llama a `cancel()` y **sigue vaciando el canal** sin escribir. Vaciar es importante: si dejara de leer, el worker podría quedarse bloqueado en el `send` (no lo hará porque también escucha `ctx.Done()`, pero vaciar garantiza que la goroutine de `StartStream` termine y no se filtre).

El worker escucha `ctx.Done()` **en los dos puntos donde puede esperar**: la pausa entre tokens y el envío al canal. Por eso la liberación es inmediata aunque la latencia entre tokens sea larga o el cliente esté atascado (lo comprueban `TestCancellationFreesCapacityImmediately`, `TestCancellationWhileConsumerIsStuck` y `TestClientDisconnectReleasesCapacity`).

## Apagado del servidor con peticiones en curso

`SIGTERM` → `http.Server.Shutdown`: no se aceptan conexiones nuevas y se espera a que los streams en curso terminen solos durante `-shutdown-grace` (15 s). Si alguno sigue vivo, `Close()` corta las conexiones; sus contextos se cancelan y se aplica exactamente el camino de cancelación anterior, de modo que no queda capacidad reservada.
