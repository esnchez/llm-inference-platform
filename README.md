# Distributed LLM Inference Platform

> **Estado:** Fases 1 y 2 implementadas (Data Plane streaming con workers simulados; scheduler con colas acotadas y *load shedding*). Decisiones de la Fase 2: [`docs/servicio/08-fase-2-decisiones.md`](docs/servicio/08-fase-2-decisiones.md).  
> **Documentación de la implementación:** [`docs/servicio/`](docs/servicio/README.md) (componentes, recorrido de una petición, leases, relación con vLLM).  
> **Objetivo:** Construir una plataforma de serving de LLM distribuida a nivel de producción, con routing consciente de KV-Cache (Prefix-aware), scheduling basado en capacidad de tokens (VRAM), control de concurrencia atómico mediante *leases*, y autoscaling predictivo para K8s/vLLM.

## 1. Visión del proyecto

Distributed LLM Inference Platform recibe peticiones de inferencia y las distribuye entre un conjunto de workers. El proyecto se centra exclusivamente en la **infraestructura de inferencia distribuida (Control Plane & Data Plane)**, delegando la ejecución de kernels CUDA, *continuous batching* y gestión de VRAM local a motores de inferencia consolidados como vLLM.

### Objetivos de ingeniería (Staff-Level)
- Enrutar peticiones maximizando el *KV Cache hit rate* (Prefix-aware routing).
- Garantizar la exclusión mutua y control de admisión mediante *Leases* atómicos.
- Proveer una arquitectura **Streaming-first** (gRPC/SSE) desde el día cero.
- Propagar desconexiones del cliente (`ctx.Done()`) hasta el motor (vLLM) para liberar VRAM instantáneamente.
- Evolucionar de workers simulados en Go a un despliegue multi-nodo en Kubernetes.
- Implementar un Autoscaler basado en métricas específicas de LLM (ej. uso de bloques de KV Cache), no en CPU.

### No objetivos
- Entrenar modelos fundacionales.
- Escribir kernels CUDA o motores de inferencia en C++/Python desde cero.
- Reimplementar mecanismos como PagedAttention.
- Introducir Redis/Kafka prematuramente si la coordinación en memoria del plano de control es suficiente inicialmente.

---

## 2. Puntos de Fricción Iniciales y Decisiones Arquitectónicas

Durante la fase de diseño se identificaron varios cuellos de botella clásicos de arquitecturas web (REST/CRUD) que fallan al aplicarse a inferencia de LLMs. A continuación, las decisiones tomadas para solventarlos:

| Área de Fricción | Diseño Inicial / Clásico | Decisión Final (Arquitectura Actual) | Justificación |
| :--- | :--- | :--- | :--- |
| **Contrato de Inferencia** | Petición/Respuesta unaria y bloqueante (`Infer`). | **Streaming-First** (`InferStream`). | El 95% del tráfico LLM es streaming. Empezar con streaming evita reescribir todo el manejo de red, buffers y cancelaciones más adelante. |
| **Control de Admisión** | Snapshot de capacidad -> Selección (Condición de carrera). | **Leases Atómicos** (`AcquireLease`). | Evita que dos peticiones reclamen la misma capacidad simultáneamente. Un *lease* garantiza los recursos antes de iniciar el streaming. |
| **Unidad de Capacidad** | Peticiones en vuelo (`in_flight / max_concurrency`). | **Presupuesto de Tokens / VRAM**. | Las peticiones LLM tienen costes asimétricos. Un prompt de 8K tokens satura la VRAM mucho antes que 10 prompts de 100 tokens. |
| **Algoritmo de Routing** | *Least-loaded* (Menor carga de conexiones). | **Prefix-Aware / Affinity Routing**. | *Least-loaded* destruye la localidad del KV-Cache (aumenta el TTFT un 80%). Usamos un Radix Tree para enviar peticiones al nodo que ya tiene ese contexto procesado. |
| **Autoscaling (Futuro)** | Basado en utilización de CPU o tráfico HTTP (HPA). | **Basado en Saturación de Cache**. | En LLMs, la GPU puede estar al 100% de cómputo con 1 request o con 64. Escalaremos leyendo `vllm:gpu_cache_usage_factor` y `vllm:num_requests_waiting` (nombre de métrica pendiente de corregir, ver [FI-4](#fi-4-nombre-de-la-métrica-de-kv-cache-de-vllm)). |
| **Contabilidad de Capacidad** | Ocupación real de KV-Cache del motor. | **Reserva pesimista** (`prompt + max_new_tokens`) en el plano de control. | Simple, segura y atómica sin API de reserva en vLLM. Infrautiliza la GPU; ver [FI-1](#fi-1-reserva-pesimista-de-capacidad). |
| **Espera ante saturación** | Rechazo inmediato (`503`) o cola ilimitada. | **Cola acotada por modelo** con timeout corto, adelantamiento acotado y barrera FIFO. | En LLM la capacidad se libera de forma continua: una cola corta absorbe picos, y acotarla por tokens y por tiempo evita la latencia descontrolada. Ver [decisiones de la Fase 2](docs/servicio/08-fase-2-decisiones.md). |
| **Topología del Control Plane** | Varias réplicas con estado compartido (Redis). | **Réplica única** con estado en memoria. | El *ledger* de leases solo es atómico si un único proceso es la puerta de entrada a cada worker. Ver [FI-2](#fi-2-plano-de-control-con-más-de-una-réplica). |

---

## 3. Arquitectura

### 3.1 Vista Lógica y Componentes

```text
                              CLIENTS (SSE / gRPC)
                                      │
                         [ Ingress / API Gateway ]
                                      │
                   ┌──────────────────┴──────────────────┐
                   │            CONTROL PLANE            │
                   │  ├─ Prefix Radix Tree (Routing)     │
                   │  ├─ Admission Queue + Leases        │
                   │  ├─ Worker Registry                 │
                   │  └─ HPA Controller (Autoscaler)     │
                   └──────────────────┬──────────────────┘
                                      │
                            AcquireLease(Request)
                                      │
              ┌───────────────────────┼───────────────────────┐
              ▼                       ▼                       ▼
    [ Worker 1 (Sim) ]      [ Worker 2 (Sim) ]      [ Worker N (vLLM) ]
    ├─ Token Budget         ├─ Token Budget         ├─ K8s Pod / GPU
    └─ Streaming Mock       └─ Streaming Mock       └─ Paged VRAM
              │                       │                       │
              └───────────────────────┴───────────────────────┘
                                      │
                                [ Metrics ] -> Prometheus / Grafana
```

### 3.2 El Contrato del Worker (Data Plane)

El contrato desacopla el scheduler del runtime, pero impone semántica atómica y de streaming:

```go
type Worker interface {
    ID() string
    Models() []string
    // AcquireLease intenta reservar capacidad de VRAM/Tokens atómicamente.
    // Devuelve ErrAtCapacity si el worker está saturado.
    AcquireLease(ctx context.Context, req InferenceRequest) (Lease, error)
    Snapshot() WorkerSnapshot
    // CapacityChanged avisa (agrupado, una vez por lease liberado) de que hay capacidad libre.
    CapacityChanged() <-chan struct{}
}

type Lease interface {
    WorkerID() string
    // ExecuteStream envía los tokens generados al canal `out`.
    // Si el context se cancela, interrumpe el motor inmediatamente.
    ExecuteStream(ctx context.Context, out chan<- TokenChunk) error
    // Release libera la capacidad reservada (debe llamarse en defer).
    Release() 
}
```

**Semántica del contrato (fijada en Fase 1, ver `internal/inference`):**
- `ExecuteStream` se puede llamar **una sola vez** (`ErrLeaseUsed` en la segunda) y **nunca cierra `out`**: el canal es del llamador. `inference.StartStream` encapsula el patrón habitual (goroutine + cierre + error).
- La capacidad se libera **en cuanto `ExecuteStream` retorna** (fin, error o `ctx.Done()`), no al llegar al `defer Release()`.
- `Release` es idempotente y concurrente-segura; el `defer lease.Release()` cubre los caminos en los que nunca se llega a ejecutar.
- Un *lease* tiene **TTL**: si no se ejecuta a tiempo, el worker recupera la capacidad (`ErrLeaseExpired`).
- `CapacityChanged()` (Fase 2) es una señal en memoria, sin datos y agrupada: se emite una vez por lease liberado, nunca por token ni por bloque de KV-Cache. El scheduler la usa para despertar su cola ([decisión 2](docs/servicio/08-fase-2-decisiones.md#decisión-2-cómo-se-entera-el-scheduler-de-que-hay-capacidad-libre)).

---

## 4. Scheduling y Routing Avanzado

### 4.1 Radix Tree para Prefix Caching
El *Control Plane* mantiene en memoria un **Radix Tree** de los hashes de los *System Prompts* o prefijos largos activos en el clúster. 
- Al llegar una petición, el Router busca la coincidencia de prefijo más larga.
- Si el `Worker-B` tiene ese prefijo en su memoria caché local y dispone de capacidad (puede emitir un Lease), la petición se enruta allí.
- **Resultado:** Reducción drástica del *Time To First Token (TTFT)*.

### 4.2 Control de Admisión (Load Shedding & Backpressure)
Hay dos preguntas distintas, resueltas por piezas distintas (implementado en la Fase 2):
- **Admisión: ¿cabe ahora?** La resuelve el *lease*: `AcquireLease` reserva tokens del *ledger* del worker de forma atómica.
- **Load shedding: ¿merece la pena esperar?** La resuelve la cola. Si no cabe, la petición espera en la cola de su modelo (FIFO con adelantamiento acotado y barrera por edad) con un timeout corto (5 s; solo tráfico interactivo). Si la cola está llena (límite por tokens por modelo) o vence el timeout, el Router responde `503 Service Unavailable` con `Retry-After` derivado de la espera reciente, antes de enviar cabeceras.
- No se rechaza por un umbral de ocupación de los workers: con reserva pesimista, un *ledger* al 100 % puede ser un 30 % de uso real. Un umbral sobre la ocupación **real** de KV-Cache (`vllm:kv_cache_usage_perc`, ver [FI-4](#fi-4-nombre-de-la-métrica-de-kv-cache-de-vllm)) llegará con vLLM en la Fase 6, junto con el *overbooking* de [FI-1](#fi-1-reserva-pesimista-de-capacidad).
- Así se evitan tanto la latencia descontrolada de las colas infinitas como el *KV Cache eviction thrashing* (motores que expulsan y recalculan memoria constantemente).

---

## 5. Simulación GPU (Fases Iniciales)

El desarrollo comienza sin hardware físico. El **Simulated Worker** en Go falsifica el comportamiento de una GPU:
- **Reserva:** Comprueba si la estimación de tokens (`input + max_new_tokens`) cabe en su presupuesto interno de "VRAM simulada".
- **Streaming:** Emite tokens simulados a través de canales con pausas artificiales (*Inter-Token Latency*).
- **Cancelación:** Reacciona a `ctx.Done()`, liberando instantáneamente los tokens de la VRAM simulada.
- **Realismo (Fase 2):** longitud de salida aleatoria (exponencial con media configurable, acotada por `max_tokens`) e ITL que crece con el número de leases activos, para que las políticas de cola se puedan evaluar.

Esto valida el encolado, la concurrencia, los data races y la resiliencia del Control Plane con miles de peticiones locales antes de provisionar hardware real.

---

## 6. Evolución hacia K8s y GPUs Reales

Cuando la arquitectura se despliegue en producción con **vLLM**:
1. **Mitigación del Cold Start:** Los pods de vLLM no descargarán pesos desde internet. Se usarán volúmenes compartidos (ej. `ReadWriteMany` / hostPath cache) para mapear los 15-30GB de modelos directamente en la memoria del contenedor al arrancar.
2. **K8s Custom Operator:** Un operador en Go leerá las métricas de saturación desde Prometheus y actuará escalando los Node Pools de GPU y los Pods, mucho antes de que el throughput empiece a degradarse.
3. **vLLM Adapter:** El `SimulatedWorker` será reemplazado por un cliente gRPC/HTTP bidireccional conectado al servidor vLLM real.

---

## 7. Roadmap Técnico

### Fase 1 — Fundaciones y Data Plane Streaming
- [x] Estructura base del proyecto en Go.
- [x] Interfaces Core: `Worker`, `Lease`, `InferenceRequest`, `TokenChunk`.
- [x] Implementación de `SimulatedWorker` con streaming vía canales.
- [x] Propagación de contextos y liberación de recursos (`Release`) en defer.
- [x] API HTTP local exponiendo endpoints Server-Sent Events (SSE).

### Fase 2 — Scheduler con Colas Acotadas y Load Shedding
> Diseño para **una réplica** con **reserva pesimista** ([FI-1](#fi-1-reserva-pesimista-de-capacidad), [FI-2](#fi-2-plano-de-control-con-más-de-una-réplica)). Alcance redefinido según [FI-5](#fi-5-solapamiento-del-roadmap). Decisiones y trade-offs: [`docs/servicio/08-fase-2-decisiones.md`](docs/servicio/08-fase-2-decisiones.md).

- [x] Reserva atómica de capacidad por tokens (desde la Fase 1).
- [x] Simulador realista: longitud de salida variable e ITL dependiente de la carga.
- [x] Señal `CapacityChanged()` en el contrato `Worker` y dispatcher único.
- [x] Scheduler separado en admisión (cola) y colocación (`Placement`, round-robin).
- [x] Colas acotadas por modelo (límite por tokens + tope global), FIFO con adelantamiento acotado y barrera por edad.
- [x] Timeouts explícitos y *load shedding* con `503` + `Retry-After` derivado de la carga.
- [x] Tests de contrato y de estrés con invariantes (`go test -race`), métricas en `/debug/scheduler` y generador de carga `cmd/loadgen`.

### Fase 3 — Inteligencia de Enrutamiento (KV-Cache)
> Revisar antes de empezar: [FI-6](#fi-6-el-radix-tree-solo-aproxima-la-caché-real-y-concentra-prefijos-populares).

- [ ] Implementación del Radix Tree en memoria.
- [ ] Detección y hasheo de prefijos compartidos en las requests.
- [ ] Algoritmo de afinidad: priorizar workers con prefijos cacheados si tienen capacidad libre.

### Fase 4 — Resiliencia y Ciclo de Vida
- [ ] Heartbeats y healthchecks del worker hacia el registry.
- [ ] Circuit breakers para aislar workers degradados (latencia alta).
- [x] Desconexiones de clientes propagadas al worker (hecho en la Fase 1, ver [FI-5](#fi-5-solapamiento-del-roadmap)).

### Fase 5 — Observabilidad
> Adelantado en la Fase 2: profundidad de cola, tiempo en cola p50/p99 y rechazos por motivo en `GET /debug/scheduler` (JSON, sin Prometheus).

- [ ] Instrumentación de logs estructurados (slog).
- [ ] Exposición de métricas Prometheus: TTFT simulado, inter-token latency simulado, ratio de saturación, tasa de rechazo.
- [ ] Grafana Dashboard local (docker-compose).

### Fase 6 — Adaptador vLLM y GPUs Físicas
> Revisar antes de empezar: [FI-1](#fi-1-reserva-pesimista-de-capacidad), [FI-3](#fi-3-estimación-de-tokens-sin-tokenizer), [FI-4](#fi-4-nombre-de-la-métrica-de-kv-cache-de-vllm), [FI-7](#fi-7-el-contrato-solo-transporta-el-prompt-aplanado), [FI-8](#fi-8-un-tokenchunk-no-siempre-es-un-token), [FI-9](#fi-9-las-cabeceras-200-se-envían-antes-de-que-el-worker-responda).

- [ ] Empaquetado en Docker de los componentes de Go.
- [ ] Implementar `VLLMAdapter` que traduce el contrato `Lease` a llamadas reales de la API de vLLM.
- [ ] Provisionar clúster efímero de GPUs (AWS/RunPod) con modelos ligeros (Llama-3-8B).
- [ ] Benchmarking (llmperf) comparando el Router Inteligente vs Balanceo clásico.

### Fase 7 — Kubernetes Operator (Autoscaling)
> Revisar antes de empezar: [FI-2](#fi-2-plano-de-control-con-más-de-una-réplica).

- [ ] Despliegue mediante Helm.
- [ ] Configuración del almacenamiento compartido para pesos (Cold start reduction).
- [ ] Desarrollo de controlador custom para autoescalar pods basado en métricas nativas (`gpu_cache_usage_factor`, ver [FI-4](#fi-4-nombre-de-la-métrica-de-kv-cache-de-vllm)).

---

## 8. Desarrollo Local

**Requisitos:**
- Go 1.23+
- [Task](https://taskfile.dev) (opcional, recomendado) y Docker para los tests de carreras.

```bash
# Plano de control + 3 workers simulados en :8080
task run -- -workers 3 -token-budget 16384 -itl 20ms   # equivale a: cd platform && go run ./cmd/platform ...
task run -- -h                    # resto de opciones (ttft, itl-load-factor, mean-output-tokens, queue-timeout, ...)
task loadgen -- -rate 30 -duration 10s   # carga realista contra el servidor en marcha
```

**Suite de tests** (definida en `Taskfile.yml`; `task --list` muestra todas las tareas):

| Tarea | Qué ejecuta |
| :--- | :--- |
| `task check` (o `task`) | Suite local: `fmt:check` + `vet` + `test`. |
| `task check:all` | `check` + `test:race`. Ejecutar antes de cerrar cada fase. |
| `task fmt:check` | Falla si algún fichero no está formateado con `gofmt`. |
| `task vet` | `go vet ./...` |
| `task test` | Tests unitarios y HTTP (leases, scheduler y cola, API SSE), incluido el test de estrés con invariantes. |
| `task test:race` | `go test -race -count=5` en un contenedor `golang:1.23` (Docker; no requiere gcc). |
| `task test:race:local` | Igual, en el host (requiere cgo: gcc en Windows). |
| `task test:cover` | Informe de cobertura por función. |

**API (compatible con OpenAI, para poder usar `llmperf` en la Fase 6):**

| Endpoint | Descripción |
| :--- | :--- |
| `POST /v1/chat/completions` | `stream: true` → SSE (`data: {...}` + `data: [DONE]`); `stream_options.include_usage` soportado. `stream: false` → respuesta agregada. |
| `GET /v1/models` | Modelos servidos por el pool. |
| `GET /debug/workers` | `WorkerSnapshot` de cada worker (presupuesto, reservado, leases). |
| `GET /debug/scheduler` | Estado de la cola: peticiones y tokens en cola por modelo, tiempo en cola p50/p99, rechazos por motivo. |
| `GET /healthz` | Liveness. |

Errores de admisión (antes de enviar cabeceras): `400` petición inválida o que no cabe en ningún worker (`context_length_exceeded`), `404` modelo desconocido, `503` + `Retry-After` cuando la cola del modelo está llena (`queue_full`) o la petición no obtuvo capacidad dentro del timeout de cola (`queue_timeout`). Mientras espera en la cola, el cliente no recibe ningún byte. Un fallo a mitad de stream se notifica en banda con un evento `data: {"error": ...}`.

```bash
curl -N localhost:8080/v1/chat/completions -H 'Content-Type: application/json' -d '{
  "model": "sim-llama-3-8b", "stream": true, "max_tokens": 16,
  "messages": [{"role": "user", "content": "Hola"}]
}'
```

**Estructura:** todo el código fuente vive en `platform/`, que es el módulo Go (`go.mod`). Las tareas de `Taskfile.yml` ya se ejecutan dentro de esa carpeta; para usar `go` directamente, haz antes `cd platform`. En este README y en `docs/`, las rutas de paquetes (`internal/...`, `cmd/...`) son relativas a `platform/`.
```text
README.md, Taskfile.yml
docs/servicio/                  # documentación detallada de la implementación
platform/                       # módulo Go llm-inference-platform
├── go.mod
├── cmd/platform/               # binario: flags, wiring, graceful shutdown
├── cmd/loadgen/                # generador de carga (Poisson, chat + RAG, cancelaciones)
└── internal/
    ├── inference/              # contrato core: Worker, Lease, InferenceRequest, TokenChunk, errores
    ├── simworker/              # SimulatedWorker: presupuesto de tokens, TTFT/ITL simulados, TTL de leases
    ├── scheduler/              # admisión (colas por modelo, dispatcher) + colocación (Placement round-robin)
    ├── httpapi/                # API OpenAI-compatible con SSE
    └── tokens/                 # estimación de tokens (~4 caracteres/token) sin tokenizer
```

---

## 9. Limitaciones Conocidas y Mejoras Futuras

Decisiones tomadas conscientemente para avanzar, con el problema que dejan abierto y las opciones para resolverlo. Cada entrada indica cuándo revisarla.

### FI-1. Reserva pesimista de capacidad
- **Decisión actual:** cada lease reserva `prompt_tokens + max_new_tokens` (peor caso) durante todo el stream (`InferenceRequest.TokenCost`).
- **Problema:** vLLM no reserva el peor caso: asigna bloques de KV-Cache a medida que genera y, si se queda sin memoria, expulsa (*preemption*) peticiones. Los clientes suelen pedir `max_tokens` de 2–4K con salidas reales de unos cientos de tokens, así que rechazaremos peticiones (`503`) con la GPU lejos de estar llena. Además, con *prefix caching* los bloques de un prefijo compartido se cuentan una vez por petición, lo que choca con el routing por afinidad de la Fase 3.
- **Opciones:**
  - Reservar `prompt + salida esperada` (p. ej. p90 histórico por modelo/cliente) y aceptar *overbooking*, usando la preemption del motor como red de seguridad.
  - Liberar progresivamente la parte no usada de la reserva y descontar los tokens de prefijo ya cacheados.
  - Usar el lease solo como *slot* de admisión y tomar la ocupación real de las métricas del motor.
- **Revisar:** al medir la utilización real con vLLM (Fase 6).

### FI-2. Plano de control con más de una réplica
- **Decisión actual:** una sola réplica del plano de control; el *ledger* de leases vive en la memoria del proceso.
- **Problema:** vLLM no ofrece una API de reserva, así que la atomicidad de `AcquireLease` depende de que un único proceso sea la puerta de entrada a cada worker. Con varias réplicas (alta disponibilidad, más throughput) cada una tendría su propia visión de la capacidad y sobre-reservarían. Si la réplica cae, se pierde el estado de leases y de routing.
- **Opciones:**
  - Particionar: cada worker pertenece a una sola réplica (hashing consistente por worker/modelo) y el gateway enruta a la réplica dueña.
  - Estado compartido (Redis/etcd) con operaciones atómicas, aceptando la latencia extra y la dependencia que hoy se excluye en *No objetivos*.
  - Admisión en el propio worker: un sidecar junto a cada vLLM es la fuente de verdad del presupuesto y las réplicas solo enrutan.
  - Leases con renovación (*heartbeat*) para recuperar la capacidad si la réplica que los emitió muere a mitad de stream.
- **Revisar:** antes del despliegue en Kubernetes (Fase 7), o antes si se necesita alta disponibilidad.

### FI-3. Estimación de tokens sin tokenizer
- **Decisión actual:** `internal/tokens.Estimate` aproxima ~4 caracteres por token.
- **Problema:** el error puede ser de ~2× en código o en idiomas distintos del inglés (incluido el español), lo que distorsiona directamente la admisión (FI-1) y los límites de contexto.
- **Opciones:** tokenizer por modelo en Go (cgo con HF *tokenizers* o una implementación BPE nativa); el endpoint `/tokenize` de vLLM (un *round-trip* extra antes de admitir); mantener la estimación con un margen de seguridad configurable por modelo; corregir a posteriori con el `usage` real que devuelve el motor.
- **Revisar:** en la Fase 6, cuando el tokenizer real esté disponible.

### FI-4. Nombre de la métrica de KV-Cache de vLLM
- **Problema:** `vllm:gpu_cache_usage_factor`, citada en las secciones 2, 4.2 y la Fase 7, no existe. vLLM expone `vllm:gpu_cache_usage_perc` (motor V0) y `vllm:kv_cache_usage_perc` (motor V1), ambas como fracción 0–1.
- **Acción:** fijar la versión de vLLM, comprobar el nombre exacto en su `/metrics` y corregir las referencias. Conviene que el adaptador abstraiga el nombre para soportar cambios entre versiones.
- **Revisar:** en la Fase 6, al fijar la versión de vLLM.

### FI-5. Solapamiento del roadmap
- **Problema:** la reserva atómica de capacidad por worker (listada en la Fase 2) y la propagación de desconexiones del cliente al worker (listada en la Fase 4) ya están implementadas en la Fase 1, porque el contrato de `Lease` las exige.
- **Propuesta:** que la Fase 2 se centre en el scheduler (colas acotadas con timeout, *load shedding* a nivel de pool, tests `-race` de alta concurrencia) y que la Fase 4 se centre en heartbeats, health checks y circuit breakers.
- **Resuelto (2026-11-09):** la Fase 2 se centró en el scheduler (colas acotadas, *load shedding*, tests de estrés) y la propagación de desconexiones queda marcada como hecha en la Fase 4.

### FI-6. El Radix Tree solo aproxima la caché real y concentra prefijos populares
- **Problema:** el router no ve las expulsiones (*evictions*) que hace vLLM en su caché, así que el árbol puede apuntar a prefijos que ya no están. Además, la afinidad pura envía todo el tráfico de un *system prompt* popular al mismo worker y lo satura.
- **Opciones:** TTL/LRU en las entradas del árbol; una puntuación que combine afinidad de prefijo y carga (p. ej. seguir la afinidad solo si la carga está por debajo de un umbral, o *power of two choices* entre los workers con el prefijo); replicar prefijos calientes en varios workers; usar eventos de KV-Cache del motor si la versión de vLLM los publica.
- **Revisar:** al diseñar la Fase 3.

### FI-7. El contrato solo transporta el prompt aplanado
- **Decisión actual:** `httpapi.renderPrompt` convierte los `messages` en texto `role: content\n` y `InferenceRequest` solo lleva ese `Prompt`, que se usa únicamente para estimar el coste. Los parámetros de muestreo (`temperature`, `top_p`, `stop`, …) se aceptan pero se ignoran.
- **Problema:** vLLM necesita los `messages` originales para aplicar la *chat template* de cada modelo, y los parámetros de muestreo para generar lo que el cliente pidió. Con el contrato actual el `VLLMAdapter` no tiene esa información.
- **Opciones:** ampliar `InferenceRequest` con los mensajes estructurados y un bloque de parámetros de muestreo tipado; o transportar el cuerpo original de la petición (`json.RawMessage`) para reenviarlo a vLLM sin reinterpretarlo, manteniendo los campos que necesita el control plane (modelo, coste) aparte.
- **Revisar:** al empezar el `VLLMAdapter` (Fase 6).

### FI-8. Un `TokenChunk` no siempre es un token
- **Decisión actual:** cada `TokenChunk` se trata como un token: `completion_tokens` se calcula contando chunks e `Index` es la posición del token.
- **Problema:** vLLM normalmente emite un token por evento, pero puede agrupar varios (al retener bytes de un carácter UTF-8 incompleto o con *speculative decoding*). El `usage` devuelto al cliente y las métricas de tokens (ITL, throughput) serían incorrectos.
- **Opciones:** tomar `completion_tokens` (y `prompt_tokens`) del `usage` que devuelve vLLM (`stream_options.include_usage`) y transportarlo en el último chunk; renombrar `Index` como índice de chunk y no de token.
- **Revisar:** al empezar el `VLLMAdapter` (Fase 6).

### FI-9. Las cabeceras `200` se envían antes de que el worker responda
- **Decisión actual:** tras obtener el lease, el handler escribe `200` y las cabeceras SSE antes de que `ExecuteStream` produzca el primer chunk.
- **Problema:** vLLM puede rechazar la petición al empezar (p. ej. `prompt + max_tokens` mayor que `--max-model-len` según su tokenizer real, que puede no coincidir con nuestra estimación, FI-3). Ese rechazo llegaría como error *en banda* dentro de un stream `200`, en lugar de un `400` que los clientes y los benchmarks clasifican correctamente.
- **Opciones:** retrasar la escritura de cabeceras hasta el primer chunk o hasta la primera respuesta de vLLM (el cliente no recibe cabeceras hasta que el worker responde, y el handler se complica); validar la longitud con el tokenizer real antes de admitir (ligado a FI-3).
- **Revisar:** al empezar el `VLLMAdapter` (Fase 6).
