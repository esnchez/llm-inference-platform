# Distributed LLM Inference Platform

> **Estado: proyecto cerrado (2026-11-21).** Fases 1 a 7 implementadas: streaming con leases atómicos, cola acotada con *load shedding*, routing por afinidad de prefijo, resiliencia, observabilidad, adaptador vLLM verificado contra **vLLM 0.34 real en CPU** y despliegue en Kubernetes local con Helm y autoscaling con KEDA. **No se ha probado en GPUs**: la Fase 6b se sustituyó por vLLM en CPU.
>
> **Objetivo:** construir, con el diseño de un sistema de producción, una plataforma de serving de LLM distribuida: routing consciente de la KV-Cache, admisión por capacidad en tokens con *leases* atómicos y autoscaling por métricas de inferencia sobre K8s/vLLM, y **poder defender cada decisión** con sus alternativas, sus mediciones y sus limitaciones. Qué le falta para producción: [análisis final](docs/analisis-final.md#distancia-a-producción-con-gpus).

| Para… | Ir a |
| :--- | :--- |
| Defender el proyecto: razonamientos, evidencias, limitaciones y distancia a producción | [`docs/analisis-final.md`](docs/analisis-final.md) |
| Entender el servicio en Go y las decisiones de cada fase | [`docs/servicio/`](docs/servicio/README.md) (decisiones: [F1](docs/servicio/08-fase-1-decisiones.md), [F2](docs/servicio/09-fase-2-decisiones.md), [F3](docs/servicio/10-fase-3-decisiones.md), [F4](docs/servicio/11-fase-4-decisiones.md), [F5](docs/servicio/12-fase-5-decisiones.md), [F6a](docs/servicio/13-fase-6a-decisiones.md), [F6b-CPU](docs/servicio/14-fase-6b-cpu-decisiones.md), [F7](docs/servicio/15-fase-7-decisiones.md)) |
| Levantar entornos, ejecutar tests, leer el dashboard o ver los resultados de las pruebas | [`docs/operaciones/`](docs/operaciones/README.md) |
| Probarlo ya | [§8 Desarrollo local](#8-desarrollo-local) |

## 1. Visión del proyecto

Distributed LLM Inference Platform recibe peticiones de inferencia y las distribuye entre un conjunto de workers. El proyecto se centra exclusivamente en la **infraestructura de inferencia distribuida (Control Plane & Data Plane)**, delegando la ejecución de kernels CUDA, *continuous batching* y gestión de VRAM local a motores de inferencia consolidados como vLLM.

### Objetivos de ingeniería (Staff-Level) y resultado

| Objetivo | Resultado |
| :--- | :--- |
| Enrutar peticiones maximizando el *KV Cache hit rate* (Prefix-aware routing). | ✅ Radix tree con afinidad y umbral de carga. Con vLLM real en CPU: acierto del 53 % al 75 % y TTFT p50 ×5 mejor que round-robin. |
| Garantizar la exclusión mutua y control de admisión mediante *Leases* atómicos. | ✅ Ningún worker superó nunca su presupuesto, tampoco en las pruebas de estrés con caos. Con una réplica (FI-2). |
| Proveer una arquitectura **Streaming-first** desde el día cero. | ✅ Con SSE sobre HTTP (API de OpenAI). gRPC se descartó: los clientes de LLM y vLLM hablan OpenAI. |
| Propagar desconexiones del cliente (`ctx.Done()`) hasta el motor (vLLM) para liberar VRAM instantáneamente. | ✅ Verificado con vLLM real: cancelar detiene la generación en el motor. |
| Evolucionar de workers simulados en Go a un despliegue en Kubernetes. | ✅ Chart de Helm en minikube (un nodo, sin GPUs), con descubrimiento de workers por DNS. Multi-nodo con GPUs: sin probar. |
| Implementar un Autoscaler basado en métricas específicas de LLM, no en CPU. | ✅ KEDA escala por la demanda en tokens que ve la plataforma (reservado + en cola); validado con vLLM falsos. Se eligió esta señal en lugar de la ocupación de la KV-Cache mientras la reserva sea pesimista (FI-1). |

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
| **Contrato de Inferencia** | Petición/Respuesta unaria y bloqueante (`Infer`). | **Streaming-First** (`Lease.ExecuteStream`). | Casi todo el tráfico de un LLM es streaming. Empezar con streaming evita reescribir todo el manejo de red, buffers y cancelaciones más adelante; la respuesta sin streaming es el mismo stream acumulado. |
| **Control de Admisión** | Snapshot de capacidad -> Selección (Condición de carrera). | **Leases Atómicos** (`AcquireLease`). | Evita que dos peticiones reclamen la misma capacidad simultáneamente. Un *lease* garantiza los recursos antes de iniciar el streaming. |
| **Unidad de Capacidad** | Peticiones en vuelo (`in_flight / max_concurrency`). | **Presupuesto de Tokens / VRAM**. | Las peticiones LLM tienen costes asimétricos. Un prompt de 8K tokens satura la VRAM mucho antes que 10 prompts de 100 tokens. |
| **Algoritmo de Routing** | *Least-loaded* (Menor carga de conexiones). | **Prefix-Aware / Affinity Routing**. | *Least-loaded* destruye la localidad del KV-Cache. Medido en este proyecto, frente a round-robin: TTFT p50 ×3,6 peor en el simulador ([Fase 3](docs/operaciones/04-resultados-de-pruebas.md#fase-3-router-por-prefijos-frente-a-round-robin-simulador)) y ×5 con vLLM real en CPU ([Fase 6b-CPU](docs/operaciones/04-resultados-de-pruebas.md#fase-6b-cpu-benchmark-con-vllm-real-en-cpu)). Usamos un Radix Tree para enviar peticiones al nodo que ya tiene ese contexto procesado. |
| **Autoscaling** | Basado en utilización de CPU o tráfico HTTP (HPA). | **Basado en la demanda en tokens** que ve la plataforma (reservado + en cola), con KEDA. | En LLMs, la GPU puede estar al 100% de cómputo con 1 request o con 64: la CPU no dice nada de la capacidad. El plan inicial era escalar por la ocupación de la KV-Cache de vLLM (`vllm:kv_cache_usage_perc`, ver [FI-4](#fi-4-nombre-de-la-métrica-de-kv-cache-de-vllm)); se eligió la demanda en tokens porque es coherente con lo que admite el ledger mientras la reserva sea pesimista (FI-1). Validado en minikube con vLLM falsos ([Fase 7](docs/servicio/15-fase-7-decisiones.md)). |
| **Contabilidad de Capacidad** | Ocupación real de KV-Cache del motor. | **Reserva pesimista** (`prompt + max_new_tokens`) en el plano de control. | Simple, segura y atómica sin API de reserva en vLLM. Infrautiliza la GPU; ver [FI-1](#fi-1-reserva-pesimista-de-capacidad). |
| **Espera ante saturación** | Rechazo inmediato (`503`) o cola ilimitada. | **Cola acotada por modelo** con timeout corto, adelantamiento acotado y barrera FIFO. | En LLM la capacidad se libera de forma continua: una cola corta absorbe picos, y acotarla por tokens y por tiempo evita la latencia descontrolada. Ver [decisiones de la Fase 2](docs/servicio/09-fase-2-decisiones.md). |
| **Topología del Control Plane** | Varias réplicas con estado compartido (Redis). | **Réplica única** con estado en memoria; en Kubernetes, estrategia `Recreate`. | El *ledger* de leases solo es atómico si un único proceso es la puerta de entrada a cada worker; `Recreate` evita dos ledgers a la vez durante un despliegue, a cambio de un corte breve. Ver [FI-2](#fi-2-plano-de-control-con-más-de-una-réplica). |
| **Descubrimiento de workers** | Lista fija, o *watch* de la API de Kubernetes con `client-go`. | **Registro en caliente** alimentado por el **DNS de un Service *headless*** (`-vllm-dns`). | Cero dependencias y cero permisos; reutiliza el conector que espera a que vLLM cargue el modelo. Cuesta hasta 10 s de retraso, irrelevante frente a arranques de minutos. Las bajas se drenan sin cortar streams. Ver [Fase 7](docs/servicio/15-fase-7-decisiones.md). |

---

## 3. Arquitectura

### 3.1 Vista Lógica y Componentes

```text
                    CLIENTES (API OpenAI, SSE)
                                │  :8080 (Service público: solo la API)
          ┌─────────────────────┴──────────────────────────┐
          │      CONTROL PLANE (una réplica, cmd/platform) │
          │  ├─ httpapi: validación, SSE, 503/400 limpios  │
          │  ├─ scheduler: cola acotada por modelo + leases│
          │  ├─ placement: radix tree de prefijos          │
          │  ├─ registro: salud, breaker, reintentos,      │
          │  │   drenaje; descubrimiento por DNS           │
          │  └─ :8081 admin: /metrics, /debug (solo red    │
          │      interna)                                  │
          └─────────────────────┬──────────────────────────┘
                     AcquireLease (ledger en el control plane)
             ┌──────────────────┼────────────────────┐
             ▼                  ▼                    ▼
    [ simworker ]        [ vLLM falso ]        [ vLLM real ]
    en el proceso        HTTP (tests, K8s)     HTTP (CPU; GPU en producción)
                                │                    │
          Prometheus ◄── /metrics de la plataforma y de los motores
             │   └──► Grafana (dashboard y alertas)
             ▼
           KEDA ──► escala los pods de vLLM por demanda en tokens
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
    // Health informa si el worker está sano y su InstanceID (cambia al reiniciarse).
    Health(ctx context.Context) (HealthStatus, error)
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
- `CapacityChanged()` (Fase 2) es una señal en memoria, sin datos y agrupada: se emite una vez por lease liberado, nunca por token ni por bloque de KV-Cache. El scheduler la usa para despertar su cola ([decisión 2](docs/servicio/09-fase-2-decisiones.md#decisión-2-cómo-se-entera-el-scheduler-de-que-hay-capacidad-libre)).

---

## 4. Scheduling y Routing Avanzado

### 4.1 Radix Tree para Prefix Caching
El *Control Plane* mantiene en memoria un **Radix Tree** de los prefijos que tiene cada worker en su KV-Cache (implementado en la Fase 3, ver [`docs/servicio/10-fase-3-decisiones.md`](docs/servicio/10-fase-3-decisiones.md)).
- Cada prompt se corta en bloques de 16 tokens y cada bloque recibe un hash **encadenado** (identifica todo el prefijo hasta él), igual que el *prefix caching* de vLLM. Así cuenta cualquier prefijo compartido: *system prompt*, historial de una conversación o documentos de RAG.
- Al llegar una petición, el Router busca, por worker, la coincidencia de prefijo más larga. Prueba primero los workers con el prefijo más largo cuya carga esté por debajo de un umbral (0,8), y después el resto del menos al más cargado. Sigue siendo `AcquireLease` quien decide si cabe.
- Si el worker afín está cargado, la petición **desborda** al menos cargado, que pasa a tener también el prefijo: los prefijos populares se replican solos en vez de saturar un worker ([FI-6](#fi-6-el-radix-tree-solo-aproxima-la-caché-real-y-concentra-prefijos-populares)).
- El router no ve lo que expulsa cada GPU, así que el árbol se acota con una capacidad LRU por worker que imita su caché.
- **Resultado (simulador):** con 24 *system prompts* de ~1.500 tokens y 3 workers, el TTFT p50 baja de 206 ms (round-robin) a 57 ms, y el acierto de caché sube del 40 % al 75 %.
- **Resultado (vLLM real en CPU, 2 instancias):** acierto real del 53 % al 75 % y TTFT p50 de 24 s a 4,7 s; el árbol predijo el acierto real a menos de un punto ([resultados](docs/operaciones/04-resultados-de-pruebas.md#fase-6b-cpu-benchmark-con-vllm-real-en-cpu)).

### 4.2 Control de Admisión (Load Shedding & Backpressure)
Hay dos preguntas distintas, resueltas por piezas distintas (implementado en la Fase 2):
- **Admisión: ¿cabe ahora?** La resuelve el *lease*: `AcquireLease` reserva tokens del *ledger* del worker de forma atómica.
- **Load shedding: ¿merece la pena esperar?** La resuelve la cola. Si no cabe, la petición espera en la cola de su modelo (FIFO con adelantamiento acotado y barrera por edad) con un timeout corto (5 s; solo tráfico interactivo). Si la cola está llena (límite por tokens por modelo) o vence el timeout, el Router responde `503 Service Unavailable` con `Retry-After` derivado de la espera reciente, antes de enviar cabeceras.
- No se rechaza por un umbral de ocupación de los workers: con reserva pesimista, un *ledger* al 100 % puede ser un 30 % de uso real. Desde la Fase 6a la plataforma lee la ocupación **real** de la KV-Cache de vLLM (`vllm:kv_cache_usage_perc`), pero solo la muestra: usarla para admitir (un umbral, o el *overbooking* de [FI-1](#fi-1-reserva-pesimista-de-capacidad)) sigue pendiente.
- Así se evitan tanto la latencia descontrolada de las colas infinitas como el *KV Cache eviction thrashing* (motores que expulsan y recalculan memoria constantemente).

---

## 5. Simulación GPU (Fases Iniciales)

El desarrollo comienza sin hardware físico. El **Simulated Worker** en Go falsifica el comportamiento de una GPU:
- **Reserva:** Comprueba si la estimación de tokens (`input + max_new_tokens`) cabe en su presupuesto interno de "VRAM simulada".
- **Streaming:** Emite tokens simulados a través de canales con pausas artificiales (*Inter-Token Latency*).
- **Cancelación:** Reacciona a `ctx.Done()`, liberando instantáneamente los tokens de la VRAM simulada.
- **Realismo (Fase 2):** longitud de salida aleatoria (exponencial con media configurable, acotada por `max_tokens`) e ITL que crece con el número de leases activos, para que las políticas de cola se puedan evaluar.
- **Caché de prefijos (Fase 3):** una LRU de bloques por worker; el prefill solo cuesta los tokens no cacheados, para que el routing por prefijos se pueda medir.

Esto valida el encolado, la concurrencia, los data races y la resiliencia del Control Plane con miles de peticiones locales. Para probar el camino HTTP sin GPU existe además un **vLLM falso** (`cmd/fakevllm`, Fase 6a) con fallos inyectables, y la integración real se verificó con **vLLM en CPU** (Fase 6b-CPU).

---

## 6. Evolución hacia K8s y GPUs Reales

Qué se hizo y qué faltaría para un despliegue en producción con **vLLM** en GPUs (el detalle, en el [análisis final](docs/analisis-final.md#distancia-a-producción-con-gpus)):
1. **vLLM Adapter (hecho, Fase 6a):** un cliente HTTP con streaming SSE (`internal/vllm`) implementa el mismo contrato que el simulador; verificado contra vLLM 0.34 real en CPU. El simulador se mantiene para los tests.
2. **Autoscaling por métricas de inferencia (hecho en local, Fase 7):** **KEDA** escala los pods de vLLM con una consulta de Prometheus sobre la demanda en tokens, en lugar de un operador propio ([decisiones](docs/servicio/15-fase-7-decisiones.md)). Falta escalar los Node Pools de GPU y adelantarse a arranques de minutos (capacidad de reserva o predicción).
3. **Mitigación del Cold Start (pendiente):** que los pods de vLLM no descarguen pesos desde internet, con volúmenes compartidos (`ReadWriteMany` / caché en el nodo) para los 15–30 GB de un modelo. Sin GPUs no se abordó: el chart solo tiene un PVC de caché para vLLM en CPU.

---

## 7. Roadmap Técnico

### Fase 1 — Fundaciones y Data Plane Streaming
> Decisiones (reconstruidas a posteriori a partir de §2, los documentos 01–07 y las FI): [`docs/servicio/08-fase-1-decisiones.md`](docs/servicio/08-fase-1-decisiones.md).

- [x] Estructura base del proyecto en Go.
- [x] Interfaces Core: `Worker`, `Lease`, `InferenceRequest`, `TokenChunk`.
- [x] Implementación de `SimulatedWorker` con streaming vía canales.
- [x] Propagación de contextos y liberación de recursos (`Release`) en defer.
- [x] API HTTP local exponiendo endpoints Server-Sent Events (SSE).

### Fase 2 — Scheduler con Colas Acotadas y Load Shedding
> Diseño para **una réplica** con **reserva pesimista** ([FI-1](#fi-1-reserva-pesimista-de-capacidad), [FI-2](#fi-2-plano-de-control-con-más-de-una-réplica)). Alcance redefinido según [FI-5](#fi-5-solapamiento-del-roadmap). Decisiones y trade-offs: [`docs/servicio/09-fase-2-decisiones.md`](docs/servicio/09-fase-2-decisiones.md).

- [x] Reserva atómica de capacidad por tokens (desde la Fase 1).
- [x] Simulador realista: longitud de salida variable e ITL dependiente de la carga.
- [x] Señal `CapacityChanged()` en el contrato `Worker` y dispatcher único.
- [x] Scheduler separado en admisión (cola) y colocación (`Placement`, round-robin).
- [x] Colas acotadas por modelo (límite por tokens + tope global), FIFO con adelantamiento acotado y barrera por edad.
- [x] Timeouts explícitos y *load shedding* con `503` + `Retry-After` derivado de la carga.
- [x] Tests de contrato y de estrés con invariantes (`go test -race`), métricas en `/debug/scheduler` y generador de carga `cmd/loadgen`.

### Fase 3 — Inteligencia de Enrutamiento (KV-Cache)
> Diseño, decisiones y mediciones: [`docs/servicio/10-fase-3-decisiones.md`](docs/servicio/10-fase-3-decisiones.md). Resuelve la parte de diseño de [FI-6](#fi-6-el-radix-tree-solo-aproxima-la-caché-real-y-concentra-prefijos-populares).

- [x] Implementación del Radix Tree en memoria (aristas comprimidas, capacidad LRU por worker).
- [x] Detección y hasheo de prefijos compartidos en las requests (bloques de 16 tokens, hashes encadenados).
- [x] Algoritmo de afinidad: priorizar workers con prefijos cacheados si tienen capacidad libre (umbral de carga y desbordamiento).
- [x] Caché de prefijos en el worker simulado, métricas de acierto previsto/real y `loadgen -prefixes`.

### Fase 4 — Resiliencia y Ciclo de Vida
> Decisiones, implementación y mediciones: [`docs/servicio/11-fase-4-decisiones.md`](docs/servicio/11-fase-4-decisiones.md). Dos cambios sobre el plan original: healthchecks activos en lugar de heartbeats (vLLM no emite heartbeats), y la latencia alta solo desprioriza un worker en vez de aislarlo.

- [x] Healthchecks activos desde el registro (`Worker.Health`, histéresis 3/2) y detección de reinicios (`InstanceID`).
- [x] Circuit breakers por worker: los abren fallos del worker y cuelgues; la latencia alta solo desprioriza (worker degradado).
- [x] Registro de workers en caliente: alta y baja con drenaje (`/debug/workers`).
- [x] Reintento en otro worker si falla antes del primer token; cabeceras con el primer chunk (resuelve [FI-9](#fi-9-las-cabeceras-200-se-envían-antes-de-que-el-worker-responda)).
- [x] Fallos simulados en `simworker`, estrés con caos y `loadgen -chaos`.
- [x] Desconexiones de clientes propagadas al worker (hecho en la Fase 1, ver [FI-5](#fi-5-solapamiento-del-roadmap)).

### Fase 5 — Observabilidad
> Decisiones e implementación: [`docs/servicio/12-fase-5-decisiones.md`](docs/servicio/12-fase-5-decisiones.md). Catálogo de métricas, dashboard, ejemplo de lectura y guía de diagnóstico: [`docs/operaciones/03-observabilidad.md`](docs/operaciones/03-observabilidad.md).
> Adelantado en la Fase 2: profundidad de cola, tiempo en cola p50/p99 y rechazos por motivo en `GET /debug/scheduler` (JSON, sin Prometheus).

- [x] Instrumentación de logs estructurados (slog): eventos de ciclo de vida del scheduler, log por petición con `outcome`, `X-Request-Id`.
- [x] Exposición de métricas Prometheus: TTFT (del cliente y del worker), inter-token latency, ratio de saturación, tasa de rechazo y ~40 más (`GET /metrics`).
- [x] Grafana Dashboard local (docker-compose): `task stack:up`, dashboard como código, alertas de ejemplo.

### Fase 6 — Adaptador vLLM y GPUs Físicas
> Revisar antes de empezar: [FI-1](#fi-1-reserva-pesimista-de-capacidad), [FI-3](#fi-3-estimación-de-tokens-sin-tokenizer), [FI-4](#fi-4-nombre-de-la-métrica-de-kv-cache-de-vllm), [FI-7](#fi-7-el-contrato-solo-transporta-el-prompt-aplanado), [FI-8](#fi-8-un-tokenchunk-no-siempre-es-un-token), [FI-9](#fi-9-las-cabeceras-200-se-envían-antes-de-que-el-worker-responda).

> Partida en dos: **6a**, código probado contra un vLLM falso, sin coste; **6b**, GPUs y benchmark. Decisiones, implementación y lo que queda para la 6b: [`docs/servicio/13-fase-6a-decisiones.md`](docs/servicio/13-fase-6a-decisiones.md).

**6a — Adaptador sin GPU**
- [x] Contrato con la petición completa (mensajes y parámetros, FI-7) y tokens reales de vLLM (FI-8).
- [x] Ledger compartido entre `simworker` y el adaptador (`internal/ledger`), con batería de contrato común.
- [x] Implementar `VLLMAdapter` que traduce el contrato `Lease` a llamadas reales de la API de vLLM (descubrimiento, errores, salud y métricas del motor).
- [x] vLLM falso (`fakevllm`) para tests y para el stack (`task stack:up:fake-vllm`).
- [x] Empaquetado en Docker de los componentes de Go (`platform/Dockerfile`, una imagen por comando).

**6b — GPUs y benchmark** *(sustituida: el proyecto no se probará en GPUs reales; ver 6b-CPU)*
- [ ] Provisionar clúster efímero de GPUs (AWS/RunPod) con modelos ligeros (Llama-3-8B).
- [ ] Fijar la versión de vLLM (FI-4), calibrar timeouts y revisar FI-1 y FI-3 con datos reales.
- [ ] Benchmarking (llmperf) comparando el Router Inteligente vs Balanceo clásico.

**6b-CPU — vLLM real en CPU** *(viabilidad comprobada el 2026-11-21; decisiones —qué sí y qué no—, resultados y limitaciones: [`docs/servicio/14-fase-6b-cpu-decisiones.md`](docs/servicio/14-fase-6b-cpu-decisiones.md))*
- [x] Fijar vLLM `v0.34.0` (CPU) con capturas reales de sus respuestas en los tests (cierra FI-4).
- [x] Corregir lo que destapó vLLM real: recorte de `max_tokens`, timeouts de CPU, log. *(El bloque de 128 y la estimación con *chat template* se documentan como limitaciones, no se corrigen.)*
- [x] Prueba de humo repetible (`task vllm-cpu:smoke`) y stack con vLLM en CPU (`task stack:up:vllm-cpu`).
- [x] Mediciones relativas (FI-1, FI-3, FI-6) y benchmark router frente a round-robin: con vLLM real, la afinidad sube el acierto de caché del 53 % al 75 % y baja el TTFT mediano ~5× (relativo, en CPU).

### Fase 7 — Kubernetes en local (Autoscaling)
> En minikube, sin GPUs. Una réplica del plano de control con `Recreate` ([FI-2](#fi-2-plano-de-control-con-más-de-una-réplica)). Decisiones y resultados: [`docs/servicio/15-fase-7-decisiones.md`](docs/servicio/15-fase-7-decisiones.md).

- [x] Despliegue mediante Helm (`deploy/helm/llm-platform`, `task k8s:up`): plataforma, vLLM falsos, vLLM en CPU opcional, Prometheus y Grafana con el mismo dashboard y alertas.
- [x] Descubrimiento de workers por el DNS de un Service *headless* (`-vllm-dns`); las bajas se drenan sin cortar streams.
- [x] Puerto de administración separado (`-admin-addr`, [FI-10](#fi-10-metrics-debug-y-la-administración-comparten-puerto-con-la-api-pública)).
- [ ] ~~Configuración del almacenamiento compartido para pesos (Cold start reduction).~~ Sin GPUs pierde sentido: basta el volumen de caché del modelo de la Fase 6b-CPU (un PVC en el chart).
- [x] ~~Desarrollo de controlador custom~~ **KEDA** autoescala los vLLM falsos con la demanda en tokens que ve la plataforma (reservado + en cola). Sustituye al operador propio: mismo resultado, sin CRDs ni `client-go`.

---

## 8. Desarrollo Local

> Guía completa de cada entorno (requisitos, variantes, problemas conocidos): [`docs/operaciones/01-entorno-local.md`](docs/operaciones/01-entorno-local.md). Tests: [`docs/operaciones/02-tests.md`](docs/operaciones/02-tests.md).

**Requisitos:**
- Go 1.23+
- [Task](https://taskfile.dev) (opcional, recomendado) y Docker para los tests de carreras y los stacks.
- Para Kubernetes: minikube, Helm y kubectl.

```bash
# Plano de control + 3 workers simulados en :8080
task run -- -workers 3 -token-budget 16384 -itl 20ms   # equivale a: cd platform && go run ./cmd/platform ...
task run -- -h                    # resto de opciones (ttft, itl-load-factor, queue-timeout, placement, affinity-max-load, ...)
task loadgen -- -rate 30 -duration 10s   # carga realista contra el servidor en marcha
task loadgen -- -rate 4 -duration 60s -prefixes 24 -rag-fraction 0   # system prompts compartidos (routing por prefijos)
task run -- -placement round-robin       # colocación de la Fase 2, para comparar
task loadgen -- -rate 4 -duration 45s -chaos down   # tumba sim-worker-1 a mitad de la prueba (o hang, errors, stall, slow, restart)

# Observabilidad (necesita Docker): plataforma + Prometheus (:9090) + Grafana (:3000) con el dashboard
task stack:up
task stack:up:fake-vllm             # variante: la plataforma habla por HTTP con 3 vLLM falsos (Fase 6a)
task stack:up:vllm-cpu              # variante: vLLM real en CPU (Fase 6b-CPU; ~6 min de calentamiento; :2 para dos instancias)
task vllm-cpu:up && task vllm-cpu:smoke   # prueba de humo del adaptador contra vLLM real en CPU
task stack:down

# Contra servidores vLLM (Fase 6a/6b): se registran solos cuando han cargado el modelo
task run -- -workers 0 -vllm http://gpu-1:8000,http://gpu-2:8000

# Kubernetes en local (Fase 7; minikube, Helm): perfil propio llm-platform, no toca otros clústeres
task k8s:up                         # clúster, imágenes, KEDA y el chart (vLLM en CPU: -- --set vllmCpu.enabled=true)
task k8s:port-forward               # API :8080, Grafana :3000, admin (/metrics, /debug) :18081
task loadgen -- -admin-url http://localhost:18081 -rate 1.5 -duration 180s -prefixes 8   # KEDA añade vLLM falsos; al parar, los quita drenando
task k8s:status                     # pods, ScaledObject y HPA
task k8s:down                       # borra el perfil
```

**Suite de tests** (definida en `Taskfile.yml`; `task --list` muestra todas las tareas):

| Tarea | Qué ejecuta |
| :--- | :--- |
| `task check` (o `task`) | Suite local: `fmt:check` + `vet` + `test`. |
| `task check:all` | `check` + `test:race`. Ejecutar antes de cerrar cada fase. |
| `task fmt:check` | Falla si algún fichero no está formateado con `gofmt`. |
| `task vet` | `go vet ./...` |
| `task test` | Tests unitarios y HTTP (leases, scheduler y cola, afinidad de prefijo y Radix Tree, API SSE), incluido el test de estrés con invariantes. |
| `task test:race` | `go test -race -count=5` en un contenedor `golang:1.23` (Docker; no requiere gcc). |
| `task test:race:local` | Igual, en el host (requiere cgo: gcc en Windows). |
| `task test:cover` | Informe de cobertura por función. |

**API (compatible con OpenAI, para usar cualquier cliente o SDK de OpenAI sin adaptadores):**

Con `-admin-addr` (como en Kubernetes), `/metrics` y las rutas `/debug/*` solo se sirven en ese puerto; sin él, todo va en el mismo puerto.

| Endpoint | Descripción |
| :--- | :--- |
| `POST /v1/chat/completions` | `stream: true` → SSE (`data: {...}` + `data: [DONE]`); `stream_options.include_usage` soportado. `stream: false` → respuesta agregada. |
| `GET /v1/models` | Modelos servidos por el pool. |
| `GET /debug/workers` | `WorkerSnapshot` de cada worker (presupuesto, reservado, leases, acierto real de su caché de prefijos). |
| `GET /debug/scheduler` | Estado de la cola: peticiones y tokens en cola por modelo, tiempo en cola p50/p99, rechazos por motivo; con afinidad, sus contadores y el tamaño del Radix Tree (`placement`). |
| `GET /healthz` | Liveness. |
| `GET /metrics` | Métricas Prometheus (Fase 5); catálogo en [`03-observabilidad.md`](docs/operaciones/03-observabilidad.md). |
| `POST /debug/workers`, `DELETE /debug/workers/{id}` | Alta de un worker simulado y baja con drenaje (Fase 4). |
| `POST /debug/workers/{id}/faults`, `.../restart` | Inyectar fallos (`{"down": true}`, `{"hang": true}`, `{"error_rate": 0.3}`, …; `{}` los quita) o simular un reinicio. |

Errores de admisión (antes de enviar cabeceras): `400` petición inválida o que no cabe en ningún worker (`context_length_exceeded`), `404` modelo desconocido, `503` + `Retry-After` cuando la cola del modelo está llena (`queue_full`), la petición no obtuvo capacidad dentro del timeout de cola (`queue_timeout`), ningún worker del modelo está sano (`no_healthy_workers`) o el worker falló antes del primer token y el reintento tampoco funcionó (`worker_unavailable`). Mientras espera en la cola, el cliente no recibe ningún byte; el `200` llega con el primer token. Un fallo después del primer token se notifica en banda con un evento `data: {"error": ...}`.

```bash
curl -N localhost:8080/v1/chat/completions -H 'Content-Type: application/json' -d '{
  "model": "sim-llama-3-8b", "stream": true, "max_tokens": 16,
  "messages": [{"role": "user", "content": "Hola"}]
}'
```

**Estructura:** todo el código fuente vive en `platform/`, que es el módulo Go (`go.mod`). Las tareas de `Taskfile.yml` ya se ejecutan dentro de esa carpeta; para usar `go` directamente, haz antes `cd platform`. En este README y en `docs/`, las rutas de paquetes (`internal/...`, `cmd/...`) son relativas a `platform/`.
```text
README.md, Taskfile.yml
docs/analisis-final.md          # resumen para defender el proyecto: razonamientos, evidencias, limitaciones
docs/servicio/                  # el servicio en Go: arquitectura y decisiones de cada fase
docs/operaciones/               # entornos locales, tests, observabilidad y resultados de las pruebas
deploy/observability/           # docker-compose con Prometheus y Grafana, alertas y dashboard (Fase 5)
deploy/helm/llm-platform/       # chart de Helm para Kubernetes en local (Fase 7)
.github/workflows/ci.yml        # CI: gofmt, vet, tests con -race y helm lint
platform/                       # módulo Go llm-inference-platform
├── go.mod
├── cmd/platform/               # binario: flags, wiring, graceful shutdown, /debug de administración de workers
├── cmd/loadgen/                # generador de carga (Poisson, chat + RAG, cancelaciones, system prompts compartidos, caos)
├── cmd/fakevllm/               # vLLM falso como binario (Fase 6a)
└── internal/
    ├── inference/              # contrato core: Worker, Lease, InferenceRequest, TokenChunk, errores
    ├── simworker/              # SimulatedWorker: presupuesto de tokens, TTFT/ITL simulados, TTL de leases, caché de prefijos, fallos inyectables
    ├── scheduler/              # admisión (colas por modelo, dispatcher), colocación (afinidad o round-robin), registro, salud, breaker, reintentos
    ├── prefix/                 # hashes de prefijo por bloque y Radix Tree de prefijos por worker
    ├── httpapi/                # API OpenAI-compatible con SSE
    ├── metrics/                # métricas Prometheus (Fase 5)
    ├── ledger/                 # contabilidad de capacidad compartida por todos los workers (Fase 6a)
    ├── vllm/                   # VLLMAdapter y vLLM falso (Fase 6a)
    ├── workertest/             # batería de contrato común a los workers (tests)
    └── tokens/                 # estimación de tokens (~4 caracteres/token) sin tokenizer
```

---

## 9. Limitaciones Conocidas y Mejoras Futuras

Atajos tomados conscientemente para avanzar. Cada uno se registró con su problema y sus opciones, y se revisó cuando hubo datos. Aquí queda el resumen; la discusión completa (alternativas, mediciones) está en el documento de fase que se enlaza. Lo que falta para producción, ordenado por importancia, está en el [análisis final](docs/analisis-final.md#distancia-a-producción-con-gpus).

| FI | Tema | Estado al cerrar el proyecto |
| :--- | :--- | :--- |
| [FI-1](#fi-1-reserva-pesimista-de-capacidad) | Reserva pesimista de capacidad | **Abierta.** Medida: ~3× lo que vLLM ocupa. |
| [FI-2](#fi-2-plano-de-control-con-más-de-una-réplica) | Una sola réplica del control plane | **Abierta.** Explícita en el despliegue (`Recreate`). |
| [FI-3](#fi-3-estimación-de-tokens-sin-tokenizer) | Estimación de tokens sin tokenizer | **Aceptada.** Medida: −5 % en prompts largos. |
| [FI-4](#fi-4-nombre-de-la-métrica-de-kv-cache-de-vllm) | Nombre de la métrica de KV-Cache | Resuelta (6b-CPU). |
| [FI-5](#fi-5-solapamiento-del-roadmap) | Solapamiento del roadmap | Resuelta (Fase 2). |
| [FI-6](#fi-6-el-radix-tree-solo-aproxima-la-caché-real-y-concentra-prefijos-populares) | El radix tree aproxima la caché real | Validada con vLLM real; depende de FI-2. |
| [FI-7](#fi-7-el-contrato-solo-transporta-el-prompt-aplanado) | Prompt aplanado en el contrato | Resuelta (6a). |
| [FI-8](#fi-8-un-tokenchunk-no-siempre-es-un-token) | Un chunk no siempre es un token | Resuelta (6a). |
| [FI-9](#fi-9-las-cabeceras-200-se-envían-antes-de-que-el-worker-responda) | Cabeceras antes de que responda el worker | Resuelta (Fases 4 y 6a). |
| [FI-10](#fi-10-metrics-debug-y-la-administración-comparten-puerto-con-la-api-pública) | Administración en el puerto público | Resuelta (Fase 7), sin autenticación. |

### FI-1. Reserva pesimista de capacidad
- **Problema:** cada lease reserva `prompt + max_new_tokens` durante todo el stream y cobra el prompt entero aunque esté en caché. vLLM asigna memoria a medida que genera, así que la plataforma rechaza peticiones con la GPU lejos de estar llena.
- **Medido (6b-CPU):** el ledger reservó de media ~3× la KV-Cache que vLLM ocupaba (29,5 % frente a 9,6 %).
- **Opciones:** reservar la salida esperada y aceptar *overbooking* con la *preemption* de vLLM como red de seguridad; descontar los prefijos en caché; admitir con la ocupación real del motor, que la plataforma ya lee.
- **Dónde:** [decisión (Fase 1)](docs/servicio/08-fase-1-decisiones.md#decisión-4-reserva-pesimista), [interacción con el routing](docs/servicio/10-fase-3-decisiones.md#interacción-con-fi-1-la-reserva-pesimista), [medición](docs/operaciones/04-resultados-de-pruebas.md#fi-1-reserva-del-ledger-frente-a-kv-cache-real).

### FI-2. Plano de control con más de una réplica
- **Problema:** vLLM no tiene API de reserva, así que la atomicidad del lease depende de que un solo proceso sea la puerta de entrada a cada worker. Sin alta disponibilidad: si la réplica cae, se pierden los leases y el árbol de prefijos.
- **Estado:** una réplica con estrategia `Recreate` en Kubernetes, para que nunca haya dos *ledgers* a la vez; el precio es un corte breve en cada despliegue.
- **Opciones:** repartir los workers entre réplicas (hash consistente); estado compartido (Redis/etcd); un *sidecar* de admisión junto a cada vLLM; leases con renovación.
- **Dónde:** [decisión (Fase 1)](docs/servicio/08-fase-1-decisiones.md#decisión-9-un-binario-una-réplica-dos-planos), [despliegue (Fase 7)](docs/servicio/15-fase-7-decisiones.md#decisión-7-una-réplica-con-recreate-fi-2).

### FI-3. Estimación de tokens sin tokenizer
- **Problema:** la admisión estima ~4 caracteres por token; el error puede ser grande en código o en idiomas distintos del inglés.
- **Medido (6b-CPU):** −5 % en prompts de ~1 500 tokens; en prompts cortos, la *chat template* añade ~28 tokens que no se cuentan. **Aceptada:** la respuesta usa los tokens reales del motor, y el recorte de `max_tokens` y el `400` de vLLM cubren los casos límite.
- **Dónde:** [atajo (Fase 1)](docs/servicio/08-fase-1-decisiones.md#decisiones-11-y-12-atajos-conscientes-en-el-contrato), [tokens reales (6a)](docs/servicio/13-fase-6a-decisiones.md#decisión-2-tokens-reales-fi-8-y-fi-3), [decisión (6b-CPU)](docs/servicio/14-fase-6b-cpu-decisiones.md#decisión-4-estimación-de-tokens-h3-fi-3), [medición](docs/operaciones/04-resultados-de-pruebas.md#fi-3-estimación-de-tokens).

### FI-4. Nombre de la métrica de KV-Cache de vLLM
- **Problema:** el diseño inicial citaba `vllm:gpu_cache_usage_factor`, que no existe.
- **Resuelta (6b-CPU):** vLLM fijado en la 0.34, que expone `vllm:kv_cache_usage_perc`; los nombres están en un solo sitio del adaptador y comprobados contra respuestas reales. [Decisión](docs/servicio/14-fase-6b-cpu-decisiones.md#decisión-1-versión-fijada-y-capturas-reales-cierra-fi-4).

### FI-5. Solapamiento del roadmap
- **Problema:** la reserva atómica (listada en la Fase 2) y la propagación de desconexiones (listada en la Fase 4) ya las exigía el contrato de la Fase 1.
- **Resuelta (Fase 2):** la Fase 2 se centró en el scheduler y la Fase 4 en la resiliencia. [Fase 2](docs/servicio/09-fase-2-decisiones.md).

### FI-6. El Radix Tree solo aproxima la caché real y concentra prefijos populares
- **Problema:** el router no ve lo que vLLM expulsa de su caché, y la afinidad pura saturaría un worker con un prefijo popular.
- **Decisión (Fase 3):** LRU por worker que imita su caché, afinidad solo por debajo de una carga de 0,8 y desbordamiento al menos cargado, que replica los prefijos calientes.
- **Medido:** el acierto previsto quedó a menos de un punto del real en el simulador y con vLLM real (75,7 % frente a 75,0 %). Sigue dependiendo de que una sola réplica vea todo el tráfico (FI-2), y conviene comprobar la *chat template* de cada modelo.
- **Dónde:** [decisiones 4 y 5 (Fase 3)](docs/servicio/10-fase-3-decisiones.md#decisión-4-cómo-se-aproxima-la-caché-real-fi-6-primera-parte), [mediciones](docs/operaciones/04-resultados-de-pruebas.md#fase-6b-cpu-benchmark-con-vllm-real-en-cpu).

### FI-7. El contrato solo transporta el prompt aplanado
- **Problema:** vLLM necesita los mensajes originales (para la *chat template*) y los parámetros de muestreo.
- **Resuelta (6a):** `InferenceRequest.Messages` y `Params`; la API rechaza con `400` lo que no sabría atender y reenvía el resto. [Decisión](docs/servicio/13-fase-6a-decisiones.md#decisión-1-la-petición-en-el-contrato-fi-7).

### FI-8. Un `TokenChunk` no siempre es un token
- **Problema:** vLLM puede agrupar varios tokens en un evento, así que contar chunks falsea `usage` y las métricas de tokens.
- **Resuelta (6a):** el último chunk lleva el `usage` del motor. [Decisión](docs/servicio/13-fase-6a-decisiones.md#decisión-2-tokens-reales-fi-8-y-fi-3).

### FI-9. Las cabeceras `200` se envían antes de que el worker responda
- **Problema:** un rechazo del motor al empezar llegaría como error dentro de un stream `200`, no como un `400`.
- **Resuelta:** las cabeceras se envían con el primer token, y un fallo antes se reintenta en otro worker o llega con su código (Fase 4); el adaptador traduce el `400` de vLLM a un error de la petición, nunca a fallo del worker (6a). [Fase 4](docs/servicio/11-fase-4-decisiones.md#decisión-4-reintentos-y-cabeceras-retrasadas-resuelve-fi-9), [6a](docs/servicio/13-fase-6a-decisiones.md#decisión-6-traducción-de-errores).

### FI-10. `/metrics`, `/debug` y la administración comparten puerto con la API pública
- **Problema:** cualquiera que llegue a la API podría leer el estado interno y alterar el pool.
- **Resuelta (Fase 7):** con `-admin-addr`, esas rutas solo se sirven en un puerto que el Service público de Kubernetes no expone. Queda abierto: ese puerto no tiene autenticación; lo protege la red del clúster. [Decisión](docs/servicio/15-fase-7-decisiones.md#decisión-6-puerto-de-administración-fi-10).
