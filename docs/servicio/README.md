# Documentación del servicio

Esta carpeta explica **el servicio en Go**: qué hace cada componente, cómo viaja una petición de principio a fin, por qué está diseñado así (las decisiones de cada fase, con sus alternativas) y qué relación tiene cada pieza con vLLM.

Cómo levantar los entornos, los tests, la observabilidad y los resultados de las pruebas están en [`../operaciones`](../operaciones/README.md). El resumen para defender el proyecto, en el [análisis final](../analisis-final.md). El [README raíz](../../README.md) cuenta la visión, las decisiones de arquitectura y el roadmap.

> **Rutas de código:** todo el código fuente está en [`platform/`](../../platform), el módulo Go. Las rutas que se citan (`internal/httpapi`, `cmd/platform`, …) son relativas a esa carpeta.

## Cómo funciona (orden de lectura recomendado)

| # | Documento | Qué responde |
| :--- | :--- | :--- |
| 1 | [Arquitectura y componentes](01-arquitectura.md) | Qué paquetes hay, qué responsabilidad tiene cada uno y cómo se conectan en un único binario. |
| 2 | [Recorrido de una petición](02-recorrido-de-una-peticion.md) | Paso a paso, qué ocurre desde que llega `POST /v1/chat/completions` hasta el último byte, incluidos errores y cancelaciones. |
| 3 | [Leases y capacidad](03-leases-y-capacidad.md) | El contrato `Worker`/`Lease`: reserva atómica, máquina de estados, TTL y liberación. |
| 4 | [Worker simulado](04-worker-simulado.md) | Cómo `simworker` imita una GPU: presupuesto de tokens, prefill, decode y sus limitaciones. |
| 5 | [API HTTP](05-api-http.md) | Contrato OpenAI-compatible, formato SSE, cabeceras y mapa de errores. |
| 6 | [Relación con vLLM](06-relacion-con-vllm.md) | Qué concepto de vLLM representa cada pieza, cómo funciona el `VLLMAdapter` y qué huecos del contrato hubo que cerrar. |
| 7 | [Concurrencia](07-concurrencia.md) | Goroutines, canales, locks y por qué no hay *data races*. |

## Decisiones por fase

Un documento por fase: el problema, las alternativas, la opción elegida y por qué, cómo quedó implementada y un resumen de lo que se midió (el detalle de cada medición está en [operaciones · resultados](../operaciones/04-resultados-de-pruebas.md)). 
| # | Documento | Qué decide |
| :--- | :--- | :--- |
| 8 | [Fase 1: fundaciones y streaming](08-fase-1-decisiones.md) | Reconstruido a posteriori: contrato *streaming-first*, lease atómico, capacidad en tokens, reserva pesimista, admisión antes de responder, TTL, cancelación de extremo a extremo, API OpenAI, un binario y una réplica, worker simulado, y los atajos conscientes (FI-1, FI-2, FI-3, FI-7, FI-8, FI-9). |
| 9 | [Fase 2: decisiones de diseño](09-fase-2-decisiones.md) | Las decisiones de la Fase 2 (cola, señal de capacidad, load shedding, tests), sus trade-offs y los conflictos resueltos. |
| 10 | [Fase 3: routing por prefijos](10-fase-3-decisiones.md) | Cómo se envía cada petición al worker que ya tiene su prefijo en la KV-Cache: hashes por bloque, radix tree, algoritmo de afinidad, caché simulada, decisiones y mediciones. |
| 11 | [Fase 4: decisiones de diseño](11-fase-4-decisiones.md) | Resiliencia: registro de workers en caliente con drenaje, healthchecks, circuit breaker, workers degradados, reintento antes del primer token, clasificación de errores, fallos simulados y mediciones con caos. |
| 12 | [Fase 5: decisiones de diseño](12-fase-5-decisiones.md) | Librería de métricas, arquitectura de la instrumentación, logs, stack local con Prometheus y Grafana, alcance, y lo que se encontró al validar. |
| 13 | [Fase 6a: decisiones de diseño](13-fase-6a-decisiones.md) | Adaptador vLLM sin GPU, implementado: petición completa en el contrato, tokens reales, ledger compartido, descubrimiento de workers, traducción de errores, métricas del motor, vLLM falso, empaquetado; y lo que el falso no puede decir. |
| 14 | [Fase 6b-CPU: decisiones de diseño](14-fase-6b-cpu-decisiones.md) | vLLM real en CPU (sin GPUs), implementada con criterio de sencillez — qué sí y qué no: verificación de viabilidad, seis hallazgos y sus correcciones, prueba de humo repetible, stack con vLLM en CPU, mediciones relativas (FI-1, FI-3, FI-6) y benchmark router frente a round-robin, con resultados y limitaciones aceptadas. |
| 15 | [Fase 7: Kubernetes en local](15-fase-7-decisiones.md) | Despliegue en minikube con Helm: descubrimiento de workers por DNS, bajas sin cortar streams, autoscaling con KEDA por demanda en tokens, puerto de administración (FI-10), una réplica con `Recreate` (FI-2). |

## Glosario rápido

| Término | Significado en este proyecto |
| :--- | :--- |
| **Control plane** | La parte que decide: admisión, routing, registro de workers: `scheduler` (cola + colocación por afinidad de prefijo) + `prefix` + `httpapi`. |
| **Data plane** | La parte que ejecuta la inferencia y emite tokens: `simworker` (simulado) o servidores vLLM a través del adaptador `internal/vllm` (Fase 6a). |
| **Lease** | Reserva de capacidad en un worker concreto, obtenida *antes* de empezar a responder. |
| **Token budget** | Capacidad de un worker, en tokens; representa la memoria de KV-Cache. Configurada en el simulador; descubierta de `vllm:cache_config_info` con vLLM. |
| **Ledger** | Libro de cuentas de tokens reservados de cada worker en el control plane (`reserved` frente a `TokenBudget`). Cuenta reservas de peor caso, no la ocupación real de la GPU. Ver [03](03-leases-y-capacidad.md#el-ledger-de-capacidad). |
| **Admisión / colocación** | Las dos mitades del scheduler (Fase 2): *cuándo* intentar (cola, timeouts, rechazo) y *dónde* (qué worker). Ver [09](09-fase-2-decisiones.md). |
| **Breaker** | *Circuit breaker* por worker (cerrado / abierto / semiabierto). Lo abren fallos del worker o un healthcheck no sano; mientras está abierto, el worker no recibe peticiones nuevas. Ver [11 · Decisión 3](11-fase-4-decisiones.md#decisión-3-aislamiento-con-circuit-breaker-la-latencia-solo-desprioriza). |
| **Degradado** | Worker cuya ITL supera 2× la mediana de los demás: solo se usa si los otros están llenos. |
| **Drenaje** | Baja de un worker sin cortar sus streams: deja de recibir peticiones y desaparece cuando termina la última. |
| **Dispatcher** | Goroutine única del scheduler que consume las señales `CapacityChanged()` de todos los workers y reintenta las colas cuando se libera capacidad. |
| **Bloque de prefijo** | 16 tokens estimados (64 caracteres) del prompt. Es la unidad de la caché de prefijos, como el bloque de KV-Cache de vLLM. Ver [10](10-fase-3-decisiones.md#decisión-1-qué-se-hashea). |
| **Hash encadenado** | Hash de un bloque *y de todos los anteriores*: identifica el prefijo completo hasta ese bloque. Viaja en `InferenceRequest.PrefixHashes`. |
| **Radix tree** | Árbol de prefijos del router (`prefix.Tree`): qué workers tienen qué prefijos, con capacidad LRU por worker. Es una aproximación de las cachés reales (FI-6). |
| **Afinidad / desbordamiento** | Afinidad: enviar la petición al worker que tiene su prefijo. Desbordamiento: si ese worker está cargado, va al menos cargado, que pasa a tener también el prefijo. Ver [10 · Decisión 5](10-fase-3-decisiones.md#decisión-5-algoritmo-de-afinidad-fi-6-segunda-parte). |
| **Barrera FIFO** | Cuando la cabeza de la cola de un modelo supera `HeadMaxAge`, ninguna otra petición de ese modelo la adelanta hasta que entra. |
| **TTFT** | *Time To First Token*: latencia hasta el primer token. Dominada por el *prefill* del prompt. |
| **ITL** | *Inter-Token Latency*: tiempo entre dos tokens generados. Dominada por el paso de *decode*. |
| **SSE** | *Server-Sent Events*: el formato de streaming HTTP (`data: ...\n\n`) que usan OpenAI y vLLM. |
| **FI-n** | Entrada de *Limitaciones conocidas y mejoras futuras* (§9 del README raíz). |
