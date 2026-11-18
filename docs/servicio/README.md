# Documentación del servicio

Esta carpeta explica **cómo funciona la implementación actual** (Fases 1 a 3) de la plataforma: qué hace cada componente, cómo viaja una petición de principio a fin, por qué está diseñado así y qué relación tiene cada pieza con los workers reales de vLLM que la sustituirán.

El [README raíz](../../README.md) cuenta la visión, las decisiones de arquitectura y el roadmap. Estos documentos bajan al nivel del código.

> **Rutas de código:** todo el código fuente está en [`platform/`](../../platform), el módulo Go. Las rutas que se citan (`internal/httpapi`, `cmd/platform`, …) son relativas a esa carpeta.

## Orden de lectura recomendado

| # | Documento | Qué responde |
| :--- | :--- | :--- |
| 1 | [Arquitectura y componentes](01-arquitectura.md) | Qué paquetes hay, qué responsabilidad tiene cada uno y cómo se conectan en un único binario. |
| 2 | [Recorrido de una petición](02-recorrido-de-una-peticion.md) | Paso a paso, qué ocurre desde que llega `POST /v1/chat/completions` hasta el último byte, incluidos errores y cancelaciones. |
| 3 | [Leases y capacidad](03-leases-y-capacidad.md) | El contrato `Worker`/`Lease`: reserva atómica, máquina de estados, TTL y liberación. |
| 4 | [Worker simulado](04-worker-simulado.md) | Cómo `simworker` imita una GPU: presupuesto de tokens, prefill, decode y sus limitaciones. |
| 5 | [API HTTP](05-api-http.md) | Contrato OpenAI-compatible, formato SSE, cabeceras y mapa de errores. |
| 6 | [Relación con vLLM](06-relacion-con-vllm.md) | Qué concepto de vLLM representa cada pieza y cómo encajará el futuro `VLLMAdapter`. |
| 7 | [Concurrencia y tests](07-concurrencia-y-tests.md) | Goroutines, canales, locks, por qué no hay *data races* y qué cubre la suite de tests. |
| 8 | [Fase 2: decisiones de diseño](08-fase-2-decisiones.md) | Las decisiones de la Fase 2 (cola, señal de capacidad, load shedding, tests), sus trade-offs y los conflictos resueltos. |
| 9 | [Fase 3: routing por prefijos](09-fase-3-routing-por-prefijos.md) | Cómo se envía cada petición al worker que ya tiene su prefijo en la KV-Cache: hashes por bloque, radix tree, algoritmo de afinidad, caché simulada, decisiones y mediciones. |

## Glosario rápido

| Término | Significado en este proyecto |
| :--- | :--- |
| **Control plane** | La parte que decide: admisión, routing, registro de workers. Hoy: `scheduler` (cola + colocación por afinidad de prefijo) + `prefix` + `httpapi`. |
| **Data plane** | La parte que ejecuta la inferencia y emite tokens. Hoy: `simworker`. Mañana: vLLM. |
| **Lease** | Reserva de capacidad en un worker concreto, obtenida *antes* de empezar a responder. |
| **Token budget** | Capacidad simulada de un worker, en tokens; representa la memoria de KV-Cache de la GPU. |
| **Ledger** | Libro de cuentas de tokens reservados de cada worker en el control plane (`reserved` frente a `TokenBudget`). Cuenta reservas de peor caso, no la ocupación real de la GPU. Ver [03](03-leases-y-capacidad.md#el-ledger-de-capacidad). |
| **Admisión / colocación** | Las dos mitades del scheduler (Fase 2): *cuándo* intentar (cola, timeouts, rechazo) y *dónde* (qué worker). Ver [08](08-fase-2-decisiones.md). |
| **Dispatcher** | Goroutine única del scheduler que consume las señales `CapacityChanged()` de todos los workers y reintenta las colas cuando se libera capacidad. |
| **Bloque de prefijo** | 16 tokens estimados (64 caracteres) del prompt. Es la unidad de la caché de prefijos, como el bloque de KV-Cache de vLLM. Ver [09](09-fase-3-routing-por-prefijos.md#decisión-1-qué-se-hashea). |
| **Hash encadenado** | Hash de un bloque *y de todos los anteriores*: identifica el prefijo completo hasta ese bloque. Viaja en `InferenceRequest.PrefixHashes`. |
| **Radix tree** | Árbol de prefijos del router (`prefix.Tree`): qué workers tienen qué prefijos, con capacidad LRU por worker. Es una aproximación de las cachés reales (FI-6). |
| **Afinidad / desbordamiento** | Afinidad: enviar la petición al worker que tiene su prefijo. Desbordamiento: si ese worker está cargado, va al menos cargado, que pasa a tener también el prefijo. Ver [09 · Decisión 5](09-fase-3-routing-por-prefijos.md#decisión-5-algoritmo-de-afinidad-fi-6-segunda-parte). |
| **Barrera FIFO** | Cuando la cabeza de la cola de un modelo supera `HeadMaxAge`, ninguna otra petición de ese modelo la adelanta hasta que entra. |
| **TTFT** | *Time To First Token*: latencia hasta el primer token. Dominada por el *prefill* del prompt. |
| **ITL** | *Inter-Token Latency*: tiempo entre dos tokens generados. Dominada por el paso de *decode*. |
| **SSE** | *Server-Sent Events*: el formato de streaming HTTP (`data: ...\n\n`) que usan OpenAI y vLLM. |
| **FI-n** | Entrada de *Limitaciones conocidas y mejoras futuras* (§9 del README raíz). |
