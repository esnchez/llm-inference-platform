# 14. Fase 6b-CPU: vLLM real en CPU — decisiones de diseño

Este documento recoge las decisiones de la Fase 6b-CPU: llevar la plataforma de un vLLM falso a **vLLM real ejecutándose en CPU**, corregir lo que eso ha destapado y hacer las mediciones que el roadmap reservaba para GPUs, en versión **relativa**. Como en las fases anteriores, es el registro del *por qué*, con alternativas y la opción recomendada.

> **Estado (2026-11-21): implementada, con un criterio explícito: un sistema funcional y sencillo, no perfecto.** De las once decisiones se aplicaron las que aportan funcionamiento real con poco código; las demás se dejan como limitaciones documentadas. Ver [Qué sí y qué no](#qué-sí-y-qué-no) y los [resultados](../operaciones/04-resultados-de-pruebas.md#fase-6b-cpu-benchmark-con-vllm-real-en-cpu).

## Por qué una 6b en CPU

El proyecto **no se va a probar en GPUs reales**. La Fase 6b original (clúster de GPUs, benchmark con llmperf) se sustituye por esta. vLLM tiene un backend de CPU, y en CPU siguen existiendo el mismo servidor, la misma API, el mismo tokenizer, la misma KV-Cache paginada y el mismo *prefix caching*. Eso permite:

| Lo que la 6b quería responder | ¿Se puede en CPU? |
| :--- | :--- |
| ¿El adaptador habla bien con vLLM? (formatos, errores, métricas: FI-4) | **Sí, del todo.** Es vLLM real. |
| ¿Cuánto desperdicia la reserva pesimista? (FI-1) | **En proporción.** `vllm:kv_cache_usage_perc` frente a la reserva del ledger, con salidas reales de un modelo real. |
| ¿Cuánto falla la estimación de tokens? (FI-3) | **Sí.** El tokenizer y la *chat template* son los del modelo. |
| ¿El radix tree acierta con la caché real? (FI-6) | **Sí.** La caché de prefijos y sus expulsiones son las de vLLM. |
| ¿Router por prefijos frente a balanceo clásico? | **En proporción.** El prefill en CPU es muy caro, así que el efecto de acertar en caché se ve *más* que en GPU. |
| Latencias y throughput absolutos | **No.** Las cifras de CPU no se trasladan a una GPU y nunca se presentarán como tales. |

## Punto de partida: la verificación del 2026-11-21

| Comprobación | Resultado |
| :--- | :--- |
| Máquina | Intel i5-9400H: AVX2, FMA y F16C, **sin AVX-512 ni bf16**. Docker con 8 CPUs y ~15,8 GB. |
| Imagen | `vllm/vllm-openai-cpu:v0.34.0-x86_64` (1,8 GB) **arranca en AVX2**. |
| Modelo | `Qwen/Qwen2.5-0.5B-Instruct`, `--dtype float32`, `--max-model-len 4096`. Con `VLLM_CPU_KVCACHE_SPACE=2`, la KV-Cache es de 87 296 tokens (682 bloques de **128**). |
| Arranque | ~9 min la primera vez (2 min de descarga y ~6 de calentamiento); las siguientes, solo el calentamiento. |
| Velocidad | ~4 tokens/s de decode. Prefill de 2000 tokens: **30,3 s** en frío, **1,6 s** con el *system prompt* en caché (×18). |
| Formatos supuestos en la 6a | **Coinciden:** SSE, `usage`, `[DONE]`, `{"error":{…}}`, `/v1/models` con `max_model_len`, `/health` y los nombres de las métricas (`vllm:cache_config_info`, `kv_cache_usage_perc`, `prefix_cache_*_total`, `request_success_total`, `process_start_time_seconds`). |
| Adaptador de extremo a extremo | Funciona: descubrimiento (87 296 tokens, `max_model_len` 4096), registro, `usage` real, `400` correctos, cancelación (vLLM deja de generar). |

Y **seis hallazgos** que el falso no podía revelar:

| # | Hallazgo | Consecuencia |
| :--- | :--- | :--- |
| H1 | Si `prompt + max_tokens` supera `max_model_len`, vLLM 0.34 **recorta `max_tokens`** en vez de rechazar. Solo da `400` si el prompt por sí solo no cabe. | La plataforma (C3 de la 6a) es **más estricta que vLLM**: rechaza peticiones que vLLM atendería. |
| H2 | En CPU, el bloque de KV-Cache es de **128 tokens**; el router hashea en bloques de 16. | El router puede predecir aciertos en prefijos que vLLM no guarda (solo cachea bloques completos de 128). |
| H3 | Para "Di hola", estimamos 4 tokens y vLLM contó 32: la *chat template* añade ~28 tokens que la estimación ignora. | Error sistemático, relativo enorme en prompts cortos (FI-3). |
| H4 | El timeout de primer token (10 s + 1 ms/token) cortaría un prefill frío de 30 s como si fuera un cuelgue. | En CPU, el timeout por defecto convierte peticiones lentas en fallos del worker. |
| H5 | El log por petición muestra `completion_tokens: 9` cuando vLLM contó 8. | Bug menor: el log cuenta chunks en lugar de usar el `usage`. Respuesta y métricas son correctas. |
| H6 | El contador `vllm:request_success_total{finished_reason="abort"}` no sube cuando el cliente se desconecta. | La cancelación funciona (`num_requests_running` vuelve a 0), pero ese contador no sirve para comprobarlo. |

## Conflictos a resolver

| # | Conflicto | Propuesta | Alternativa descartada |
| :--- | :--- | :--- | :--- |
| C1 | Recortar `max_tokens` como vLLM (decisión 2) depende de la estimación del prompt, que yerra (H3, decisión 4). | Recortar con la estimación **corregida** (decisión 4). Si aun así se queda corta, vLLM recorta por su cuenta (es lo que hace hoy); si se pasa, la petición genera algo menos de lo posible. | Esperar a un tokenizer exacto: bloquearía H1 por FI-3. |
| C2 | Alinear el router a bloques de 128 (decisión 3) cambia el significado de `MinMatchBlocks` (hoy en bloques de 16). | `MinMatchBlocks` sigue en unidades de 16 tokens, y se aplica **después** de redondear el acierto al bloque del motor: con bloques de 128, el mínimo efectivo es un bloque del motor. | Cambiar la unidad: rompería la configuración y las métricas de la Fase 3. |
| C3 | El benchmark necesita dos instancias de vLLM, pero comparten 8 núcleos: la contención distorsiona las latencias. | **Fijar cada instancia a núcleos distintos** (`VLLM_CPU_OMP_THREADS_BIND`) y ejecutar los dos modos **uno detrás de otro**, nunca a la vez. | Dos máquinas: no las hay. |

## Resumen

| # | Decisión | Opción recomendada | Hallazgo |
| :--- | :--- | :--- | :--- |
| 1 | Versión fijada y capturas reales | vLLM `v0.34.0` + Qwen2.5-0.5B; capturas reales en `testdata/` que alimentan los tests y el vLLM falso | FI-4 |
| 2 | `max_tokens` por encima del contexto | Recortar como vLLM; `400` solo si el prompt no cabe | H1 |
| 3 | Tamaño de bloque del router | Redondear el acierto de cada worker a su bloque de KV-Cache | H2 |
| 4 | Estimación de tokens | Corrección en línea por modelo a partir del `usage` real | H3 (FI-3) |
| 5 | Timeouts en CPU | Flag `-ttft-timeout-per-token` y un perfil de CPU documentado | H4 |
| 6 | Correcciones menores | `completion_tokens` del log desde el `usage`; documentar el contador `abort` | H5, H6 |
| 7 | Prueba de humo repetible | Test de Go activado con `VLLM_URL` + tareas para arrancar vLLM en CPU | — |
| 8 | Stack con vLLM en CPU | `docker-compose.vllm-cpu.yml`: 1–2 instancias fijadas a núcleos, plataforma con perfil de CPU | C3 |
| 9 | Mediciones relativas | FI-1, FI-3 y FI-6 con un protocolo fijo | — |
| 10 | Benchmark router frente a round-robin | `loadgen` (no llmperf), KV-Cache reducida, prompts nuevos por ejecución | C3 |
| 11 | Alcance y roadmap | Sin cifras absolutas; FI-1 se mide, no se cambia; la Fase 7 pierde el punto de "pesos compartidos" | — |

---

## Decisión 1: versión fijada y capturas reales (cierra FI-4)

**Recomendación:** fijar `vllm/vllm-openai-cpu:v0.34.0-x86_64` con `Qwen/Qwen2.5-0.5B-Instruct` (`float32`, `max-model-len 4096`) y guardar **capturas reales** de sus respuestas en `internal/vllm/testdata/vllm-0.34.0/`: un stream SSE con `usage`, los cuerpos de error (`400` por contexto, `400` por parámetro, `404`), `/v1/models` y un `/metrics` completo.

| Opción | A favor | En contra |
| :--- | :--- | :--- |
| Seguir solo con el falso escrito a mano | Nada nuevo | El falso puede volver a desviarse de vLLM sin que nadie lo note. |
| **Capturas reales como *fixtures*** | Los tests del parser SSE, de `/metrics` y de la tabla de errores se ejecutan **contra lo que vLLM devolvió de verdad**; el falso se ajusta a ellas (bloque de 128 por defecto incluido) | Hay que renovarlas al cambiar de versión (una tarea las regenera). |

Con esto, FI-4 se cierra: los nombres de las métricas son los de la versión fijada y quedan en un test.

## Decisión 2: `max_tokens` por encima del contexto (H1)

**Recomendación:** hacer lo mismo que vLLM. Si `prompt + max_tokens` supera el `max_model_len` del modelo, **recortar `max_tokens`** a lo que cabe (como mínimo 1) y reservar solo eso; responder `400` solo si el prompt por sí solo no cabe.

| Opción | A favor | En contra |
| :--- | :--- | :--- |
| A. Seguir rechazando (C3 de la 6a) | Simple | Rechaza peticiones válidas para vLLM; un cliente que pide `max_tokens: 4096` por costumbre recibe `400` con cualquier prompt. |
| **B. Recortar como vLLM** | Mismo comportamiento con y sin la plataforma; además se reserva menos | Depende de la estimación del prompt (C1). |
| C. No comprobar nada y dejar que vLLM decida | Cero código | La reserva del ledger usaría el `max_tokens` sin recortar: reservaríamos tokens que nunca se pueden generar. |

El recorte lo hace el scheduler (que conoce el `max_model_len` de cada modelo por los snapshots). La respuesta acaba con `finish_reason: "length"` si se llega al tope, como en vLLM. Se añade una métrica `llmplatform_max_tokens_clamped_total{model}`.

## Decisión 3: tamaño de bloque del router (H2)

**Recomendación:** el router sigue hasheando en bloques de 16 tokens, pero **redondea el acierto de cada worker a su tamaño de bloque** antes de decidir. Con bloques de 128, un acierto de 20 bloques de 16 (320 tokens) cuenta como 16 (2 bloques del motor, 256 tokens). El worker anuncia su tamaño de bloque en el snapshot (`WorkerSnapshot.CacheBlockTokens`; el adaptador lo descubre en `vllm:cache_config_info`, el simulador usa 16).

| Opción | A favor | En contra |
| :--- | :--- | :--- |
| A. Dejarlo en 16 | Nada que hacer | El router sobrestima aciertos: hasta 127 tokens por petición que vLLM no puede reutilizar. |
| B. Un flag `-prefix-block-tokens` (128 para vLLM en CPU) | Exacto si todo el pool usa el mismo bloque | Configuración manual; los hashes cambian; no admite pools mixtos (simulador + vLLM, CPU + GPU). |
| **C. Redondear por worker** | Sin configuración; funciona con pools mixtos; los hashes y el árbol no cambian | El árbol guarda bloques de 16 que el motor agrupa de 8 en 8: solo cambia el cálculo del acierto, no la memoria. |

Con C2, `MinMatchBlocks` se aplica tras el redondeo.

## Decisión 4: estimación de tokens (H3, FI-3)

**Recomendación:** **corrección en línea por modelo**. Con cada respuesta que trae `usage`, el scheduler ajusta una recta `real ≈ a · estimado + b` por modelo, con olvido exponencial: `b` captura la *chat template* (los ~28 tokens fijos de H3) y `a` el error proporcional del "4 caracteres por token". La estimación corregida se usa para la reserva y para el recorte de la decisión 2.

| Opción | A favor | En contra |
| :--- | :--- | :--- |
| A. Una constante fija por mensaje | Trivial | Distinta para cada modelo y plantilla (Qwen inserta un *system prompt* por defecto si no hay uno); hay que calibrarla a mano. |
| B. Preguntar a vLLM (`POST /tokenize`) antes de admitir | Exacto | Un *round-trip* más por petición, y a un worker concreto **antes** de elegir worker. |
| **C. Corrección en línea a partir del `usage`** | Se calibra sola con el tráfico real, por modelo; no añade latencia; la métrica `prompt_token_estimate_ratio` de la Fase 5 mide si funciona | Necesita unas decenas de peticiones para converger; hasta entonces se usa la estimación cruda. |

La métrica `prompt_token_estimate_ratio` pasa a medir la estimación **corregida** (lo que de verdad usa el ledger) y se añade `llmplatform_prompt_token_raw_estimate_ratio` con la cruda, para ver el efecto de la corrección.

## Decisión 5: timeouts en CPU (H4)

**Recomendación:** exponer `-ttft-timeout-per-token` (hoy solo existe en código) y documentar un **perfil de CPU**: `-ttft-timeout 30s -ttft-timeout-per-token 25ms -stall-timeout 30s`. En la verificación, el prefill fue de unos 15 ms por token; 25 ms deja margen. El stack con vLLM en CPU (decisión 8) arranca con ese perfil.

| Opción | A favor | En contra |
| :--- | :--- | :--- |
| **Flags + perfil documentado** | Simple y explícito | Hay que acordarse de usar el perfil fuera del stack. |
| Timeout adaptativo (p99 del TTFT por token de cada worker) | Se ajusta solo | Un worker que se degrada poco a poco subiría su propio umbral y ocultaría el problema que el timeout debe detectar. |

## Decisión 6: correcciones menores (H5, H6)

- **H5:** el log por petición toma `completion_tokens` del `usage` del motor cuando existe, como ya hacen la respuesta y las métricas.
- **H6:** se documenta que vLLM 0.34 no cuenta las desconexiones en `request_success_total{finished_reason="abort"}`. La prueba de humo (decisión 7) comprueba la cancelación con `num_requests_running`. El vLLM falso mantiene su contador de abortos para los tests, que es lo que importa allí.

## Decisión 7: prueba de humo repetible

**Recomendación:** convertir la verificación manual del 2026-11-21 en algo repetible:

- `task vllm-cpu:up` / `vllm-cpu:down`: arranca y para el contenedor fijado (decisión 1), con el volumen de caché del modelo.
- `TestRealVLLM` en `internal/vllm`, que **solo se ejecuta si `VLLM_URL` está definida** (si no, `t.Skip`), y comprueba: descubrimiento (presupuesto = bloques × tamaño, `max_model_len`, tamaño de bloque), un stream con `usage`, el `400` de prompt demasiado largo, el recorte de `max_tokens` (decisión 2), los nombres de las métricas, y que una cancelación devuelve `num_requests_running` a 0.
- `task vllm-cpu:smoke`: ejecuta ese test contra `localhost:8100`.

No entra en `task check`: tarda minutos y necesita la imagen de varios GB.

## Decisión 8: stack con vLLM en CPU

**Recomendación:** una tercera variante del stack, `docker-compose.vllm-cpu.yml` (`task stack:up:vllm-cpu`):

| Servicio | Configuración |
| :--- | :--- |
| `vllm-1`, `vllm-2` | La imagen fijada, cada una con `VLLM_CPU_OMP_THREADS_BIND` a núcleos distintos (C3), `VLLM_CPU_KVCACHE_SPACE` configurable y el volumen de caché compartido. Un perfil para arrancar solo una. |
| `platform` | `-workers 0 -vllm http://vllm-1:8000,http://vllm-2:8000` con el perfil de CPU (decisión 5). |
| `prometheus` | Lee la plataforma y los dos `/metrics` de vLLM (`job="vllm"`). |

Con 8 CPUs y ~15,8 GB, caben **dos instancias** como mucho (cada una ~3 hilos y ~5 GB entre modelo y KV-Cache). El arranque frío es de ~9 minutos; el conector de la 6a ya espera a que terminen.

## Decisión 9: mediciones relativas

**Recomendación:** un protocolo fijo por cada pregunta abierta, ejecutado con el stack de la decisión 8 y `loadgen`, con los resultados en [operaciones · resultados](../operaciones/04-resultados-de-pruebas.md#fase-6b-cpu-benchmark-con-vllm-real-en-cpu):

| Pregunta | Cómo se mide | Qué se espera aprender |
| :--- | :--- | :--- |
| **FI-1**: desperdicio de la reserva | Tráfico de chat con `max_tokens` 256 (lo habitual en clientes) y salidas reales del modelo; comparar `worker_tokens_reserved / budget` con `vllm:kv_cache_usage_perc` en la fila 6 del dashboard | Cuánta KV-Cache real hay detrás de un ledger lleno. Con eso se decide (más adelante, no en esta fase) si merece la pena el *overbooking* de FI-1. |
| **FI-3**: error de estimación | `prompt_token_raw_estimate_ratio` frente a `prompt_token_estimate_ratio` con prompts cortos, largos y con *system prompt* | Si la corrección de la decisión 4 acerca la estimación a 1, o si hace falta un tokenizer. |
| **FI-6**: el árbol frente a la caché real | Acierto previsto frente a real (fila 4) con la KV-Cache reducida para forzar expulsiones | Si la LRU del árbol imita la de vLLM con expulsiones reales y bloques de 128. |

## Decisión 10: benchmark router frente a round-robin

**Recomendación:** el benchmark con **`loadgen`**, no con llmperf, diseñado para que la diferencia se vea en pocas peticiones:

| Parámetro | Valor propuesto | Por qué |
| :--- | :--- | :--- |
| Instancias | 2 vLLM, fijadas a núcleos distintos | C3. |
| KV-Cache por instancia | Reducida (~10 000 tokens, unos 5 *system prompts*) | Para que el conjunto de prompts **no quepa en una instancia pero sí en las dos**: el caso en el que el router debería ganar (como en la Fase 3). |
| Tráfico | 8 *system prompts* de ~1500 tokens, `max_tokens` 16, una petición cada ~10 s | Prefill caro y decode corto: mide sobre todo el efecto de la caché; poca carga para no saturar dos CPUs. |
| Duración | ~25 min por modo | Unas 150 peticiones por modo: suficiente para p50 y p90 con un efecto de ×18. |
| Modos | `-placement prefix`, luego `-placement round-robin` | Uno detrás de otro (C3). |
| Caché entre ejecuciones | **Prompts distintos en cada ejecución** (nuevo flag `-prefix-namespace` en `loadgen`) | Así no hace falta reiniciar vLLM (6 minutos de calentamiento) para empezar con la caché fría. |
| Resultados | TTFT p50/p90 del cliente, acierto real de caché, reparto de peticiones, desbordamientos | Comparación relativa; se documenta junto a las cifras de la Fase 3 con el simulador. |

**Por qué no llmperf:** es una herramienta Python pensada para medir throughput y latencia absolutos contra una API. Aquí solo interesa la comparación relativa entre dos colocaciones, y `loadgen` ya genera el tráfico con prefijos compartidos que el benchmark necesita.

## Decisión 11: alcance y roadmap

| Tema | Propuesta |
| :--- | :--- |
| Cifras absolutas (tokens/s, latencias) | Fuera. Cualquier cifra de CPU se presenta como relativa y con la máquina indicada. |
| Cambiar la reserva (FI-1) | Fuera: se **mide** (decisión 9) y se propone, pero el cambio sería una fase propia. |
| Varias GPUs, *tensor parallel*, modelos grandes | Fuera. |
| Roadmap | La Fase 6b de GPUs se marca como sustituida por esta. En la Fase 7, "almacenamiento compartido para pesos" pierde sentido sin GPUs y se sustituye por el volumen de caché del modelo. |

## Plan de implementación (propuesto antes de aplicar el criterio de sencillez)

1. **Capturas y versión:** guardar las respuestas reales en `testdata/`, tests del parser y de los errores contra ellas, ajustar el vLLM falso (bloque de 128 configurable). `task vllm-cpu:up/down/smoke` y `TestRealVLLM` (decisiones 1 y 7).
2. **Correcciones de comportamiento:** recorte de `max_tokens` (2), redondeo al bloque del motor (3), corrección en línea de la estimación (4), flag del timeout por token (5), log (6). Con tests contra el simulador y el falso.
3. **Stack con vLLM en CPU** (8) y validación con la prueba de humo.
4. **Mediciones y benchmark** (9 y 10): ejecutarlos y documentar los resultados.
5. **Documentación:** documentos 1–7, la guía de observabilidad, este documento con los resultados, y el README (roadmap 6b → 6b-CPU, FI-1, FI-3, FI-4, FI-6).

**Tiempo de máquina esperado:** el paso 4 es el lento. Son dos arranques de vLLM (~9 min la primera vez) y unos 50 minutos de benchmark, más las mediciones. Se puede dejar ejecutándose en segundo plano.

---

## Qué sí y qué no

Criterio (indicado al aprobar): **probar un sistema funcional, no perfecto; preferir lo sencillo y documentar los inconvenientes.**

| # | Decisión | Veredicto | Por qué |
| :--- | :--- | :--- | :--- |
| 1 | Versión fijada y capturas reales | **Sí, ligera** | `v0.34.0` fijada; respuestas reales en `internal/vllm/testdata/vllm-0.34.0/` usadas por los tests del parser SSE, de `/metrics`, de errores y del descubrimiento. El vLLM falso **no** se reajustó a las capturas. |
| 2 | Recortar `max_tokens` como vLLM | **Sí** | Pocas líneas y quita una incompatibilidad real (rechazábamos peticiones válidas para vLLM). |
| 3 | Redondear el acierto al bloque del motor (128) | **No** | Tocaba el contrato y la colocación. Limitación: el router puede contar hasta 127 tokens de acierto que vLLM no reutiliza. El benchmark mostró que el acierto previsto y el real coinciden igualmente (75,7 % frente a 75,0 %). |
| 4 | Corrección en línea de la estimación de tokens | **No** | Era la pieza más compleja. Con el recorte de la decisión 2, el error no rompe nada: si nos quedamos cortos, vLLM recorta o devuelve `400` (bien clasificado). En prompts largos el error medido es pequeño (ver [resultados](../operaciones/04-resultados-de-pruebas.md#fi-3-estimación-de-tokens)). |
| 5 | Flag del timeout por token y perfil de CPU | **Sí** | Sin él, en CPU los prefills lentos se tratan como cuelgues. |
| 6 | Log de `completion_tokens` y nota sobre `abort` | **Sí** | Trivial. |
| 7 | Prueba de humo con `VLLM_URL` | **Sí** | `TestRealVLLM` y `task vllm-cpu:up/down/smoke`; vuelve repetible la verificación. |
| 8 | Stack con vLLM en CPU | **Sí** | `task stack:up:vllm-cpu` (una instancia) y `:2` (dos, para el benchmark). |
| 9 | Mediciones FI-1, FI-3, FI-6 | **Sí, sin protocolo aparte** | Se leyeron de Prometheus durante el propio benchmark. |
| 10 | Benchmark router frente a round-robin | **Sí** | Con un único flag nuevo en `loadgen` (`-prefix-namespace`). |
| 11 | Alcance | **Sí** | Nada de cifras absolutas; FI-1 se mide, no se cambia. |

## Implementación (2026-11-21)

| Pieza | Dónde |
| :--- | :--- |
| Capturas reales y sus tests | `internal/vllm/testdata/vllm-0.34.0/`, `internal/vllm/captures_test.go` |
| Recorte de `max_tokens` | `Scheduler.clampToContext` (solo contra el `max_model_len` que informa un motor real; el simulador no cambia); la API informa la reserva recortada (`ReservedTokens` del lease) |
| Timeout por token | Flag `-ttft-timeout-per-token` en `cmd/platform` |
| Log | `completion_tokens` del log desde el `usage` del motor |
| Prueba de humo | `internal/vllm/real_test.go` (`TestRealVLLM`, se salta sin `VLLM_URL`); tareas `vllm-cpu:up`, `vllm-cpu:down`, `vllm-cpu:smoke` |
| Stack | `deploy/observability/docker-compose.vllm-cpu.yml` (variables `VLLM_URLS`, `VLLM_KV_BYTES`, `PLACEMENT`), `prometheus-vllm-cpu.yml`; tareas `stack:up:vllm-cpu` y `stack:up:vllm-cpu:2` |
| Benchmark | `loadgen -prefix-namespace` |

Por el camino, un test destapó que la API informaba en las métricas la reserva **sin recortar** (108 tokens cuando el ledger reservaba 50): ahora la toma del lease. Y las primeras capturas salieron vacías porque vLLM aún calentaba: los tests de las capturas lo detectaron.

## Resultados del benchmark y de las mediciones

Con dos instancias de vLLM real en CPU, el router por prefijos sube el acierto real de caché del 53 % al 75 % y baja el TTFT mediano unas 5× (de 24 s a 4,7 s); el árbol predice el acierto real a menos de un punto (FI-6). El ledger reserva ~3 veces lo que vLLM ocupa (FI-1) y la estimación de tokens se queda un 5 % corta en prompts largos (FI-3). Montaje, tablas y lectura en [resultados](../operaciones/04-resultados-de-pruebas.md#fase-6b-cpu-benchmark-con-vllm-real-en-cpu).

## Limitaciones conocidas (aceptadas)

| Limitación | Efecto | Mitigación posible (no aplicada) |
| :--- | :--- | :--- |
| **El ledger limita por memoria, no por cómputo.** En CPU admitió hasta 4–5 prefills fríos simultáneos por instancia; cada uno se ralentizó y algunos superaron el timeout. | Los timeouts se contaron como cuelgues, los reintentos fueron a la otra instancia (también ocupada) y el breaker se abrió: 23–29 % de `503` en el benchmark, en ambos modos. | Bajar el presupuesto con `-vllm-token-budget` para admitir menos peticiones a la vez, o alargar `-ttft-timeout-per-token`. No se probó. |
| **El timeout de primer token no distingue lento de colgado.** | En CPU con concurrencia, una petición lenta pero viva abre el breaker. | Calibrar el perfil de CPU con la concurrencia real. |
| El router no conoce el bloque de 128 tokens del motor (decisión 3: no). | Puede contar como acierto hasta 127 tokens por petición que vLLM no reutiliza. Medido: el acierto previsto y el real coinciden. | Redondear por worker. |
| La estimación ignora la *chat template* (decisión 4: no). | Prompts cortos: subestimación de ~28 tokens. | Corrección en línea a partir del `usage`. |
| El TTFT p90 queda por encima del último *bucket* (40 s). | El histograma no distingue entre 40 s y 90 s. | Añadir *buckets* para CPU (no hace falta para GPU). |
| El vLLM falso no se reajustó a las capturas (bloque de 16, no recorta `max_tokens`). | Sus magnitudes no son las reales (ya lo eran antes). | Reajustarlo si se vuelve a usar para medir. |
| vLLM 0.34 no cuenta las desconexiones en `request_success_total{finished_reason="abort"}`. | Ese contador no sirve para comprobar cancelaciones. | La prueba de humo usa `num_requests_running`. |
| Arranque de ~6 minutos por instancia (más la primera vez) y ~4 tokens/s. | Probar cuesta tiempo de reloj. | Ninguna en CPU. |

---

## Qué pasó después

Este documento es el registro de la fase tal como se decidió; lo que anunciaba para fases posteriores se resolvió así (estado al cerrar el proyecto, 2026-11-21):

| Lo que quedó dicho aquí | Qué pasó |
| :--- | :--- |
| La Fase 7 pierde el punto de "pesos compartidos" (decisión 11). | Así fue: el chart de la Fase 7 usa un PVC para la caché del modelo de vLLM en CPU (desactivado por defecto). |
| Mitigación de la concurrencia limitada por memoria y no por cálculo: bajar `-vllm-token-budget` (no probada). | Sigue sin probar; documentada como limitación en el [análisis final](../analisis-final.md#limitaciones-conocidas). |
| Autoscaling del vLLM en CPU. | Fuera de la Fase 7: ~6 minutos de arranque lo hacen inútil. KEDA escala los vLLM falsos. |
