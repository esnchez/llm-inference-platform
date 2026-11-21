# 4. Resultados de las pruebas

Todas las mediciones del proyecto, por fase, con su montaje y su lectura. Las decisiones que cada prueba valida están en los documentos de fase de [servicio](../servicio/README.md); el resumen para defender el proyecto, en el [análisis final](../analisis-final.md).

> **Cada cifra lleva su entorno.** Las del simulador validan la lógica, no el rendimiento. Las de vLLM en CPU son reales pero **relativas** (un modelo de 0,5B en una CPU de portátil): vale la comparación, no el valor absoluto. Las de Kubernetes usan vLLM falsos: validan el mecanismo (descubrimiento, escalado, drenaje).

| Fase | Qué se midió | Entorno |
| :--- | :--- | :--- |
| 2 | [Sobrecarga deliberada de 2×](#fase-2-sobrecarga-deliberada-simulador) | Simulador |
| 3 | [Router por prefijos frente a round-robin](#fase-3-router-por-prefijos-frente-a-round-robin-simulador) | Simulador |
| 4 | [Worker caído y colgado, con y sin reintentos](#fase-4-fallos-con-loadgen--chaos-simulador) | Simulador |
| 5 | [Validación del stack de observabilidad](#fase-5-validación-del-stack-de-observabilidad) | Stack de Docker |
| 6a | [Contrato, integración y caos por la red](#fase-6a-validación-con-el-vllm-falso) | vLLM falso |
| 6b-CPU | [Router frente a round-robin, FI-1 y FI-3](#fase-6b-cpu-benchmark-con-vllm-real-en-cpu) | **vLLM 0.34 real en CPU** |
| 7 | [Despliegue, autoscaling y bajas de pods](#fase-7-kubernetes-en-minikube) | minikube, vLLM falso |

Cómo levantar cada entorno: [entorno local](01-entorno-local.md).

## Fase 2: sobrecarga deliberada (simulador)

*Decisiones: [09](../servicio/09-fase-2-decisiones.md).*

Dos workers de 8.192 tokens, valores por defecto del binario y `task loadgen -- -rate 30 -duration 8s` (10 % de peticiones tipo RAG, 5 % de clientes que cancelan):

| Resultado | Peticiones |
| :--- | :--- |
| `200` | 49 % |
| `503 queue_full` | 39 % |
| `503 queue_timeout` | 7 % |
| Canceladas por el cliente | 4 % |

El 66 % de las admitidas pasó por la cola (75 de 118), con una espera p50 de 4,0 s. Es una sobrecarga deliberada (unas 29 peticiones por segundo, más del doble de lo que pueden servir dos workers) para comprobar que la cola absorbe lo que puede y rechaza pronto el resto. Ajustar los valores por defecto con cargas realistas quedó pendiente y no se hizo: sin GPUs no hay cargas de referencia, así que los valores de la cola siguen siendo los de diseño.

## Fase 3: router por prefijos frente a round-robin (simulador)

*Decisiones: [10](../servicio/10-fase-3-decisiones.md).*

Con `-prefill-per-token 100us` (~10.000 tokens/s de prefill, más cerca de un modelo 8B real que los 20 µs por defecto), 3 workers con el presupuesto por defecto (16.384 tokens, caché de 1.024 bloques cada uno), 4 req/s durante 60 s, sin RAG ni cancelaciones, y la misma semilla en ambos modos:

```bash
task run -- -placement prefix -prefill-per-token 100us        # o -placement round-robin
task loadgen -- -rate 4 -duration 60s -prefixes 24 -rag-fraction 0 -cancel-fraction 0
```

| Escenario | Colocación | TTFT p50 | TTFT p99 | Acierto real | Acierto previsto | Reparto de leases |
| :--- | :--- | ---: | ---: | ---: | ---: | :--- |
| 24 *system prompts* de ~1.500 tokens, uniformes | **prefix** | **57 ms** | 210 ms | **74,5 %** | 73,8 % | 82 / 79 / 74 |
| | round-robin | 206 ms | 214 ms | 39,8 % | — | 79 / 78 / 78 |
| 24 prompts, Zipf `-prefix-skew 1.5` (unos pocos calientes) | **prefix** | 56 ms | 207 ms | **85,9 %** | 85,4 % | 72 / 50 / 89 |
| | round-robin | 57 ms | 208 ms | 75,8 % | — | 71 / 70 / 70 |
| 1 único prompt para todo el tráfico | **prefix** | 56 ms | 73 ms | 95,7 % | — | 88 / 83 / 64 |
| | round-robin | 56 ms | 78 ms | 95,7 % | — | 79 / 78 / 78 |

Lectura:

- **Uniforme:** el caso para el que existe el router. Los 24 prompts ocupan ~2.250 bloques, más que la caché de un worker (1.024) pero menos que la de los tres. Con round-robin cada worker intenta guardarlos todos y se expulsan entre sí. Con afinidad se reparten y caben, así que el TTFT p50 baja de 206 ms a 57 ms (el prefill de 1.500 tokens desaparece). El p99 sigue en ~210 ms porque cada prompt hace un prefill completo la primera vez en cada worker, y 22 peticiones desbordaron por carga.
- **Zipf:** los prompts calientes caben en todas las cachés, así que round-robin también acierta en la mediana. La afinidad mejora el acierto global (+10 puntos) a costa de un reparto menos uniforme (50–89), que el umbral de carga mantiene acotado.
- **Un solo prompt (el caso de FI-6):** sin el umbral, los 235 leases habrían ido a un worker. Con él, 2 desbordamientos replicaron el prompt en los 3 workers y desde entonces el empate se resolvió por carga. Acierto y latencia son iguales que con round-robin: la afinidad no empeora el caso que más temía FI-6.
- El acierto previsto por el árbol queda a menos de 1 punto del real en los dos casos con expulsiones. Aproximar la caché con una LRU propia funciona mientras haya una sola réplica.

A 20 req/s (unas 4× la capacidad del pool con reserva pesimista) ambos modos saturan y el TTFT lo domina la cola (~5 s). La afinidad no añade capacidad ([10 · Interacción con FI-1](../servicio/10-fase-3-decisiones.md#interacción-con-fi-1-la-reserva-pesimista)).

## Fase 4: fallos con `loadgen -chaos` (simulador)

*Decisiones: [11](../servicio/11-fase-4-decisiones.md).*

Tres workers con los valores por defecto del binario, `-ttft-timeout 2s`, 4 req/s durante 45 s (10 % de peticiones RAG, sin cancelaciones). `sim-worker-1` se rompe entre los segundos 15 y 30:

```bash
task run -- -ttft-timeout 2s                     # añadir -max-retries -1 para comparar sin reintentos
task loadgen -- -rate 4 -duration 45s -cancel-fraction 0 -chaos down   # o -chaos hang
```

| Fallo | Reintentos | `200` | `503 worker_unavailable` | Fallos del worker | Reintentos (con éxito) | TTFT p99 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| Caído (`down`) | 1 (por defecto) | **181 (100 %)** | 0 | 9 | 9 (9) | 132 ms |
| | ninguno | 177 (97,8 %) | 4 | 4 | 0 | 157 ms |
| Colgado (`hang`) | 1 (por defecto) | **181 (100 %)** | 0 | 10 (todos por TTFT) | 10 (10) | 2,18 s |
| | ninguno | 171 (94,5 %) | 10 | 10 | 0 | 132 ms |

Lectura:

- **Con reintentos el cliente no ve ninguno de los fallos.** Cada petición que cayó en el worker roto antes del primer token se completó en otro.
- **Caído:** los healthchecks lo detectan en ~6 s (3 fallos cada 2 s) y abren el breaker; los fallos de antes (4–9) son los que absorben los reintentos.
- **Colgado:** pasa los healthchecks, así que solo lo detecta el timeout de TTFT. Tras 5 cuelgues seguidos se abre el breaker; cada prueba en semiabierto vuelve a colgarse y lo reabre con el doble de espera. El coste está en la cola de latencia: las peticiones reintentadas esperaron el timeout entero (p99 de 2,18 s con `-ttft-timeout 2s`; con el valor por defecto de 10 s, el p99 sería de ~10 s).
- En los cuatro casos el worker se recuperó solo al quitar el fallo: breaker cerrado y sano al terminar.

## Fase 5: validación del stack de observabilidad

*Decisiones: [12](../servicio/12-fase-5-decisiones.md).*

- Tests: `internal/metrics` (catálogo, *lint*, valores, histogramas, cola, contadores a 0), eventos del scheduler con un observador falso y logs, resultados de cada camino de la API y `X-Request-Id`. Ver [tests](02-tests.md).
- Stack: `promtool check config` y `check rules`; *target* `UP`; dashboard y fuente de datos aprovisionados; **cada consulta del dashboard ejecutada contra Prometheus** tras una prueba con `loadgen`; revisión visual con una captura (ver el [ejemplo de lectura](03-observabilidad.md#ejemplo-de-lectura-un-worker-colgado)).

## Fase 6a: validación con el vLLM falso

*Decisiones: [13](../servicio/13-fase-6a-decisiones.md).*

- Tests: batería de contrato contra el adaptador (red real a un `httptest.Server`), unidad (parser SSE, errores, cuerpo de la petición, `/metrics`), integración (parámetros reenviados, `usage`, errores, cancelación que **aborta en el motor**, reinicios con y sin `process_start_time_seconds`, conector que espera a que cargue el modelo, extremo a extremo con reintento) y **estrés con caos sobre la red**: 400 peticiones contra 3 vLLM falsos que fallan, se cuelgan, cortan el stream, emiten errores, se reinician y dejan de estar sanos, sin fugas ni sobrecargas del ledger. `-race -count=5` en Go 1.23.
- Stack `task stack:up:fake-vllm`: los tres falsos se registraron solos al terminar de "cargar" (~15 s), con presupuesto descubierto (1024 × 16 = 16 384 tokens) y `max_model_len` 8192; Prometheus lee el `/metrics` de la plataforma y el de los motores; `loadgen` con 24 prompts compartidos y un falso colgado 30 s; cada consulta del dashboard, incluida la fila 6, devolvió datos.

## Fase 6b-CPU: benchmark con vLLM real en CPU

*Decisiones: [14](../servicio/14-fase-6b-cpu-decisiones.md).*

**Montaje:** dos instancias de vLLM 0.34 en CPU (i5-9400H, AVX2), cada una con 3 hilos en núcleos distintos y una KV-Cache de 192 MiB (8 192 tokens, 64 bloques de 128, descubiertos por el adaptador). `loadgen`: 8 *system prompts* de 6 000 caracteres (~1 450–1 600 tokens reales por petición), `max_tokens` 16, 0,1 peticiones/s, 20 minutos por modo, misma semilla, prompts nuevos en cada modo (`-prefix-namespace`). Perfil de CPU: `-ttft-timeout 30s -ttft-timeout-per-token 25ms -stall-timeout 30s`.

```bash
VLLM_KV_BYTES=201326592 task stack:up:vllm-cpu:2
task loadgen -- -model Qwen/Qwen2.5-0.5B-Instruct -rate 0.1 -duration 20m -prefixes 8 -system-chars 6000 \
  -chat-max-tokens 16 -rag-fraction 0 -cancel-fraction 0 -prefix-namespace A -seed 7
# después, con PLACEMENT=round-robin (solo se reinicia la plataforma) y -prefix-namespace B
```

### Router por prefijos frente a round-robin

| | Afinidad de prefijo | Round-robin |
| :--- | ---: | ---: |
| Peticiones enviadas | 124 | 124 |
| `200` | **96 (77,4 %)** | 88 (71,0 %) |
| `503` (`worker_unavailable` + `no_healthy_workers` + `queue_timeout`) | 28 | 36 |
| **TTFT del cliente, p50** | **4,7 s** | 24,0 s |
| TTFT del worker (prefill), p50 | 4,7 s | 23,3 s |
| TTFT p90 (ambos) | > 40 s (último *bucket*) | > 40 s |
| **Acierto real de la caché de vLLM** | **75,0 %** | 53,4 % |
| Acierto previsto por el router | 75,7 % | — |
| Fallos atribuidos a workers (cuelgues por timeout) | 24 (18) | 50 (26) |
| Aperturas del breaker | 3 | 7 |

**Lectura:**

- **El router funciona con un motor real.** Con los 8 prompts repartidos entre dos instancias que solo caben a medias en cada una, la afinidad sube el acierto real de la caché del 53 % al 75 % y **baja el TTFT mediano unas 5×** (4,7 s frente a 24 s). El efecto va en la misma dirección que en el simulador (Fase 3: ×3,6) y es mayor porque en CPU el prefill es mucho más caro.
- **FI-6 en real:** el acierto previsto por el árbol (75,7 %) quedó a menos de un punto del real (75,0 %), con expulsiones reales y bloques de 128. La LRU del árbol imita bien la caché de vLLM.
- **Las cifras absolutas no significan nada fuera de esta máquina** (0,5B parámetros, CPU de portátil). Lo que vale es la comparación.

### FI-1: reserva del ledger frente a KV-Cache real

| | Afinidad | Round-robin |
| :--- | ---: | ---: |
| Reserva del ledger, media | 29,5 % | 42,9 % |
| **KV-Cache real (`vllm:kv_cache_usage_perc`), media** | **9,6 %** | **13,4 %** |
| Reserva del ledger, máximo | 95,9 % | 95,9 % |
| KV-Cache real, máximo | 63,5 % | 82,5 % |

El ledger reserva **unas 3 veces más** de lo que vLLM ocupa de media. En este tráfico, la causa principal no es `max_tokens` (solo 16) sino que la reserva cobra el prompt completo a cada petición aunque esté en caché (FI-1, segundo problema). Con `max_tokens` grandes, la diferencia sería mayor. Es el dato que FI-1 pedía; cambiar la reserva queda para una fase propia.

### FI-3: estimación de tokens

| | p10 | p50 | p90 |
| :--- | ---: | ---: | ---: |
| Estimados / reales (prompts de ~1 500 tokens) | 0,91 | 0,95 | 0,99 |

Con prompts largos, la estimación de "4 caracteres por token" se queda solo un 5 % corta (mediana). El caso malo son los prompts **cortos**: la *chat template* añade ~28 tokens fijos (en la verificación, "Di hola": estimados 4, reales 32). Como se rechazó la corrección (decisión 4), queda como limitación: afecta poco a la reserva (son pocos tokens) y vLLM recorta o rechaza si hace falta.

## Fase 7: Kubernetes en minikube

*Decisiones: [15](../servicio/15-fase-7-decisiones.md).*

**Entorno:** minikube (Kubernetes 1.37, driver docker) en un portátil Windows, perfil `llm-platform` con 6 CPU y 10 GB; KEDA 2.21.1 instalado con Helm; vLLM falsos con 16 384 tokens de presupuesto cada uno. Acceso con `kubectl port-forward`. **Todo es vLLM falso:** las cifras validan el mecanismo (descubrimiento, escalado, drenaje), no el rendimiento.

### Despliegue y FI-10

`task k8s:up` levanta el clúster, carga las imágenes, instala KEDA y el chart. La plataforma registra el primer vLLM falso a los pocos milisegundos de arrancar (`vllm dns: new server` → `vllm registered`). Prometheus descubre la plataforma y los motores (`job="vllm"` por `dns_sd_configs`), y Grafana carga el mismo dashboard que el stack de Docker.

| Petición | Puerto público (Service `platform`) | Puerto de administración (`platform-admin`) |
| :--- | :--- | :--- |
| `/healthz`, `/v1/*` | `200` | — |
| `/debug/workers`, `/metrics` | **`404`** | `200` |

### Prueba 1: sobrecarga, escalado de 1 a 4 y vuelta a 1

`loadgen -rate 4 -duration 180s -prefixes 8` (725 peticiones; la demanda supera a propósito la capacidad de 4 réplicas):

| Segundo | Pods listos | Demanda (tokens) | Workers en la plataforma |
| ---: | ---: | ---: | ---: |
| 28 | 1 | 37 217 | 1 |
| 57 | 1 de 4 | 55 934 | 4 |
| 71 | 4 | 98 364 | 4 |
| 71–198 | 4 (máximo) | 78 000–107 000 | 4 |
| 209 | 4 | 25 130 (fin de la carga) | 4 |
| 233 | 3 | 0 | 4 → 3 |
| 257 | **1** | 0 | **1** |

- **Subida:** de 1 a 4 réplicas en ~40 s desde que la demanda supera el umbral (sondeo de KEDA cada 10 s, arranque del pod y 5 s de "carga del modelo", y hasta 10 s de DNS).
- **Bajada:** de 4 a 1 en ~50 s tras acabar la carga (estabilización de 30 s).
- **Resultado:** 69,7 % `200`, 24,7 % `503` (`queue_full` y `queue_timeout`), 5,7 % canceladas por el cliente (deliberado). Los `503` son el *load shedding* de la Fase 2 haciendo su trabajo: con el máximo de 4 réplicas la demanda (~90 000 tokens) sigue por encima de la capacidad (65 536), y además el primer minuto lo sirve una sola réplica mientras llegan las demás.

### Prueba 2: carga que el autoscaling sí cubre, y un pod borrado en mitad de streams

`loadgen -rate 1.5 -duration 180s -prefixes 8` (271 peticiones) y, en el segundo 110, `kubectl delete pod` de un vLLM falso con streams en curso:

| Resultado | Valor |
| :--- | :--- |
| `200` | **258 (95,2 %)** |
| `503 queue_timeout` | 1 (0,4 %), en el primer minuto, antes de escalar |
| Canceladas por el cliente | 12 (4,4 %, deliberado) |
| **Errores dentro del stream** | **0** |
| TTFT p50 / p99 | 151 ms / 3,9 s (incluye la espera en cola mientras escala) |
| Peticiones reintentadas en otro worker | 8, todas con éxito |

Qué pasó al borrar el pod (logs de la plataforma):

1. El pod recibe `SIGTERM`, cierra su *listener* y termina sus streams.
2. Durante ~10 s el pod aún está en la última respuesta de DNS que vio la plataforma: las peticiones nuevas que le tocan fallan **al conectar** (antes del primer token), se **reintentan en otro worker** (Fase 4) y el breaker lo aísla. El cliente no ve nada.
3. Al desaparecer del DNS, la plataforma lo da de baja con drenaje (`worker draining` → `worker removed`).
4. Mientras, KEDA reducía réplicas porque bajó la demanda: dos workers se dieron de baja con **un stream en curso cada uno** (`in_flight: 1`) y se eliminaron cuando ese stream terminó, sin cortarlo.

**Conclusión:** las bajas de pods, sean del autoscaler o manuales, no producen errores visibles; el reintento antes del primer token absorbe la ventana en la que el DNS va por detrás.

### Lo que la validación destapó

| Hallazgo | Estado |
| :--- | :--- |
| El puerto 8081 está reservado en Windows (rango excluido por Hyper-V): `port-forward` falla al usarlo en local. | **Corregido:** el puerto de administración se reenvía a `localhost:18081`. Dentro del clúster sigue siendo el 8081. |
| `loadgen` pedía `/debug/*` al puerto público, que ahora responde `404`. | **Corregido:** flag `-admin-url`. |
| Ventana de hasta 10 s en la que un pod que se apaga sigue en el DNS de la plataforma: sus conexiones fallan y abren el breaker. | **Aceptado:** el reintento lo hace invisible (8 reintentos, 0 errores). Se podría eliminar haciendo que el pod siga aceptando conexiones unos segundos tras el `SIGTERM` (el patrón habitual de *deregistration delay*, o un `preStop`), pero añade espera a cada baja y con vLLM real habría que comprobar cómo se comporta. |
| La estrategia `Recreate` de la plataforma deja **unos segundos sin servicio** en cada `helm upgrade` o `rollout restart` (el pod viejo termina antes de que arranque el nuevo). | **Aceptado:** es el precio de FI-2, buscado a propósito (decisión 7). |
