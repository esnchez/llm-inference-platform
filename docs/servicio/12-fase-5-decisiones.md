# 12. Fase 5: observabilidad — decisiones de diseño

Este documento recoge las decisiones de diseño de la Fase 5 (métricas Prometheus, logs y dashboards de Grafana), con sus alternativas y la opción elegida. Como en las fases 2 y 4, es el registro del *por qué*. **Qué se mide, por qué y cómo leer los dashboards** está en la guía [observabilidad · Observabilidad](../operaciones/03-observabilidad.md), que es la referencia de uso y concreta estas decisiones.

> **Estado (2026-11-18):** las ocho decisiones y los tres conflictos están **aprobados con la opción recomendada** e **implementados**; ver [Implementación](#implementación-2026-11-18). La guía 12 es la referencia de lo implementado.

## Punto de partida

| Qué hay hoy | Problema |
| :--- | :--- |
| `GET /debug/scheduler` y `/debug/workers`: contadores acumulados y estado actual en JSON. | No hay historia: no se puede ver cómo evolucionó una métrica durante una prueba ni comparar dos momentos. |
| Percentiles de espera en cola sobre las últimas 1.024 peticiones. | Ventana arbitraria; no se pueden agregar entre réplicas ni recalcular para otro intervalo. |
| TTFT y latencia total solo en el log de cada petición y en el resumen de `loadgen`. | Para un percentil hay que procesar logs; no hay TTFT del lado del worker separado de la espera en cola. |
| Un log por petición en `httpapi`. | **El scheduler no registra nada:** ni aperturas del breaker, ni cambios de salud, ni altas y bajas, ni reintentos. Un incidente de la Fase 4 solo se ve consultando `/debug/workers` en el momento justo. |

Lo que pide el roadmap: logs estructurados, métricas Prometheus (TTFT, ITL, ratio de saturación, tasa de rechazo) y un dashboard de Grafana local. Además, la Fase 6 necesita estas métricas para su benchmark y la Fase 7 las usará como entrada del autoscaler.

## Conflictos a resolver

| # | Conflicto | Propuesta | Alternativa descartada |
| :--- | :--- | :--- | :--- |
| C1 | La etiqueta `worker` es útil en las métricas de latencia (ver qué GPU va lenta), pero multiplica las series de cada histograma. | Dos familias: las métricas **de petición** llevan solo `model`; las métricas **de worker** (TTFT e ITL medidos en el worker) llevan solo `worker`. | Poner `model` y `worker` en todo: N modelos × M workers × ~16 *buckets* por histograma, sin preguntas que lo justifiquen. |
| C2 | `/metrics` y `/debug/*` (con los endpoints de administración de la Fase 4) quedarían en el mismo puerto que la API pública. | **Mismo puerto en la Fase 5**, porque el stack es local. Se registra como limitación (FI-10) para separarlos en otro puerto antes de desplegar (Fase 7). | Un segundo puerto ya: obliga a cambiar `loadgen`, los tests y la documentación de `/debug` sin beneficio en local. |
| C3 | La ITL por token es la métrica más frecuente (≈ 50 observaciones por segundo y stream). | Se observa **por token**, igual que vLLM. Observar un histograma cuesta decenas de nanosegundos frente a una ITL de decenas de milisegundos. | Una ITL media por stream: pierde la cola de la distribución, que es justo lo que se quiere ver. |

## Resumen

| # | Decisión | Opción recomendada | Depende de |
| :--- | :--- | :--- | :--- |
| 1 | Librería de métricas | `prometheus/client_golang`, primera dependencia externa del módulo | — |
| 2 | Cómo se instrumenta | Interfaces de observación en `scheduler` y `httpapi`, implementadas en un paquete nuevo `internal/metrics`; el estado actual se lee al hacer el *scrape* | 1 |
| 3 | Qué se mide | Catálogo de la guía de observabilidad: señales de petición, admisión, capacidad, routing por prefijos y salud, con dos vistas del TTFT (cliente y worker) | 2, C1 |
| 4 | Histogramas | *Buckets* explícitos alineados con los de vLLM | 3 |
| 5 | Logs | Eventos de ciclo de vida desde el scheduler, log por petición ampliado, niveles definidos, `X-Request-Id` del cliente respetado | — |
| 6 | `/debug` | Se mantiene como está (depuración y `loadgen`) | — |
| 7 | Stack local | `docker-compose` con la plataforma en un contenedor, Prometheus y Grafana con el dashboard aprovisionado como código | 1–3 |
| 8 | Alcance | Fuera: trazas (OpenTelemetry), alertas activas, almacenamiento a largo plazo y agregación de logs. Las alertas y SLO se documentan como propuesta | — |

---

## Decisión 1: librería de métricas

**Recomendación:** `github.com/prometheus/client_golang`, con su versión fijada en `go.mod` al implementar. Es la **primera dependencia externa** del módulo: hasta ahora todo es biblioteca estándar.

| Opción | A favor | En contra |
| :--- | :--- | :--- |
| A. Formato de texto de Prometheus escrito a mano | Sin dependencias | Histogramas, concurrencia y escape de etiquetas hechos a mano: fácil equivocarse y nada que aprender de ello. |
| **B. `client_golang`** | Estándar de facto en Go; histogramas correctos y sin locks; métricas de proceso y del runtime de Go gratis (`go_*`, `process_*`) | Primera dependencia (y sus transitivas); hay que mantenerla actualizada (Dependabot). |
| C. SDK de métricas de OpenTelemetry con exportador Prometheus | Neutral respecto al backend; encaja si luego hay trazas | Más capas y conceptos para el mismo resultado; las trazas están fuera de alcance (decisión 8). |

La dependencia queda confinada al paquete `internal/metrics` y a `cmd/platform` (decisión 2): ni el contrato `inference`, ni `scheduler`, ni `simworker` importan Prometheus.

## Decisión 2: cómo se instrumenta

**Recomendación:** dos mecanismos según el tipo de dato.

1. **Eventos** (algo pasó: una petición terminó, un token llegó, el breaker se abrió) → **interfaces de observación** pequeñas, definidas por quien las consume: `scheduler.Observer` y `httpapi.Observer`. El paquete nuevo `internal/metrics` las implementa con contadores e histogramas de Prometheus. Si no se configura ninguna, se usa una implementación vacía y los tests no cambian.
2. **Estado** (cuánto hay ahora: cola, reservado, salud) → un **`prometheus.Collector`** que, en cada *scrape*, lee `Scheduler.Stats()` y `Scheduler.Snapshots()`. No hay que mantener cada gauge al día en el código: se reutiliza lo que ya calcula `/debug`, y ambos ven lo mismo.

| Opción | A favor | En contra |
| :--- | :--- | :--- |
| Llamar a Prometheus directamente desde `scheduler` y `httpapi` | Menos código | El núcleo depende de Prometheus; los tests tendrían que aislar el registro global. |
| **Interfaces de observación + collector** | El núcleo no conoce Prometheus; patrón ya usado en el proyecto ("la interfaz la define quien la consume"); se puede probar con un observador falso | Una interfaz y un paquete más. |
| Solo collector (leer contadores al hacer *scrape*) | Cero cambios en el núcleo | No sirve para histogramas: necesitan cada observación, no un acumulado. |

Los eventos que necesita el scheduler (ver la guía de observabilidad para el detalle): salida de la cola (admitida, timeout o cancelada, con su espera), colocación (afinidad, desbordamiento o por carga, y bloques acertados), fin de cada stream en un worker (TTFT del worker, ITL por token, resultado), reintento, transición del breaker, cambio de salud y reinicio detectado.

## Decisión 3: qué se mide

**Recomendación:** el catálogo de la guía [observabilidad](../operaciones/03-observabilidad.md#catálogo-de-métricas), organizado en cinco áreas que responden a cinco preguntas:

| Área | Pregunta |
| :--- | :--- |
| Peticiones (lo que ve el cliente) | ¿Responde bien y rápido? |
| Admisión y cola | ¿Estamos rechazando o haciendo esperar, y por qué? |
| Capacidad | ¿Cuánto margen queda en cada worker? |
| Routing por prefijos | ¿Acierta la caché? ¿El router predice bien lo que hay en cada GPU? |
| Salud y resiliencia | ¿Algún worker está fallando, colgado, lento o reiniciándose? |

Puntos clave:

- **Dos TTFT.** El del **cliente** (desde que llega la petición hasta el primer token enviado: incluye cola y reintentos) y el del **worker** (desde que empieza `ExecuteStream` hasta su primer token: prefill puro). Su diferencia es la espera en cola más los reintentos. Sin esta separación, un TTFT alto no dice si falta capacidad o si el worker es lento.
- **Convenciones de Prometheus:** prefijo `llmplatform_`, unidades base (segundos, tokens), `_total` en contadores y etiquetas acotadas (`model`, `worker`, `reason`, `outcome`, `kind`, `state`). Nunca IDs de petición ni prefijos como etiqueta.
- **Nombres paralelos a los de vLLM** (`llmplatform_time_to_first_token_seconds` frente a `vllm:time_to_first_token_seconds`), para poner ambos en el mismo panel en la Fase 6.
- **"Ratio de saturación" del roadmap** = tokens reservados / presupuesto del worker. Se documenta que es la ocupación del **ledger pesimista** (FI-1), no la de la GPU.

## Decisión 4: histogramas

**Recomendación:** histogramas clásicos con ***buckets* explícitos**, alineados con los de vLLM en el rango útil (la guía de observabilidad los lista), para comparar percentiles entre el control plane y el motor en la Fase 6.

| Opción | A favor | En contra |
| :--- | :--- | :--- |
| **Buckets explícitos** | Funcionan con cualquier Prometheus y Grafana; comparables con vLLM | El percentil se interpola dentro del *bucket*: es una aproximación. |
| Histogramas nativos de Prometheus | Resolución automática, sin elegir *buckets* | Requieren activar una función experimental en Prometheus y no son comparables con los de vLLM. |
| *Summaries* (percentiles calculados en el cliente) | Percentiles exactos por proceso | No se pueden agregar entre réplicas ni recalcular para otra ventana. |

## Decisión 5: logs

**Recomendación:**

- **Eventos de ciclo de vida desde el scheduler** (nuevo `Config.Logger`): alta, baja y fin del drenaje de workers; cambios de salud; transiciones del breaker (con motivo y espera); reinicio detectado; reintento de una petición. Son pocos por definición (cambios de estado), así que no generan volumen.
- **Log por petición ampliado:** añade `outcome`, `http_status`, `queue_wait_ms`, `retries` y `worker_ttft_ms` a los campos actuales.
- **Niveles:** `INFO` para el funcionamiento normal (peticiones completadas, altas, cierre del breaker); `WARN` para degradaciones que el sistema absorbe (worker no sano, breaker abierto, cuelgue detectado, reintento, rechazo por cola); `ERROR` solo para lo que llega al cliente como fallo del servidor (`5xx` que no son de carga y errores a mitad de stream).
- **Correlación:** si el cliente envía `X-Request-Id` (hasta 64 caracteres `[A-Za-z0-9._-]`), se usa como ID de la petición; si no, se genera como hoy. Así los logs de la plataforma se cruzan con los del cliente.
- **Sin muestreo:** un log por petición es asumible en local y en el benchmark. Se revisará con tráfico real.

## Decisión 6: qué pasa con `/debug`

**Recomendación:** se mantiene **sin cambios**. Es útil para mirar el estado al momento con `curl`, `loadgen` lo usa en su resumen y los tests lo comprueban. Prometheus y Grafana son la fuente para todo lo que tenga historia; `/debug` es la foto del instante. El *collector* de la decisión 2 lee los mismos datos, así que no pueden contradecirse.

## Decisión 7: stack local

**Recomendación:** un `deploy/observability/docker-compose.yml` con tres servicios, y tareas `task stack:up` / `task stack:down`:

| Servicio | Qué es | Puerto |
| :--- | :--- | :--- |
| `platform` | El binario en un contenedor (Dockerfile multi-etapa, imagen final mínima) | `8080` |
| `prometheus` | *Scrape* de `platform:8080/metrics` cada 5 s, retención corta | `9090` |
| `grafana` | Fuente de datos y dashboards **aprovisionados desde ficheros del repositorio**; acceso anónimo de solo lectura en local | `3000` |

`loadgen` se sigue lanzando desde el host contra `localhost:8080`.

- **Dashboards como código:** el JSON del dashboard vive en el repositorio y Grafana lo carga al arrancar. Un cambio hecho en la UI se exporta y se guarda en el fichero; si no, se pierde al recrear el contenedor.
- **Adelanta parte de la Fase 6:** el Dockerfile del binario estaba en "Empaquetado en Docker". Se hace ahora porque el stack lo necesita.
- **Alternativa:** que Prometheus haga *scrape* del binario en el host (`host.docker.internal:8080`). Es útil para depurar con `task run` y se documentará como variante, pero no como modo principal, porque depende de la red de Docker Desktop.

## Decisión 8: alcance

| Tema | Propuesta | Por qué |
| :--- | :--- | :--- |
| Trazas distribuidas (OpenTelemetry) | Fuera | Con un solo proceso, el recorrido de una petición ya se ve en su log. Tendrá sentido cuando el adaptador hable con vLLM por red (Fase 6). |
| Alertas activas (Alertmanager) | Fuera; **reglas de ejemplo documentadas** en la guía de observabilidad | No hay a quién avisar en local, pero escribir las reglas fija qué consideramos un problema. |
| SLO | Propuesta de SLI y objetivos en la guía de observabilidad | Define qué `503` cuentan como fallo y cuáles son protección de carga. |
| Almacenamiento a largo plazo, Loki | Fuera | Local y efímero. |
| Métricas de vLLM | Fuera hasta la Fase 6 | La guía ya muestra la correspondencia, para que el dashboard pueda añadir sus paneles. |

## Plan de implementación (seguido)

1. **Métricas:** dependencia `client_golang`, paquete `internal/metrics`, interfaces de observación en `scheduler` y `httpapi`, *collector* de estado, `GET /metrics`. Tests del catálogo (cada métrica existe con sus etiquetas) y de los eventos (un observador falso).
2. **Logs:** `Config.Logger` en el scheduler con los eventos de ciclo de vida, log por petición ampliado, `X-Request-Id` del cliente.
3. **Stack:** Dockerfile, `docker-compose`, aprovisionamiento de Prometheus y Grafana, dashboard JSON, tareas de Task.
4. **Validación:** `loadgen` con prefijos y con `-chaos` contra el stack; comprobar que cada panel cuenta lo que dice la guía; capturas y ajustes; actualizar los documentos 1–7, la guía de observabilidad y el README.

## Nueva limitación (registrada en el README raíz, §9)

**FI-10. `/metrics`, `/debug` y la administración comparten puerto con la API pública.** Cualquiera que llegue a la API puede leer métricas, tumbar workers simulados o darlos de baja. Aceptable en local; antes de desplegar (Fase 7), moverlos a un puerto de administración solo accesible dentro del clúster.

---

## Implementación (2026-11-18)

| Decisión | Dónde | Cómo quedó |
| :--- | :--- | :--- |
| 1. Librería | `go.mod` | `github.com/prometheus/client_golang v1.23.2`. Es la última versión que declara `go 1.23` (la `v1.25.0` exige Go 1.26), así que el módulo sigue en su Go mínimo y los tests `-race` en `golang:1.23`. |
| 2. Instrumentación | `scheduler/observe.go`, `httpapi/observe.go`, `internal/metrics` | `scheduler.Observer` (9 eventos) y `httpapi.Observer` (`RequestStarted`, `RequestFinished` con un `RequestRecord`); por defecto vacíos. `metrics.Metrics` los implementa; `Watch(sched)` registra el *collector* de estado. |
| 3. Catálogo | `internal/metrics` | Las métricas de la guía de observabilidad, con los nombres, etiquetas y tipos que allí se listan. Un test comprueba que existen todas y pasa el *linter* de Prometheus (`testutil.GatherAndLint`). |
| 4. Histogramas | `metrics.go` | *Buckets* de la guía de observabilidad. |
| 5. Logs | `scheduler` (`Config.Logger`), `httpapi` | Eventos de ciclo de vida con `component=scheduler`; log por petición con `outcome`, `http_status`, `queue_wait_ms`, `retries`, `worker_ttft_ms`; niveles por `outcome`; `X-Request-Id` del cliente. |
| 6. `/debug` | — | Sin cambios. |
| 7. Stack | `platform/Dockerfile`, `deploy/observability/`, `Taskfile.yml` | Imagen multi-etapa (`golang:1.23` → `distroless/static:nonroot`); Prometheus `v3.5.0`, Grafana `12.1.0`; `task stack:up`, `stack:down`, `stack:logs`. |
| 8. Alcance | — | Sin trazas ni Alertmanager. |

### Decisiones de implementación tomadas por el camino

| Tema | Elegido | Por qué |
| :--- | :--- | :--- |
| Contadores a 0 desde el primer *scrape* | El *collector* crea a 0 los contadores de eventos de cada worker y modelo conocidos | Una serie que nace con valor 1 en su primer evento es invisible para `increase()`. Lo destapó la validación: la anotación "breaker abierto" salía vacía para la primera apertura de cada worker. Lo cubre `TestEventCountersStartAtZero`. |
| "Ha llegado un token" | Un indicador explícito (`RequestRecord.FirstToken`), no `TTFT > 0` | En Windows el reloj monotónico avanza a saltos de ~0,5 ms: con un worker instantáneo el TTFT medía 0 y la petición no entraba en el histograma. Lo destapó un test. |
| `admissions_total` | Cuenta leases, incluido el de cada reintento | Es lo que ve el scheduler; documentado en el catálogo. |
| `model` desconocido | Etiqueta `unknown` si ningún worker sirve el modelo (también en JSON inválido) | Si no, un cliente crearía series a voluntad. |
| Reglas de alerta | Cargadas en Prometheus (visibles en `/alerts`) sin Alertmanager | Valida su sintaxis (`promtool`) y que se disparan cuando deben, sin avisar a nadie. |
| Dashboard | JSON aprovisionado, sin guardar desde la UI; cada panel lleva su explicación en la descripción (ⓘ) | Dashboards como código; la guía y el dashboard dicen lo mismo. |
| Línea de tiempo del breaker | `sum by (worker)` antes de sumar los estados, colores por umbrales | La primera versión sumaba series que difieren en `state`, que PromQL no empareja, y el panel salía vacío; los colores de los *value mappings* no se aplicaban. Ambos detectados al validar. |
| Logs del scheduler bajo su lock | Se escriben con el lock tomado | Son pocos (cambios de estado); `request retried` y `worker hung` se escriben fuera del lock. |

### Validación

Tests del catálogo y de los eventos, y validación del stack consulta a consulta contra Prometheus: ver [resultados](../operaciones/04-resultados-de-pruebas.md#fase-5-validación-del-stack-de-observabilidad) y la [suite de tests](../operaciones/02-tests.md).

---

## Qué pasó después

Este documento es el registro de la fase tal como se decidió; lo que anunciaba para fases posteriores se resolvió así (estado al cerrar el proyecto, 2026-11-21):

| Lo que quedó dicho aquí | Qué pasó |
| :--- | :--- |
| FI-10: `/metrics`, `/debug` y la administración en el puerto público (conflicto C2). | **Resuelta en la Fase 7** con `-admin-addr`: en Kubernetes, el Service público solo expone la API. El puerto de administración no tiene autenticación. |
| Las métricas de vLLM quedaban para la Fase 6. | Hecho en la 6a: fila *6 · Motor (vLLM)* del dashboard y Prometheus leyendo el `/metrics` de los motores; verificado con vLLM real en la 6b-CPU. |
| Las trazas tendrían sentido cuando el adaptador hablara con vLLM por red. | Siguen fuera: el log por petición (con `worker_id`, `queue_wait_ms`, `retries`, `worker_ttft_ms`) bastó para todas las pruebas. Están en la lista de lo que falta para producción ([análisis final](../analisis-final.md#distancia-a-producción-con-gpus)). |
| *Buckets* de los histogramas alineados con los de vLLM. | En CPU, el TTFT p90 superó el último *bucket* (40 s); se aceptó porque en GPU no hace falta (6b-CPU). |
| Stack local con Docker Compose. | El mismo dashboard y las mismas alertas se despliegan también en Kubernetes con el chart de Helm (Fase 7), sin copias: se pasan con `--set-file`. |
