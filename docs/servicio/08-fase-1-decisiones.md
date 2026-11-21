# 8. Fase 1: fundaciones y streaming — decisiones de diseño

La Fase 1 construyó la base: el contrato entre control plane y workers, el worker simulado, la API con streaming y la cancelación de extremo a extremo. **No tuvo un documento de decisiones propio cuando se hizo** (el formato "un documento por fase" empezó en la Fase 2). Sus decisiones quedaron registradas en la tabla de fricciones del [README raíz (§2)](../../README.md#2-puntos-de-fricción-iniciales-y-decisiones-arquitectónicas), en los documentos 01–07 de esta carpeta y como limitaciones conscientes (FI-n) en el README §9.

Este documento las reúne **a posteriori**, con el mismo formato que las demás fases: el problema, las alternativas, la opción elegida, por qué, y qué pasó después con cada una. No introduce decisiones nuevas; cada punto enlaza con el documento donde se explica en detalle.

> **Estado:** implementada (2026-11-17, primer commit del repositorio, junto con la Fase 2). Documento reconstruido el 2026-11-21 a partir de lo ya registrado.

## Resumen

| # | Decisión | Opción elegida | Alternativa descartada | Qué pasó después |
| :--- | :--- | :--- | :--- | :--- |
| 1 | Forma del contrato | **Streaming-first**: un solo camino de ejecución (`ExecuteStream`); la respuesta sin streaming es el mismo stream acumulado | Petición/respuesta unaria (`Infer`) y streaming añadido después | Se mantuvo. El adaptador de vLLM (6a) es un stream SSE sobre el mismo contrato. |
| 2 | Admisión | **Lease atómico** (`AcquireLease`): comprobar y reservar en una sola operación dentro del worker | Leer el estado de todos, elegir el más libre y enviar | Se mantuvo; es la base de todo lo demás. Desde la 6a vive en `internal/ledger`, compartido por todos los workers. |
| 3 | Unidad de capacidad | **Tokens** (`prompt + max_new_tokens`) frente a un presupuesto que representa la KV-Cache | Peticiones en vuelo (`in_flight / max_concurrency`) | Se mantuvo. Con vLLM, el presupuesto se **descubre** de la KV-Cache real (6a). |
| 4 | Contabilidad | **Reserva pesimista** (el peor caso durante todo el stream) en el control plane | Seguir la ocupación real de la KV-Cache del motor | Atajo consciente (**FI-1**). Medido en la 6b-CPU: reserva ~3× lo que vLLM ocupa. Sigue abierto. |
| 5 | Momento de la admisión | **Antes de escribir nada al cliente**, para poder rechazar con un `503` limpio | Responder `200` y negociar después | Se mantuvo. La Fase 4 lo afinó: las cabeceras se retrasan hasta el primer token (resolvió **FI-9**). |
| 6 | Vida del lease | **TTL** entre `AcquireLease` y `ExecuteStream`, como red de seguridad; `Release` idempotente y en `defer` | Confiar en que el handler siempre libere | Se mantuvo; ganó importancia con la cola (Fase 2) y con workers remotos (6a). |
| 7 | Cancelación | **`ctx.Done()` hasta el worker**, escuchado en los dos puntos donde puede esperar (pausa entre tokens y envío al canal) | Detectar la desconexión solo al escribir | Se mantuvo. Con vLLM, cerrar la conexión aborta la generación en el motor (verificado en la 6b-CPU). |
| 8 | API pública | **Compatible con OpenAI** (subconjunto de `/v1/chat/completions` con SSE) | API propia o gRPC | Se mantuvo. Permitió usar los mismos clientes contra vLLM y que el adaptador sea casi un *passthrough*. |
| 9 | Topología | **Un binario y una réplica**, con el estado en memoria, pero con el código separado en control plane y data plane por el contrato `inference` | Varias réplicas con estado compartido (Redis) desde el principio | Atajo consciente (**FI-2**). La Fase 7 lo hizo explícito en el despliegue (`Recreate`). Sigue abierto. |
| 10 | Workers sin GPU | **Worker simulado en Go**: presupuesto de tokens, TTFT e ITL, salida determinista por ID | Empezar directamente con vLLM o con un mock trivial | Se mantuvo como herramienta de pruebas; la Fase 3 le añadió caché de prefijos y la 4, fallos inyectables. |
| 11 | Coste de una petición | **Estimar tokens** (~4 caracteres por token), sin tokenizer | Tokenizer por modelo o `/tokenize` de vLLM antes de admitir | Atajo consciente (**FI-3**). Medido en la 6b-CPU: −5 % en prompts largos; la respuesta usa los tokens reales del motor. |
| 12 | Qué viaja en el contrato | **Prompt aplanado** y **un chunk = un token** | Mensajes estructurados, parámetros de muestreo y `usage` del motor | Atajos conscientes (**FI-7**, **FI-8**), resueltos en la 6a. |
| 13 | Módulo y dependencias | **Módulo Go en `platform/`**, Go 1.23 como mínimo, **sin dependencias externas** | Módulo en la raíz; librerías de terceros desde el inicio | La primera dependencia llegó en la Fase 5 (`prometheus/client_golang`) y es la única. |

---

## Decisión 1: streaming-first

**Problema:** casi todo el tráfico de un LLM es streaming. Un sistema pensado para petición/respuesta acaba reescribiendo el manejo de red, los buffers y la cancelación cuando llega el streaming.

| Opción | A favor | En contra |
| :--- | :--- | :--- |
| Unaria (`Infer`) y streaming después | Más simple al principio | Dos caminos de ejecución; la cancelación y los errores a mitad de respuesta se diseñan dos veces. |
| **Streaming-first** | Un único camino de ejecución en el data plane | Más complejo desde el día uno (canales, goroutines, errores en banda). |

**Elegida:** streaming-first. `Lease.ExecuteStream` emite `TokenChunk` por un canal; la respuesta sin streaming (`stream: false`) usa el mismo mecanismo y solo acumula el texto. Ver [02 · Sin streaming](02-recorrido-de-una-peticion.md#fase-b--sin-streaming-stream-false).

## Decisión 2: lease atómico

**Problema:** con concurrencia, "mirar el estado de los workers, elegir el más libre y enviar" sobrecompromete: dos peticiones ven el mismo hueco y lo ocupan las dos.

| Opción | A favor | En contra |
| :--- | :--- | :--- |
| Mirar y elegir | Sencillo | Condición de carrera; o un lock global en el control plane. |
| **Lease atómico** (`AcquireLease`) | Comprobar y reservar es una operación atómica dentro del worker; sin lock global | Un objeto con ciclo de vida (concedido, en ejecución, liberado) que hay que gestionar bien. |

**Elegida:** lease atómico. Si dos peticiones compiten, una obtiene el lease y la otra `ErrAtCapacity`, y se prueba otro worker. La colocación solo *intenta*; nunca decide con datos obsoletos. Ver [03 · Por qué un lease](03-leases-y-capacidad.md#por-qué-un-lease-y-no-mirar-el-estado-y-elegir).

## Decisión 3: capacidad en tokens

**Problema:** en un motor de LLM lo que se agota es la memoria de KV-Cache, que crece con los tokens de cada secuencia, no con el número de peticiones. Un prompt de 8 000 tokens no ocupa lo mismo que uno de 100.

**Elegida:** un **presupuesto de tokens por worker** (`TokenBudget`), y cada petición cuesta `prompt + max_new_tokens`. Descartado: contar peticiones en vuelo, que trataría igual peticiones de costes muy distintos. Ver [03 · La unidad de capacidad](03-leases-y-capacidad.md#la-unidad-de-capacidad-tokens).

## Decisión 4: reserva pesimista

**Problema:** vLLM no ofrece una API de reserva; la ocupación real de su KV-Cache solo se conoce después.

| Opción | A favor | En contra |
| :--- | :--- | :--- |
| Ocupación real del motor | Aprovecha la GPU | No es atómica: hay que confiar en la *preemption* del motor como red de seguridad; métricas con retraso. |
| **Reserva pesimista** en el control plane | Simple, segura y atómica sin ayuda del motor; nunca provoca *preemption* | Infrautiliza la GPU cuando los clientes piden `max_tokens` mucho mayores de lo que generan. |

**Elegida:** reserva pesimista, como atajo consciente registrado en **FI-1** con sus alternativas (*overbooking* con la ocupación real como red de seguridad, descontar prefijos en caché). La 6b-CPU midió su coste: el ledger reservó de media ~3 veces lo que vLLM ocupaba.

## Decisión 5: admitir antes de responder

**Problema:** una vez enviado `200 OK`, el código de estado ya no se puede cambiar.

**Elegida:** obtener el lease **antes** de escribir nada. Así se puede rechazar con un `503` que clientes, balanceadores y herramientas de benchmark entienden y reintentan. Ver [02 · Antes de escribir nada al cliente](02-recorrido-de-una-peticion.md#fase-a--antes-de-escribir-nada-al-cliente).

**Lo que quedó pendiente (FI-9):** en la Fase 1 las cabeceras `200` se enviaban justo después del lease, antes de que el worker respondiera; un rechazo del motor al empezar habría llegado como error dentro del stream. La Fase 4 retrasó las cabeceras hasta el primer token ([11 · Decisión 4](11-fase-4-decisiones.md#decisión-4-reintentos-y-cabeceras-retrasadas-resuelve-fi-9)), lo que además permitió reintentar en otro worker sin que el cliente lo note.

## Decisión 6: TTL del lease y liberación garantizada

**Problema:** un lease concedido que nunca se ejecuta (el handler falla, el proceso que lo pidió muere) dejaría capacidad reservada para siempre.

**Elegida:**

- `Release()` en `defer` en el handler, idempotente: liberar dos veces no devuelve los tokens dos veces.
- Un **TTL** (`-lease-ttl`, 5 s) entre `AcquireLease` y `ExecuteStream`: si vence, el worker recupera la capacidad por su cuenta. En la Fase 1 nunca vence en condiciones normales (ambas llamadas ocurren en el mismo handler); es una red de seguridad que gana sentido con la cola y con workers remotos.
- Una máquina de estados explícita (concedido → en ejecución → liberado) que devuelve los tokens exactamente una vez.

Ver [03 · Máquina de estados](03-leases-y-capacidad.md#máquina-de-estados-del-lease-internalledger) y [03 · El TTL](03-leases-y-capacidad.md#el-ttl-del-lease). El caso de una réplica que muere *a mitad* de stream necesita renovación de leases, registrado en FI-2.

## Decisión 7: cancelación de extremo a extremo

**Problema:** en un LLM, cada petición abandonada que siga generando ocupa memoria de la GPU para nada.

**Elegida:** el contexto de la petición llega hasta el worker. La desconexión se detecta por dos vías (`net/http` cancela el contexto, o falla una escritura y el handler llama a `cancel()`), y el worker escucha `ctx.Done()` en sus dos puntos de espera. El handler sigue vaciando el canal tras un fallo de escritura para que el productor termine. Resultado: la capacidad se libera al instante aunque la latencia entre tokens sea larga o el cliente esté atascado. Ver [02 · Cancelación](02-recorrido-de-una-peticion.md#cancelación-el-cliente-se-va-a-mitad-de-respuesta).

Las reglas de concurrencia que lo hacen seguro (un único productor que cierra el canal, `errc` con buffer 1, un único orden de locks) están en [07 · Modelo de concurrencia](07-concurrencia.md#modelo-de-concurrencia).

## Decisión 8: API compatible con OpenAI

**Problema:** elegir el formato de la API pública.

| Opción | A favor | En contra |
| :--- | :--- | :--- |
| API propia | Libertad total | Ningún cliente existente la habla; habría que escribir adaptadores y herramientas de carga. |
| gRPC | Streaming nativo, tipado | Los clientes de LLM y las herramientas de benchmark hablan HTTP con el formato de OpenAI. |
| **OpenAI (subconjunto, con SSE)** | Lo hablan vLLM, los SDK de OpenAI, `llmperf` y casi cualquier cliente de LLM | Hay que decidir qué subconjunto se soporta y rechazar el resto con claridad. |

**Elegida:** compatible con OpenAI, solo con lo que el control plane necesitaba. Ver [05 · Por qué compatible con OpenAI](05-api-http.md#por-qué-compatible-con-openai). El objetivo del README hablaba de "gRPC/SSE"; se eligió SSE sobre HTTP por esta compatibilidad.

## Decisión 9: un binario, una réplica, dos planos

**Problema:** cómo empezar sin cerrar la puerta a separar los componentes después.

**Elegida:** un único proceso (`cmd/platform`) con el estado en memoria, pero con el código separado en **control plane** (API, admisión, colocación) y **data plane** (workers). La frontera es el contrato `inference` (`Worker`, `Lease`): el control plane nunca habla con `simworker` directamente. Ver [01 · Un binario, dos planos](01-arquitectura.md#un-binario-dos-planos).

- **Por qué una sola réplica:** la atomicidad del lease depende de que un único proceso sea la puerta de entrada a cada worker. Varias réplicas tendrían cada una su propia visión de la capacidad. Introducir Redis para compartir estado estaba explícitamente en los *no objetivos* iniciales. Registrado como **FI-2**, con sus opciones (particionar los workers, estado compartido, *sidecar* de admisión por GPU).
- **Lo que confirmó después:** pasar del simulador a vLLM real (6a, 6b-CPU) no cambió ni el scheduler ni la API, solo añadió un adaptador que implementa el mismo contrato.

## Decisión 10: worker simulado

**Problema:** el proyecto empieza sin GPU, y hace falta probar admisión, streaming, cancelación y concurrencia con miles de peticiones.

**Elegida:** `simworker`, que imita lo que importa al control plane: un presupuesto de tokens, un TTFT que crece con el prompt, una ITL entre tokens y una salida determinista por ID de petición (la misma petición produce siempre el mismo texto, lo que facilita depurar). Las esperas usan un único temporizador reutilizable, interrumpible por el contexto. No simula *continuous batching* ni *preemption*. Ver [04](04-worker-simulado.md) y lo que no captura en [06](06-relacion-con-vllm.md#lo-que-la-simulación-no-captura-y-por-qué-importa).

## Decisiones 11 y 12: atajos conscientes en el contrato

Tres simplificaciones que la Fase 1 tomó sabiendo que vLLM las haría insuficientes, registradas como limitaciones con sus opciones:

| Atajo | Por qué se tomó | Registro | Qué pasó |
| :--- | :--- | :--- | :--- |
| Estimar tokens (~4 caracteres por token) | Tokenizar exige el tokenizer de cada modelo o una llamada al motor antes de elegir worker | FI-3 | Medido en la 6b-CPU: −5 % en prompts largos, ~28 tokens de la *chat template* en los cortos. Aceptado. |
| `InferenceRequest` solo lleva el prompt aplanado | Al simulador le basta para estimar el coste | FI-7 | Resuelto en la 6a: mensajes estructurados y parámetros de muestreo en el contrato. |
| Cada `TokenChunk` cuenta como un token | Cierto en el simulador | FI-8 | Resuelto en la 6a: `usage` real del motor en el último chunk. |

## Decisión 13: módulo y dependencias

- **Módulo Go en `platform/`**, separado del resto del repositorio (README, `docs/`, `Taskfile.yml`, y más tarde `deploy/`). Así, los Dockerfiles, el chart de Helm y la documentación viven junto al módulo sin mezclarse con él. Ver [01 · Paquetes](01-arquitectura.md#paquetes).
- **Go 1.23 como versión mínima**, y los tests de carreras se ejecutan con ella (`task test:race`, en un contenedor `golang:1.23`). La semántica de temporizadores de Go ≥ 1.23 es la que hace seguro el temporizador reutilizable del simulador.
- **Sin dependencias externas:** solo la biblioteca estándar. La primera llegó en la Fase 5 (`prometheus/client_golang`, [12 · Decisión 1](12-fase-5-decisiones.md#decisión-1-librería-de-métricas)) y sigue siendo la única.

## Lo que la Fase 1 dejó para la siguiente

La Fase 1 resolvía *si* una petición cabe en un worker, pero no *qué hacer cuando no cabe*: un `Pool` intentaba obtener el lease y, si no había sitio, respondía `503` de inmediato. Sin cola, un pico de pocos segundos que una cola corta habría absorbido se convertía en rechazos. La [Fase 2](09-fase-2-decisiones.md) lo sustituyó por el scheduler con colas acotadas por modelo y separó admisión (cuándo) y colocación (dónde).
