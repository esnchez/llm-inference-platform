# 15. Fase 7: Kubernetes en local — decisiones de diseño

Este documento recoge las decisiones de la Fase 7: desplegar la plataforma en Kubernetes en local, con descubrimiento de workers y autoscaling. Mantiene el criterio de la Fase 6b-CPU: **un sistema funcional y sencillo, con sus limitaciones documentadas**, frente a uno completo y complejo.

> **Estado (2026-11-21): implementada y validada en minikube.** Se aplicaron las ocho decisiones recomendadas. Resultados: [operaciones · resultados](../operaciones/04-resultados-de-pruebas.md#fase-7-kubernetes-en-minikube).
>
> **Código:** `-admin-addr` y `-vllm-dns` en `cmd/platform`; observador de DNS en `internal/vllm/dnswatch.go`; apagado ordenado en `cmd/fakevllm`; chart en `deploy/helm/llm-platform`; tareas `k8s:*` en el `Taskfile.yml`; CI en `.github/workflows/ci.yml`.

## Qué cambia respecto al roadmap original

El roadmap pedía tres cosas: despliegue con Helm, almacenamiento compartido para los pesos y un **controlador propio** de autoscaling basado en la ocupación de la KV-Cache.

| Punto del roadmap | Qué se hace | Por qué |
| :--- | :--- | :--- |
| Despliegue con Helm | **Sí:** un chart | Es el formato estándar y permite instalar todo con un comando. |
| Almacenamiento compartido para pesos | **No** | Sin GPUs ni modelos de decenas de GB, no aporta nada; el vLLM en CPU usa un volumen de caché (6b-CPU). |
| Controlador propio de autoscaling | **No: KEDA** | Escribir un operador (CRD, reconciliación, RBAC, `client-go`) es mucho código para demostrar lo mismo. KEDA escala un Deployment a partir de una consulta de Prometheus, que son exactamente nuestras métricas. |

## Resumen

| # | Decisión | Opción elegida | Alternativa descartada |
| :--- | :--- | :--- | :--- |
| 1 | Clúster local | minikube, perfil propio `llm-platform` (driver docker) | `kind`: no está instalado; minikube sí |
| 2 | Empaquetado | Un chart de Helm (`deploy/helm/llm-platform`): plataforma, vLLM falsos, vLLM en CPU opcional, Prometheus, Grafana | kube-prometheus-stack: cientos de recursos para dos servicios |
| 3 | Descubrimiento de workers | DNS de un Service *headless*: el conector resuelve sus IPs cada 10 s | *Watch* de la API de Kubernetes con `client-go` |
| 4 | Bajas sin cortar streams | El vLLM falso, al recibir `SIGTERM`, deja de anunciarse sano y termina sus streams; la plataforma lo drena al desaparecer del DNS | `preStop` con un `sleep` fijo |
| 5 | Autoscaling | KEDA con una consulta de Prometheus (demanda en tokens / capacidad objetivo por réplica) | Controlador propio, HPA con *custom metrics adapter* |
| 6 | Puerto de administración (FI-10) | `-admin-addr`: `/metrics` y `/debug/*` en otro puerto; el Service público solo expone la API | Autenticación en esas rutas |
| 7 | Réplicas del control plane (FI-2) | **Una**, con estrategia `Recreate` | `RollingUpdate`: tendría dos réplicas a la vez durante cada actualización |
| 8 | Alcance | Fuera: operador propio, varias réplicas, Ingress/TLS, autoscaling del vLLM en CPU, persistencia de Prometheus | — |

---

## Decisión 1: clúster local

**minikube** con un **perfil propio** (`minikube start -p llm-platform`). Así no se toca ningún otro clúster de la máquina, y todos los comandos usan el contexto `llm-platform` de forma explícita (`kubectl --context llm-platform`, `helm --kube-context llm-platform`). Las imágenes se construyen con Docker en local y se cargan con `minikube -p llm-platform image load`, sin registro.

## Decisión 2: un chart de Helm

`deploy/helm/llm-platform`, con valores para activar o desactivar cada parte:

| Componente | Recursos | Notas |
| :--- | :--- | :--- |
| Plataforma | Deployment (1 réplica, `Recreate`), Service `platform` (API, puerto 8080), Service `platform-admin` (8081) | Lee los workers del DNS de `fakevllm` (y de `vllm-cpu` si está activado). |
| vLLM falsos | Deployment + Service *headless* `fakevllm` | Lo que escala KEDA. |
| vLLM en CPU | Deployment + Service *headless* `vllm-cpu` + PVC para la caché del modelo | **Desactivado por defecto** (imagen de 1,8 GB, ~6 GB de memoria y ~6 min de arranque). Sin autoscaling. |
| Prometheus | Deployment + ConfigMap | Descubre los pods por el DNS de los Services *headless* (`dns_sd_configs`). Datos efímeros. |
| Grafana | Deployment + ConfigMaps | El **mismo** dashboard y las **mismas** alertas que el stack de Docker: se pasan al chart con `--set-file`, así que no hay copias. |

## Decisión 3: descubrimiento por DNS

Un Service *headless* (`clusterIP: None`) responde con **una IP por pod listo**. El flag nuevo `-vllm-dns fakevllm.<ns>.svc.cluster.local:8000` hace que la plataforma resuelva ese nombre cada 10 s:

- Una IP nueva arranca un **conector** de la Fase 6a: descubre el pod y lo registra cuando está listo.
- Una IP que desaparece se da de **baja con drenaje** (Fase 4): no recibe peticiones nuevas y desaparece cuando terminan sus streams.

| Opción | A favor | En contra |
| :--- | :--- | :--- |
| ***Watch* de la API con `client-go`** | Notificación inmediata y estados detallados | Una dependencia enorme, RBAC y código de reconciliación. |
| **DNS del Service *headless*** | Cero dependencias y cero permisos; reutiliza el conector de la 6a | Hasta 10 s de retraso en ver un pod nuevo o desaparecido. Con arranques de minutos (vLLM real) es irrelevante. |

## Decisión 4: bajas sin cortar streams

Al reducir réplicas, Kubernetes envía `SIGTERM` al pod y lo quita de los *endpoints* del Service, y por tanto del DNS. El vLLM falso, al recibir `SIGTERM`:

1. Deja de responder `200` en `/health`.
2. Sigue sirviendo los streams que tiene en curso.
3. Se apaga cuando terminan (`http.Server.Shutdown`), dentro de `terminationGracePeriodSeconds` (60 s).

La plataforma deja de enviarle peticiones en cuanto lo ve desaparecer del DNS o fallar el healthcheck, y lo drena. **Con vLLM real** no se ha verificado cómo se comporta ante `SIGTERM` con streams en curso; como el vLLM en CPU no escala, queda documentado.

## Decisión 5: autoscaling con KEDA

Un `ScaledObject` sobre el Deployment `fakevllm`, con un disparador de Prometheus:

```promql
sum(llmplatform_worker_tokens_reserved) + sum(llmplatform_queue_tokens)
```

Es la **demanda en tokens**: lo reservado ahora más lo que espera en cola. El umbral es la capacidad objetivo por réplica (70 % del presupuesto de un vLLM falso). KEDA calcula `réplicas = ⌈demanda / umbral⌉` entre un mínimo de 1 y un máximo de 4. La bajada la gobierna la estabilización del HPA que crea KEDA (30 s en la demo; Kubernetes usa 300 s por defecto); el `cooldownPeriod` de KEDA (60 s) solo cuenta para escalar a cero, que aquí no se usa.

| Opción | A favor | En contra |
| :--- | :--- | :--- |
| HPA por CPU | Nativo | La CPU de un worker de inferencia no dice nada de su capacidad (la tesis del proyecto desde el README §1). |
| Controlador propio | Control total | Mucho código para lo mismo. |
| HPA + *Prometheus adapter* | Nativo | Configuración del adaptador compleja y frágil. |
| **KEDA (disparador Prometheus)** | Un `ScaledObject` de 20 líneas sobre nuestras propias métricas | Un componente más que instalar (con Helm). |

**Por qué la demanda y no `vllm:kv_cache_usage_perc`:** la cola solo crece cuando el ledger está lleno. Escalar por demanda (reservado + en cola) reacciona a lo que la plataforma ve, y funciona igual con el simulador, con vLLM falso o con vLLM real. La ocupación real del motor sería la señal correcta si la reserva dejara de ser pesimista (FI-1).

## Decisión 6: puerto de administración (FI-10)

Flag nuevo `-admin-addr`. Si se define, `/metrics`, `/debug/scheduler`, `/debug/workers` y los endpoints de administración de workers simulados se sirven **solo** en ese puerto, y el de la API solo sirve la API (y `/healthz`). Sin el flag, todo sigue en el mismo puerto, como hasta ahora (compatibilidad con `loadgen`, el stack de Docker y los tests). En Kubernetes, el Service público expone solo el 8080; el 8081 solo lo alcanza Prometheus dentro del clúster.

## Decisión 7: una réplica con `Recreate` (FI-2)

La plataforma se despliega con **una réplica** y estrategia `Recreate`. Con `RollingUpdate`, Kubernetes arrancaría la réplica nueva antes de parar la vieja, y durante unos segundos habría **dos ledgers** frente a los mismos workers, cada uno creyéndose la única puerta de entrada (FI-2). Con `Recreate` hay un corte breve en cada despliegue, pero el ledger nunca se duplica. Es la limitación de alta disponibilidad hecha explícita en el despliegue.

## Decisión 8: alcance

Fuera: operador propio (sustituido por KEDA), varias réplicas (FI-2), Ingress y TLS (se accede con `kubectl port-forward`), autoscaling del vLLM en CPU (6 minutos de arranque lo hacen inútil), persistencia de Prometheus y Grafana.

## Plan de implementación

1. **Plataforma:** `-admin-addr` (6) y `-vllm-dns` con el observador de DNS (3); apagado ordenado del vLLM falso (4). Con tests.
2. **Chart** (2), con KEDA (5) y la estrategia `Recreate` (7).
3. **Tareas:** `task k8s:up` (perfil de minikube, imágenes, KEDA, chart), `task k8s:down`, `task k8s:port-forward`.
4. **Validación:** carga con `loadgen` → KEDA añade vLLM falsos → la plataforma los registra; se quita la carga → KEDA reduce → la plataforma drena sin errores; el dashboard lo muestra.
5. **Documentación:** resultados, README y análisis final.

---

## Implementación

| Pieza | Qué hace | Tests |
| :--- | :--- | :--- |
| `-admin-addr` | `httpapi` separa `RegisterPublic` (API y `/healthz`) de `RegisterDebug` (`/debug/*`). Con el flag, `/debug/*`, `/metrics` y la administración de workers simulados van a un segundo `http.Server`; sin él, todo sigue en `-addr`. | `TestPublicAndDebugRoutesSplit` |
| `-vllm-dns host:port` | `vllm.DNSWatcher` resuelve el nombre cada `-vllm-dns-interval` (10 s). IP nueva → un `Connector` (Fase 6a) con su propio contexto. IP desaparecida → se cancela el conector, que al salir da de baja su worker (`RemoveOnStop`): drenaje de la Fase 4. Un error del resolvedor **no** da de baja a nadie (evita vaciar el pool por un fallo de DNS); `NXDOMAIN` sí, porque un Service sin pods listos no tiene registros. | `TestDNSWatcherFollowsTheAnswer` (alta, error transitorio, baja, `NXDOMAIN`; dos vLLM falsos en `127.0.0.1` y `127.0.0.2` con el mismo puerto, como dos pods), `TestIsNotFound` |
| Apagado del vLLM falso | Al recibir `SIGTERM`: `/health` deja de responder `200`, `http.Server.Shutdown` espera a los streams en curso (`-shutdown-grace`, 50 s; el pod da 60 s). | Validación en el clúster |
| Chart | Lo descrito en la decisión 2. La plataforma no lleva la lista de workers: solo `-vllm-dns` con los Services *headless* activos. El *readiness* del vLLM falso es su `/health`, así que un pod solo entra en el DNS cuando "ha cargado el modelo". | `helm lint` (también en la CI) |
| KEDA | `ScaledObject` con la consulta de la decisión 5 y `or vector(0)` para que esté definida sin workers ni colas. Estabilización de bajada del HPA en 30 s (la de Kubernetes es 300 s) para que la demostración sea rápida. | Validación en el clúster |
| `loadgen -admin-url` | Con `-admin-addr`, los informes de `/debug/*` y el caos se piden al puerto de administración. | — |
| CI | GitHub Actions: `gofmt`, `go vet`, `go test -race -count=2` en Go 1.23 y `helm lint` del chart. | — |

## Resultados de la validación

En minikube: el autoscaling pasa de 1 a 4 vLLM falsos en ~40 s y vuelve a 1 en ~50 s; con carga que el autoscaling cubre, 95 % `200` y **0 errores dentro del stream**, también al borrar un pod con streams en curso; `/metrics` y `/debug` dan `404` en el Service público. Montaje, tablas y lo que destapó la validación en [resultados](../operaciones/04-resultados-de-pruebas.md#fase-7-kubernetes-en-minikube).

## Qué sí y qué no

| Sí | No |
| :--- | :--- |
| Un chart que instala todo con un comando, con el mismo dashboard y las mismas alertas que el stack de Docker | Operador propio (lo sustituye KEDA) |
| Descubrimiento por DNS, sin dependencias ni permisos | *Watch* de la API de Kubernetes |
| Autoscaling por demanda en tokens, validado de 1 a 4 y de vuelta a 1 | Autoscaling del vLLM en CPU (6 min de arranque); escalar a cero |
| Bajas sin errores visibles para el cliente | Ventana de DNS eliminada (se cubre con reintentos) |
| Puerto de administración separado (FI-10) | Autenticación en ese puerto; Ingress y TLS |
| Una réplica con `Recreate` (FI-2 explícito) | Alta disponibilidad del control plane |
| vLLM en CPU en el chart, desactivado por defecto (se renderiza y pasa `helm lint`) | Arrancarlo en minikube (pide ~6 GB de memoria): vLLM en CPU se validó en Docker en la 6b-CPU |
| CI con tests, carreras y `helm lint` | Despliegue continuo, imágenes publicadas en un registro |
