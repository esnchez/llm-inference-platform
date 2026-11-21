# 1. Entorno local

Cómo levantar cada entorno de prueba del proyecto, de menos a más realista, y los problemas conocidos de la máquina de desarrollo. Todas las órdenes están en el `Taskfile.yml` de la raíz (`task --list` las muestra); se ejecutan desde la raíz del repositorio.

| Entorno | Para qué | Necesita | Arranque |
| :--- | :--- | :--- | :--- |
| [Workers simulados](#plataforma-con-workers-simulados) | Lógica del control plane, comparar configuraciones | Go 1.23+ | Segundos |
| [Stack de observabilidad](#stack-de-observabilidad-docker) | Métricas, dashboard y alertas | Docker | ~1 min |
| [vLLM falsos](#vllm-falsos) | El camino HTTP real del adaptador, con fallos inyectables | Docker | ~15 s |
| [vLLM real en CPU](#vllm-real-en-cpu) | Integración con vLLM de verdad, sin GPU | Docker, CPU con AVX2, ~5 GB de RAM por instancia | ~6 min por instancia |
| [Kubernetes en local](#kubernetes-en-local) | Despliegue con Helm, descubrimiento por DNS y autoscaling | Docker, minikube, Helm, kubectl | ~3 min |

## Requisitos

- **Go 1.23+** (la versión mínima del módulo; los tests de carreras corren con ella).
- **[Task](https://taskfile.dev)**, opcional pero recomendado: cada tarea ya se ejecuta dentro de `platform/`.
- **Docker** para los tests de carreras (`task test:race` usa un contenedor `golang:1.23`), los stacks y minikube.
- **minikube, Helm y kubectl** solo para Kubernetes.

## Plataforma con workers simulados

Sin Docker. El simulador imita una GPU (presupuesto de tokens, prefill, decode, caché de prefijos y fallos inyectables; ver [servicio · 04](../servicio/04-worker-simulado.md)).

```bash
task run -- -workers 3 -token-budget 16384 -itl 20ms   # control plane + 3 workers simulados en :8080
task run -- -h                                         # resto de opciones
task loadgen -- -rate 30 -duration 10s                 # carga realista (Poisson, chat + RAG, cancelaciones)
task loadgen -- -rate 4 -duration 60s -prefixes 24 -rag-fraction 0   # system prompts compartidos (routing por prefijos)
task run -- -placement round-robin                     # colocación de la Fase 2, para comparar
task loadgen -- -rate 4 -duration 45s -chaos down      # tumba sim-worker-1 a mitad (o hang, errors, stall, slow, restart)
```

`loadgen` imprime al terminar el reparto de resultados, el TTFT y el estado de `/debug/workers` y `/debug/scheduler`. Con la plataforma arrancada con `-admin-addr`, añadir `-admin-url` para que lea esos endpoints del puerto de administración.

## Stack de observabilidad (Docker)

La plataforma, Prometheus y Grafana con el dashboard y las alertas. Cómo leerlo: [observabilidad](03-observabilidad.md).

```bash
task stack:up                     # construye la imagen y arranca plataforma + Prometheus + Grafana (necesita Docker)
task loadgen -- -rate 4 -duration 2m -prefixes 24 -rag-fraction 0      # tráfico con prefijos compartidos
task loadgen -- -rate 5 -duration 100s -prefixes 24 -chaos hang        # o con un worker que se cuelga a mitad
task stack:up:fake-vllm           # variante (Fase 6a): la plataforma habla por HTTP con 3 vLLM falsos (:8001-8003)
task stack:up:vllm-cpu            # variante (Fase 6b-CPU): vLLM real en CPU (~6 min de calentamiento); :2 para dos instancias
task loadgen -- -rate 5 -duration 90s -prefixes 24 -chaos hang -chaos-worker http://localhost:8001   # caos sobre un vLLM falso
task stack:logs                   # logs JSON de la plataforma
task stack:down                   # para y borra el stack (no guarda datos)
```

Ficheros (`deploy/observability/`): `docker-compose.yml`, `prometheus/prometheus.yml` (*scrape* cada 5 s, retención 2 días), `prometheus/alerts.yml` (las [alertas de ejemplo](03-observabilidad.md#sli-slo-y-alertas-propuestas)), `grafana/provisioning/` (fuente de datos y proveedor de dashboards) y `grafana/dashboards/llm-platform-overview.json` (el dashboard). La plataforma corre en el contenedor con `-prefill-per-token 100us`, para que el efecto de la caché de prefijos se vea en el TTFT.

| Servicio | URL | Para qué |
| :--- | :--- | :--- |
| Plataforma | `http://localhost:8080` | API, `/metrics`, `/debug/*` (en Kubernetes, `/metrics` y `/debug/*` van al puerto de administración: ver [Kubernetes en local](#kubernetes-en-local)) |
| Prometheus | `http://localhost:9090` | Consultas PromQL sueltas, estado del *scrape* (`/targets`) |
| Grafana | `http://localhost:3000` | Dashboard **LLM Platform · Overview**, página de inicio (acceso anónimo de solo lectura; `admin`/`admin` para editar) |
| Alertas | `http://localhost:9090/alerts` | Estado de las reglas de ejemplo (sin Alertmanager: no avisan a nadie) |

## vLLM falsos

`task stack:up:fake-vllm` arranca el stack con la plataforma en `-workers 0` y tres vLLM falsos (`cmd/fakevllm`) en `:8001–8003`. Los falsos "cargan el modelo" 10 s y la plataforma registra cada uno cuando está listo (log `vllm registered`). Prometheus lee también su `/metrics` (`job="vllm"`).

```bash
curl -X POST localhost:8001/fake/faults -d '{"status":500}'   # romper uno ({} lo repara; también "hang", "unhealthy", ...)
curl -X POST localhost:8001/fake/restart                      # simular un reinicio
task loadgen -- -rate 5 -duration 90s -prefixes 24 -chaos hang -chaos-worker http://localhost:8001
```

**Las magnitudes del falso son inventadas** (ocupación de KV-Cache, tokenizer): sirve para probar el camino y los fallos, no para medir ([servicio · 13](../servicio/13-fase-6a-decisiones.md#lo-que-el-falso-no-puede-decir-pendiente-de-la-6b)).

## vLLM real en CPU

La imagen fijada `vllm/vllm-openai-cpu:v0.34.0-x86_64` con `Qwen/Qwen2.5-0.5B-Instruct` en `float32`. Funciona en una CPU de portátil con AVX2 (sin AVX-512). Decisiones y hallazgos: [servicio · 14](../servicio/14-fase-6b-cpu-decisiones.md).

```bash
task vllm-cpu:up && task vllm-cpu:smoke   # contenedor suelto en :8100 y prueba de humo del adaptador (TestRealVLLM)
task vllm-cpu:down
task stack:up:vllm-cpu                    # stack con una instancia; :2 para dos (el benchmark)
VLLM_KV_BYTES=201326592 task stack:up:vllm-cpu:2   # KV-Cache reducida para forzar expulsiones (benchmark de la 6b-CPU)
```

- **Lento por naturaleza:** ~6 minutos de calentamiento por instancia (más la primera vez, mientras descarga el modelo al volumen `vllm-hf-cache`) y ~4 tokens/s.
- **Cada instancia en sus núcleos** (`VLLM_CPU_OMP_THREADS_BIND`), para que dos no compitan. Con 8 CPU y ~16 GB caben dos como mucho.
- **La plataforma necesita el perfil de CPU** (`-ttft-timeout 30s -ttft-timeout-per-token 25ms -stall-timeout 30s`): sin él, los prefills lentos se tratan como cuelgues. El compose ya lo pasa.
- `TestRealVLLM` solo se ejecuta si `VLLM_URL` está definida; no entra en `task check`.

## Kubernetes en local

minikube con un **perfil propio** (`llm-platform`), así que no toca otros clústeres de la máquina: cada orden usa `kubectl --context llm-platform` y `helm --kube-context llm-platform`. Decisiones: [servicio · 15](../servicio/15-fase-7-decisiones.md).

```bash
task k8s:up              # perfil de minikube (6 CPU, 10 GB), imágenes, KEDA y el chart deploy/helm/llm-platform
task k8s:port-forward    # API :8080, Grafana :3000, puerto de administración (/metrics, /debug) en :18081
task loadgen -- -admin-url http://localhost:18081 -rate 1.5 -duration 180s -prefixes 8
task k8s:status          # pods, ScaledObject y HPA
task k8s:down            # borra el perfil (clúster y datos)
```

- `task k8s:up` construye las imágenes con Docker y las carga con `minikube image load` (sin registro), y reinicia los Deployments para que tomen la imagen nueva.
- La plataforma descubre los vLLM falsos por el DNS del Service *headless* `fakevllm`; KEDA los escala entre 1 y 4 según la demanda en tokens.
- vLLM en CPU dentro del clúster: `task k8s:up -- --set vllmCpu.enabled=true` y el perfil de CPU en `platform.extraArgs` (ver `values.yaml`). No se ha probado en minikube.
- Para ver el autoscaling en directo: `kubectl --context llm-platform -n llm get scaledobject,hpa,pods -w`.

## Problemas conocidos del entorno

| Problema | Solución |
| :--- | :--- |
| Docker Desktop se para solo y las tareas fallan con errores de conexión al *daemon*. | Volver a arrancarlo y esperar a que `docker info` responda. |
| `port-forward` al puerto 8081 falla en Windows (*access permissions*): Hyper-V reserva ese rango. | El puerto de administración se reenvía a `localhost:18081` (`task k8s:port-forward` ya lo hace). |
| Desde Git Bash, `docker run -v ...` a mano convierte las rutas. | `MSYS_NO_PATHCONV=1` delante (Task no lo necesita). |
| Un panel de Grafana sale vacío. | Comprobar en `http://localhost:9090/targets` que los *targets* están `UP`; Prometheus tarda entre 5 y 15 s en reflejar un cambio. |
| Las capturas o la prueba de humo de vLLM salen vacías. | vLLM aún calienta: esperar a que `/health` responda `200`. |
