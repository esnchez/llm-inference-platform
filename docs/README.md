# Documentación

| Dónde | Qué hay |
| :--- | :--- |
| [**Análisis final**](analisis-final.md) | **Para defender el proyecto:** los razonamientos clave con su evidencia, qué se midió y en qué entorno, puntos fuertes, limitaciones, distancia a producción con GPUs y preguntas difíciles con su respuesta. |
| [`servicio/`](servicio/README.md) | **El servicio en Go:** arquitectura, recorrido de una petición, leases y capacidad, worker simulado, API, relación con vLLM, concurrencia, y las decisiones de diseño de cada fase (documentos 08–15, seguidos, de la Fase 1 a la 7). |
| [`operaciones/`](operaciones/README.md) | **Probar y operar:** entornos locales (Docker, vLLM en CPU, minikube), tests, observabilidad (métricas, dashboard, alertas) y los resultados de todas las pruebas. |

El [README raíz](../README.md) cuenta la visión, las decisiones de arquitectura, el roadmap y las limitaciones conocidas (FI-n).
