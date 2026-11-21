# Documentación de operaciones

Esta carpeta explica **cómo se prueba y se opera** la plataforma: cómo levantar cada entorno en local, qué cubren los tests, cómo leer las métricas y el dashboard, y qué resultados dieron las pruebas.

El diseño del servicio y las decisiones de cada fase están en [`../servicio`](../servicio/README.md). El resumen para defender el proyecto, en el [análisis final](../analisis-final.md).

| # | Documento | Qué responde |
| :--- | :--- | :--- |
| 1 | [Entorno local](01-entorno-local.md) | Cómo levantar cada entorno, de menos a más realista: workers simulados, stack de observabilidad, vLLM falsos, vLLM real en CPU y Kubernetes en minikube. Requisitos y problemas conocidos de la máquina. |
| 2 | [Tests](02-tests.md) | Las tareas de la suite, qué comprueba cada test por paquete, la batería de contrato común a los workers, el estrés con caos, la CI y los huecos conocidos. |
| 3 | [Observabilidad](03-observabilidad.md) | **Guía de uso.** Qué se mide y por qué, catálogo de métricas, el dashboard panel a panel, un ejemplo de lectura con una captura real, guía de diagnóstico, logs, SLO y alertas, relación con las métricas de vLLM. |
| 4 | [Resultados de las pruebas](04-resultados-de-pruebas.md) | Todas las mediciones por fase, con su montaje, sus cifras y su lectura: simulador, vLLM falso, vLLM real en CPU y Kubernetes. |
