# Respuestas reales de vLLM 0.34.0 (CPU)

Capturadas el 2026-11-21 de `vllm/vllm-openai-cpu:v0.34.0-x86_64` con
`Qwen/Qwen2.5-0.5B-Instruct` (`--dtype float32 --max-model-len 4096`,
`VLLM_CPU_KVCACHE_SPACE=2`). Los tests de `internal/vllm` las usan para que
el parser SSE, el de `/metrics`, la tabla de errores y el descubrimiento se
comprueben contra lo que vLLM devolvió de verdad (Fase 6b-CPU, decisión 1).

Para regenerarlas al cambiar de versión: `task vllm-cpu:up` y repetir las
peticiones de `docs/servicio/14-fase-6b-cpu-decisiones.md`.
