{{/* Common labels. */}}
{{- define "llm-platform.labels" -}}
app.kubernetes.io/part-of: llm-platform
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end }}

{{/* DNS name of a headless Service, port included: what -vllm-dns resolves. */}}
{{- define "llm-platform.dns" -}}
{{ .name }}.{{ .ns }}.svc.cluster.local:8000
{{- end }}
