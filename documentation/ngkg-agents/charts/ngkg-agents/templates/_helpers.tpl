{{- define "ngkg-agents.labels" -}}
app.kubernetes.io/name: ngkg-agents
app.kubernetes.io/component: mcp-gateway
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}
