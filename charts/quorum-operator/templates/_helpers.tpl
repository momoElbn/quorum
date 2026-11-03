{{/* Base name for every object, e.g. "quorum-operator". */}}
{{- define "quorum-operator.fullname" -}}
{{- .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}

{{/* Labels used to find the operator pod. Never change them after install. */}}
{{- define "quorum-operator.selectorLabels" -}}
app.kubernetes.io/name: quorum-operator
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* Labels put on every object. */}}
{{- define "quorum-operator.labels" -}}
{{ include "quorum-operator.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}
