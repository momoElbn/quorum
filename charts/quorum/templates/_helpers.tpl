{{/*
Base name for every object. With release name "quorum", the pods are
quorum-0, quorum-1 and quorum-2.
*/}}
{{- define "quorum.fullname" -}}
{{- .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}

{{/* Name of the headless Service that gives each pod a DNS name. */}}
{{- define "quorum.headless" -}}
{{- include "quorum.fullname" . -}}-headless
{{- end -}}

{{/*
Labels used to find the pods. They must never change after the first
install: a StatefulSet's selector is immutable.
*/}}
{{- define "quorum.selectorLabels" -}}
app.kubernetes.io/name: quorum
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* Labels put on every object. */}}
{{- define "quorum.labels" -}}
{{ include "quorum.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{/*
DNS name of pod number $i through the headless Service, e.g.
quorum-0.quorum-headless.quorum.svc.cluster.local.
Call it with (list $ $i).
*/}}
{{- define "quorum.podHost" -}}
{{- $ := index . 0 -}}
{{- $i := index . 1 -}}
{{ include "quorum.fullname" $ }}-{{ $i }}.{{ include "quorum.headless" $ }}.{{ $.Release.Namespace }}.svc.{{ $.Values.clusterDomain }}
{{- end -}}
