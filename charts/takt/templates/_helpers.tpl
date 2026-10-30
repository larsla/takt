{{- define "takt.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "takt.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "takt.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "takt.labels" -}}
helm.sh/chart: {{ include "takt.chart" . }}
{{ include "takt.selectorLabels" . }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "takt.selectorLabels" -}}
app.kubernetes.io/name: {{ include "takt.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "takt.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "takt.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "takt.image" -}}
{{- $repo := .Values.image.repository }}
{{- if .Values.image.registry }}
{{- $repo = printf "%s/%s" .Values.image.registry .Values.image.repository }}
{{- end }}
{{- if .Values.image.digest }}
{{- printf "%s@%s" $repo .Values.image.digest }}
{{- else }}
{{- printf "%s:%s" $repo (.Values.image.tag | default .Chart.AppVersion) }}
{{- end }}
{{- end }}

{{/* The port that serves /healthz, empty when neither UI nor metrics run. */}}
{{- define "takt.healthPort" -}}
{{- if .Values.metrics.enabled }}metrics{{- else if .Values.ui.enabled }}ui{{- end }}
{{- end }}

{{- define "takt.validate" -}}
{{- if and .Values.embeddedNats.enabled (or .Values.autoscaling.enabled (gt (int .Values.replicaCount) 1)) }}
{{- fail "embeddedNats runs NATS inside the pod and needs replicaCount: 1 and autoscaling.enabled: false" }}
{{- end }}
{{- end }}
