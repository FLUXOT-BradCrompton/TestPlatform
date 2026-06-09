{{/*
Flux OT Platform — Helm Template Helpers
Provides standard named templates used throughout the chart.
*/}}

{{/*
Expand the name of the chart.
Default: chart name. Override with .Values.nameOverride.
*/}}
{{- define "flux-ot-platform.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
Truncated to 63 characters because some Kubernetes name fields have limits.
If .Values.fullnameOverride is set it is used as-is (after truncation).
Otherwise: <release-name>-<chart-name>, or just <chart-name> if
the release name already contains it.
*/}}
{{- define "flux-ot-platform.fullname" -}}
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

{{/*
Create chart label: "<chart-name>-<chart-version>"
Used in the helm.sh/chart label.
*/}}
{{- define "flux-ot-platform.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels applied to every resource.
Includes the Helm recommended set plus Flux OT specifics.
*/}}
{{- define "flux-ot-platform.labels" -}}
helm.sh/chart: {{ include "flux-ot-platform.chart" . }}
{{ include "flux-ot-platform.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: fluxot-platform
environment: {{ .Values.global.environment | default "production" | quote }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Selector labels — used in matchLabels and podSelector.
These must be stable across upgrades; do not add mutable labels here.
*/}}
{{- define "flux-ot-platform.selectorLabels" -}}
app.kubernetes.io/name: {{ include "flux-ot-platform.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Platform API selector labels
*/}}
{{- define "flux-ot-platform.api.selectorLabels" -}}
app: {{ include "flux-ot-platform.fullname" . }}-api
app.kubernetes.io/name: {{ include "flux-ot-platform.name" . }}-api
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: api
{{- end }}

{{/*
MQTT Bridge selector labels
*/}}
{{- define "flux-ot-platform.bridge.selectorLabels" -}}
app: {{ include "flux-ot-platform.fullname" . }}-bridge
app.kubernetes.io/name: {{ include "flux-ot-platform.name" . }}-bridge
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: bridge
{{- end }}

{{/*
TimescaleDB selector labels
*/}}
{{- define "flux-ot-platform.timescaledb.selectorLabels" -}}
app: {{ include "flux-ot-platform.fullname" . }}-timescaledb
app.kubernetes.io/name: {{ include "flux-ot-platform.name" . }}-timescaledb
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: database
{{- end }}

{{/*
Create the name for the platform API ServiceAccount.
If .Values.platformApi.serviceAccount.name is set, use that; otherwise
derive from the fullname.
*/}}
{{- define "flux-ot-platform.api.serviceAccountName" -}}
{{- if .Values.platformApi.serviceAccount.create }}
{{- if .Values.platformApi.serviceAccount.name }}
{{- .Values.platformApi.serviceAccount.name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-api-sa" (include "flux-ot-platform.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- else }}
{{- default "default" .Values.platformApi.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the name for the MQTT Bridge ServiceAccount.
*/}}
{{- define "flux-ot-platform.bridge.serviceAccountName" -}}
{{- if .Values.mqttBridge.serviceAccount.create }}
{{- if .Values.mqttBridge.serviceAccount.name }}
{{- .Values.mqttBridge.serviceAccount.name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-bridge-sa" (include "flux-ot-platform.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- else }}
{{- default "default" .Values.mqttBridge.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Render image reference with optional global registry override.
Usage: {{ include "flux-ot-platform.image" (dict "image" .Values.platformApi.image "global" .Values.global) }}
*/}}
{{- define "flux-ot-platform.image" -}}
{{- $registry := .global.imageRegistry -}}
{{- $repository := .image.repository -}}
{{- $tag := .image.tag | default "latest" -}}
{{- if .image.digest -}}
{{- if $registry -}}
{{- printf "%s/%s@%s" $registry $repository .image.digest -}}
{{- else -}}
{{- printf "%s@%s" $repository .image.digest -}}
{{- end -}}
{{- else -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" $registry $repository $tag -}}
{{- else -}}
{{- printf "%s:%s" $repository $tag -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
Return the effective storage class: use component-level override if set,
otherwise fall back to global.storageClass.
Usage: {{ include "flux-ot-platform.storageClass" (dict "storageClass" .Values.timescaledb.storage.storageClass "global" .Values.global) }}
*/}}
{{- define "flux-ot-platform.storageClass" -}}
{{- if .storageClass -}}
{{ .storageClass }}
{{- else -}}
{{ .global.storageClass | default "gp3-csi" }}
{{- end -}}
{{- end }}

{{/*
Return the Route hostname for a given service.
If the component-level hostname is set, use it; otherwise build
<prefix>.<global.clusterDomain>.
Usage: {{ include "flux-ot-platform.routeHostname" (dict "hostname" .Values.platformApi.route.hostname "prefix" "platform-api" "global" .Values.global) }}
*/}}
{{- define "flux-ot-platform.routeHostname" -}}
{{- if .hostname -}}
{{ .hostname }}
{{- else -}}
{{- printf "%s.%s" .prefix .global.clusterDomain -}}
{{- end -}}
{{- end }}

{{/*
Common annotations for all resources.
*/}}
{{- define "flux-ot-platform.annotations" -}}
{{- with .Values.commonAnnotations }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Render imagePullSecrets from either the global list or component-specific list.
Merges both without duplicates.
*/}}
{{- define "flux-ot-platform.imagePullSecrets" -}}
{{- $global := .Values.global.imagePullSecrets | default (list) -}}
{{- $local := .Values.imagePullSecrets | default (list) -}}
{{- $combined := concat $global $local | uniq -}}
{{- if $combined }}
imagePullSecrets:
  {{- range $combined }}
  - name: {{ . }}
  {{- end }}
{{- end }}
{{- end }}

{{/*
Topology spread constraints helper.
Accepts a list of constraints and the selector labels dict.
Usage: {{ include "flux-ot-platform.topologySpreadConstraints" (dict "constraints" .Values.platformApi.topologySpreadConstraints "selectorLabels" (include "flux-ot-platform.api.selectorLabels" .)) }}
*/}}
{{- define "flux-ot-platform.topologySpreadConstraints" -}}
{{- if .constraints }}
topologySpreadConstraints:
  {{- range .constraints }}
  - maxSkew: {{ .maxSkew | default 1 }}
    topologyKey: {{ .topologyKey }}
    whenUnsatisfiable: {{ .whenUnsatisfiable | default "DoNotSchedule" }}
    labelSelector:
      matchLabels:
        {{- .selectorLabels | nindent 8 }}
  {{- end }}
{{- end }}
{{- end }}
