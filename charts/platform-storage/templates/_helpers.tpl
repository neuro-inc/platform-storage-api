{{- define "platformStorage.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "platformStorage.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "platformStorage.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" -}}
{{- end -}}

{{- define "platformStorage.labels.standard" -}}
app: {{ include "platformStorage.name" . }}
chart: {{ include "platformStorage.chart" . }}
heritage: {{ .Release.Service | quote }}
release: {{ .Release.Name | quote }}
{{- end -}}

{{- define "platformStorage.env" -}}
- name: NP_STORAGE_LOCAL_BASE_PATH
  value: /var/storage
- name: NP_STORAGE_MODE
{{- if gt (len .Values.storages) 1 }}
  value: multiple
{{- else }}
  value: single
{{- end }}
- name: NP_PLATFORM_CLUSTER_NAME
  value: {{ .Values.platform.clusterName | quote }}
- name: NP_PLATFORM_AUTH_URL
  value: {{ .Values.platform.authUrl | quote }}
- name: NP_PLATFORM_ADMIN_URL
  value: {{ .Values.platform.adminUrl | quote }}
- name: NP_PLATFORM_TOKEN
  {{- if .Values.platform.token }}
  {{- toYaml .Values.platform.token | nindent 2 }}
  {{- end }}
- name: NP_PERMISSION_EXPIRATION_INTERVAL
  value: {{ .Values.permissionExpirationInterval | quote }}
- name: NP_PERMISSION_FORGETTING_INTERVAL
  value: {{ .Values.permissionForgettingInterval | quote }}
- name: NP_STORAGE_API_KEEP_ALIVE_TIMEOUT
  value: {{ .Values.keepAliveTimeout | quote }}
- name: NP_STORAGE_API_K8S_API_URL
  value: https://kubernetes.default:443
- name: NP_STORAGE_API_K8S_AUTH_TYPE
  value: token
- name: NP_STORAGE_API_K8S_CA_PATH
  value: {{ include "platformStorage.kubeAuthMountRoot" . }}/ca.crt
- name: NP_STORAGE_API_K8S_TOKEN_PATH
  value: {{ include "platformStorage.kubeAuthMountRoot" . }}/token
- name: NP_STORAGE_API_K8S_NS
  value: {{ .Release.Namespace }}
- name: NP_STORAGE_ADMISSION_CONTROLLER_SERVICE_NAME
  value: {{ .Values.admissionController.serviceName }}
- name: NP_STORAGE_ADMISSION_CONTROLLER_SECRET_NAME_CERTS
  value: {{ .Values.admissionController.secretNameCerts}}
{{ include "platformStorage.env.s3" . }}
{{- if .Values.sentry }}
- name: SENTRY_DSN
  value: {{ .Values.sentry.dsn }}
- name: SENTRY_CLUSTER_NAME
  value: {{ .Values.sentry.clusterName }}
- name: SENTRY_APP_NAME
  value: {{ .Values.sentry.appName }}
- name: SENTRY_SAMPLE_RATE
  value: {{ .Values.sentry.sampleRate | default 0 | quote }}
{{- end }}
{{- end -}}

{{- define "platformStorage.env.s3" -}}
{{- if .Values.s3.accessKeyId }}
- name: AWS_ACCESS_KEY_ID
  {{- toYaml .Values.s3.accessKeyId | nindent 2 }}
{{- end }}
{{- if .Values.s3.secretAccessKey }}
- name: AWS_SECRET_ACCESS_KEY
  {{- toYaml .Values.s3.secretAccessKey | nindent 2 }}
{{- end }}
- name: S3_REGION
  value: {{ .Values.s3.region }}
{{- if .Values.s3.endpoint }}
- name: S3_ENDPOINT_URL
  value: {{ .Values.s3.endpoint }}
{{- end }}
- name: S3_BUCKET_NAME
  value: {{ .Values.s3.bucket }}
{{- if .Values.s3.keyPrefix }}
- name: S3_KEY_PREFIX
  value: {{ .Values.s3.keyPrefix }}
{{- end -}}
{{- end -}}

{{- define "platformStorage.volumes" -}}
{{- range $index, $storage := .Values.storages -}}
- name: storage-{{ $index }}
  {{- if eq $storage.type "pvc" }}
  persistentVolumeClaim:
    claimName: {{ $storage.claimName }}
  {{- else if eq $storage.type "nfs" }}
  nfs:
    server: {{ $storage.server }}
    path: {{ .Values.exportPath }}
  {{- end }}
{{- end -}}
{{- end -}}

{{- define "platformStorage.volumeMounts" -}}
{{- if gt (len .Values.storages) 1 -}}
{{- range $index, $storage := .Values.storages -}}
- name: storage-{{ $index }}
  {{- if $storage.path }}
  mountPath: /var/storage{{ $storage.path }}
  {{- else }}
  mountPath: /var/storage/{{ $.Values.platform.clusterName }}
  {{- end }}
{{- end -}}
{{- else -}}
- name: storage-0
  mountPath: /var/storage
{{- end -}}
{{- end -}}

{{- define "platformStorage.metrics.fullname" -}}
{{ include "platformStorage.fullname" . }}-metrics
{{- end -}}

{{- define "platformStorage.metrics.selectorLabels" -}}
app: {{ include "platformStorage.name" . }}
release: {{ .Release.Name }}
service: platform-storage-metrics
{{- end -}}

{{- define "platformStorage.kubeAuthMountRoot" -}}
{{- printf "/var/run/secrets/kubernetes.io/serviceaccount" -}}
{{- end -}}
