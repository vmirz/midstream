{{/*
Every name derives from the release: one namespace can hold several installations.
*/}}
{{- define "midstream.fullname" -}}
{{- default .Release.Name .Values.fullnameOverride | trunc 30 | trimSuffix "-" -}}
{{- end -}}

{{- define "midstream.controlPlane" -}}
{{- include "midstream.fullname" . }}-control-plane
{{- end -}}

{{- define "midstream.maintenance" -}}
{{- include "midstream.fullname" . }}-maintenance
{{- end -}}

{{- define "midstream.tail" -}}
{{- include "midstream.fullname" . }}-tail
{{- end -}}

{{- define "midstream.console" -}}
{{- include "midstream.fullname" . }}-console
{{- end -}}

{{- define "midstream.authSecret" -}}
{{- default (printf "%s-auth" (include "midstream.fullname" .)) .Values.auth.existingSecret -}}
{{- end -}}

{{/* The control plane's and the tail app's web Services, as kaspr-operator names them. */}}
{{- define "midstream.controlPlaneUrl" -}}
http://{{ include "midstream.controlPlane" . }}-app-api:{{ .Values.controlPlane.webPort }}
{{- end -}}

{{- define "midstream.tailUrl" -}}
http://{{ include "midstream.tail" . }}-app-api:{{ .Values.tail.webPort }}
{{- end -}}

{{- define "midstream.labels" -}}
app.kubernetes.io/name: midstream
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{/* The package reference pip installs: this repository at a ref, or a release from an index. */}}
{{- define "midstream.packageRef" -}}
{{- if eq .Values.package.source "git" -}}
git+{{ .Values.package.git.repository }}@{{ .Values.package.git.ref }}#egg=midstream&subdirectory=python
{{- else if eq .Values.package.source "index" -}}
midstream=={{ default .Chart.AppVersion .Values.package.index.version }}
{{- else -}}
{{- fail (printf "package.source must be git or index, not %q" .Values.package.source) -}}
{{- end -}}
{{- end -}}

{{/* Kaspr's pythonPackages block, the same for every app and for every runtime. */}}
{{- define "midstream.pythonPackages" -}}
packages:
  - {{ include "midstream.packageRef" . }}
{{- if eq .Values.package.source "index" }}
{{- with .Values.package.index.url }}
indexUrl: {{ . | quote }}
{{- end }}
{{- with .Values.package.index.extraUrls }}
extraIndexUrls:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.package.index.credentialsSecret }}
credentials:
  secretRef:
    name: {{ . }}
{{- end }}
{{- end }}
{{- with .Values.package.cache }}
cache:
  {{- toYaml . | nindent 2 }}
{{- end }}
installPolicy:
  {{- toYaml .Values.package.installPolicy | nindent 2 }}
{{- end -}}

{{/* A persistent claim of the given size, on the cluster's default class unless one is named. */}}
{{- define "midstream.storage" -}}
type: persistent-claim
{{- with .root.Values.kaspr.storageClass }}
class: {{ . }}
{{- end }}
size: {{ .size }}
deleteClaim: true
{{- end -}}

{{/*
Where a Kaspr pod is placed: the app's own nodeSelector, tolerations and affinity when it sets them,
kaspr's otherwise, which also place every runtime. Called with (dict "root" $ "own" <the app's values>).
*/}}
{{- define "midstream.podScheduling" -}}
{{- $own := .own | default dict -}}
{{- with $own.nodeSelector | default .root.Values.kaspr.nodeSelector }}
nodeSelector:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with $own.tolerations | default .root.Values.kaspr.tolerations }}
tolerations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with $own.affinity | default .root.Values.kaspr.affinity }}
affinity:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{/* The GCS service-account key, when one is given: a volume, its mount, and the variable that names it. */}}
{{- define "midstream.gcsVolumes" -}}
{{- with .Values.gcs.credentialsSecret }}
volumes:
  - name: gcp-credentials
    secret:
      secretName: {{ . }}
      items:
        - key: {{ $.Values.gcs.credentialsKey }}
          path: google_application_credentials.json
{{- end }}
{{- end -}}

{{- define "midstream.gcsVolumeMounts" -}}
{{- if .Values.gcs.credentialsSecret }}
volumeMounts:
  - name: gcp-credentials
    mountPath: /app/gcp
    readOnly: true
{{- end }}
{{- end -}}

{{- define "midstream.gcsEnv" -}}
{{- if .Values.gcs.credentialsSecret }}
- name: GOOGLE_APPLICATION_CREDENTIALS
  value: /app/gcp/google_application_credentials.json
{{- end }}
{{- end -}}

{{/*
The spec every materialization's runtime is rendered from (handbook 10 §10.1). The control plane
sets each runtime's replicas, storage size and package pin on top of it.
*/}}
{{- define "midstream.runtimeSpec" -}}
bootstrapServers: {{ .Values.kaspr.bootstrapServers | quote }}
{{- with .Values.kaspr.authentication }}
authentication:
  {{- toYaml . | nindent 2 }}
{{- end }}
storage:
  {{- include "midstream.storage" (dict "root" . "size" .Values.controlPlane.settings.defaultRuntimeStorageSize) | nindent 2 }}
resources:
  {{- toYaml .Values.controlPlane.runtime.resources | nindent 2 }}
verticalPodAutoscaler:
  {{- toYaml .Values.controlPlane.runtime.verticalPodAutoscaler | nindent 2 }}
config:
  {{- toYaml .Values.controlPlane.runtime.config | nindent 2 }}
pythonPackages:
  {{- include "midstream.pythonPackages" . | nindent 2 }}
template:
  pod:
    {{- include "midstream.podScheduling" (dict "root" . "own" .Values.controlPlane.runtime) | nindent 4 }}
    {{- include "midstream.gcsVolumes" . | nindent 4 }}
  kasprContainer:
    env:
      - name: RUNTIME_MEMBER_ID
        valueFrom:
          fieldRef:
            fieldPath: metadata.name
      {{- include "midstream.gcsEnv" . | nindent 6 }}
      - name: PROTOCOL_LOGGING_ENABLED
        value: {{ .Values.controlPlane.runtime.protocolLogging | quote }}
      {{- with .Values.controlPlane.runtime.extraEnv }}
      {{- toYaml . | nindent 6 }}
      {{- end }}
    {{- include "midstream.gcsVolumeMounts" . | nindent 4 }}
{{- end -}}

{{/* Kaspr's Kafka client authentication, required by the KasprApp CRD: scram-sha-512, scram-sha-256 or plain. */}}
{{- define "midstream.authentication" -}}
{{- $auth := required "kaspr.authentication is required: Kaspr's Kafka client authentication, with type scram-sha-512, scram-sha-256 or plain, a username and a passwordSecret" .Values.kaspr.authentication -}}
{{- if not $auth.type -}}
{{- fail "kaspr.authentication.type is required: scram-sha-512, scram-sha-256 or plain" -}}
{{- end -}}
{{- toYaml $auth -}}
{{- end -}}
