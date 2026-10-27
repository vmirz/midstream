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

{{/*
This release's domain (handbook 20): its id is a URL segment, a catalog in SQL and a level of the
federated catalog's namespaces; empty, the release name with `-` read as `_`.
*/}}
{{- define "midstream.domainId" -}}
{{- $id := .Values.domain.id | default (include "midstream.fullname" . | replace "-" "_") -}}
{{- include "midstream.checkDomainId" (dict "id" $id "what" "domain.id") -}}
{{- $id -}}
{{- end -}}

{{- define "midstream.domainName" -}}
{{- .Values.domain.name | default (include "midstream.domainId" .) -}}
{{- end -}}

{{/*
A domain id: a letter, then up to 31 lowercase letters, digits or underscores; not a word the console's
URLs or SQL use, and not ending `_iceberg`, which names a domain's native tables in SQL. The console and
the control plane also refuse DuckDB's reserved words. Called with (dict "id" <id> "what" <the value>).
*/}}
{{- define "midstream.checkDomainId" -}}
{{- if not (regexMatch "^[a-z][a-z0-9_]{0,31}$" .id) -}}
{{- fail (printf "%s %q must be a letter followed by up to 31 lowercase letters, digits or underscores" .what .id) -}}
{{- end -}}
{{- if or (has .id (list "api" "d" "catalog" "iceberg" "temp" "system" "main" "memory" "information_schema" "pg_catalog")) (hasSuffix "_iceberg" .id) -}}
{{- fail (printf "%s %q is reserved: the console's URLs or SQL use it" .what .id) -}}
{{- end -}}
{{- end -}}

{{/* Which parts this release deploys: the maintenance and tail apps need the control plane. */}}
{{- define "midstream.controlPlaneEnabled" -}}
{{- if .Values.controlPlane.enabled }}true{{ end -}}
{{- end -}}

{{- define "midstream.maintenanceEnabled" -}}
{{- if and .Values.controlPlane.enabled .Values.maintenance.enabled }}true{{ end -}}
{{- end -}}

{{- define "midstream.tailEnabled" -}}
{{- if and .Values.controlPlane.enabled .Values.tail.enabled }}true{{ end -}}
{{- end -}}

{{/*
The domains the console serves, as JSON: console.domains with every rule checked, or this release's
domain alone. A local domain is reached through this release's Services; another through its API.
*/}}
{{- define "midstream.consoleDomains" -}}
{{- $root := . -}}
{{- $given := .Values.console.domains -}}
{{- if not $given -}}
{{- if not .Values.controlPlane.enabled -}}
{{- fail "console.domains is required: a release without a control plane of its own must name the domains its console serves" -}}
{{- end -}}
{{- $given = list (dict "id" (include "midstream.domainId" .) "name" (include "midstream.domainName" .) "local" true "settingsHome" true) -}}
{{- end -}}
{{- $out := list -}}
{{- $seen := dict -}}
{{- $homes := 0 -}}
{{- range $index, $entry := $given -}}
{{- $where := printf "console.domains[%d]" $index -}}
{{- $id := required (printf "%s.id is required" $where) $entry.id -}}
{{- include "midstream.checkDomainId" (dict "id" $id "what" (printf "%s.id" $where)) -}}
{{- if hasKey $seen $id -}}
{{- fail (printf "%s: domain %q is named twice" $where $id) -}}
{{- end -}}
{{- $_ := set $seen $id true -}}
{{- if $entry.settingsHome -}}
{{- $homes = add1 $homes -}}
{{- end -}}
{{- $domain := dict "id" $id "name" ($entry.name | default $id) "local" (default false $entry.local) "settingsHome" (default false $entry.settingsHome) -}}
{{- if $entry.local -}}
{{- if not $root.Values.controlPlane.enabled -}}
{{- fail (printf "%s is local, but this release has no control plane (controlPlane.enabled)" $where) -}}
{{- end -}}
{{- if ne $id (include "midstream.domainId" $root) -}}
{{- fail (printf "%s is local, so its id must be this release's domain.id %q" $where (include "midstream.domainId" $root)) -}}
{{- end -}}
{{- $_ := set $domain "controlPlaneUrl" (include "midstream.controlPlaneUrl" $root) -}}
{{- $_ := set $domain "tailUrl" (ternary (include "midstream.tailUrl" $root) "" (eq (include "midstream.tailEnabled" $root) "true")) -}}
{{- else -}}
{{- $_ := set $domain "controlPlaneUrl" (required (printf "%s.controlPlaneUrl is required for a domain in another release" $where) $entry.controlPlaneUrl | trimSuffix "/") -}}
{{- $_ := set $domain "tailUrl" ($entry.tailUrl | default "" | trimSuffix "/") -}}
{{- $secret := $entry.serviceTokenSecret | default dict -}}
{{- if not (and $secret.name $secret.key) -}}
{{- fail (printf "%s.serviceTokenSecret {name, key} is required: the Secret holding that domain's service-token" $where) -}}
{{- end -}}
{{- end -}}
{{- $out = append $out $domain -}}
{{- end -}}
{{- if ne $homes 1 -}}
{{- fail (printf "console.domains must name exactly one settingsHome, the domain storing the console's own settings; it names %d" $homes) -}}
{{- end -}}
{{- toJson $out -}}
{{- end -}}

{{/* The position of the domain named `id` in a list of domains, as text. */}}
{{- define "midstream.domainIndex" -}}
{{- $found := -1 -}}
{{- range $index, $entry := .domains -}}
{{- if eq $entry.id $.id -}}{{- $found = $index -}}{{- end -}}
{{- end -}}
{{- $found -}}
{{- end -}}

{{/*
The console's OAuth clients (handbook 18 §18.17), as CONSOLE_SSO_<PROVIDER>_*: a provider the console
offers once its client id and secret are set.
*/}}
{{- define "midstream.consoleSsoEnv" -}}
{{- $sso := .Values.console.sso -}}
{{- range $provider := list "entra" "github" "google" "oidc" }}
{{- $config := index $sso $provider }}
{{- if $config.enabled }}
{{- $secret := $config.clientSecretSecret | default dict }}
{{- if not (and $config.clientId $secret.name $secret.key) }}
{{- fail (printf "console.sso.%s needs clientId and clientSecretSecret {name, key}" $provider) }}
{{- end }}
- name: CONSOLE_SSO_{{ upper $provider }}_CLIENT_ID
  value: {{ $config.clientId | quote }}
- name: CONSOLE_SSO_{{ upper $provider }}_CLIENT_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ $secret.name }}
      key: {{ $secret.key }}
{{- if eq $provider "entra" }}
{{- if not $config.tenantId }}
{{- fail "console.sso.entra needs tenantId" }}
{{- end }}
- name: CONSOLE_SSO_ENTRA_TENANT_ID
  value: {{ $config.tenantId | quote }}
{{- end }}
{{- if eq $provider "google" }}
- name: CONSOLE_SSO_GOOGLE_HOSTED_DOMAIN
  value: {{ $config.hostedDomain | default "" | quote }}
{{- end }}
{{- if eq $provider "oidc" }}
{{- if not $config.issuer }}
{{- fail "console.sso.oidc needs issuer" }}
{{- end }}
- name: CONSOLE_SSO_OIDC_ISSUER
  value: {{ $config.issuer | trimSuffix "/" | quote }}
- name: CONSOLE_SSO_OIDC_LABEL
  value: {{ $config.label | default "" | quote }}
- name: CONSOLE_SSO_OIDC_SCOPES
  value: {{ join " " ($config.scopes | default (list "openid" "email" "profile")) | quote }}
{{- end }}
{{- end }}
{{- end }}
{{- end -}}

{{/* The variable carrying a domain's service token to the console: CONSOLE_DOMAIN_<ID>_SERVICE_TOKEN. */}}
{{- define "midstream.domainTokenEnv" -}}
CONSOLE_DOMAIN_{{ upper . }}_SERVICE_TOKEN
{{- end -}}

{{/*
The URL people use for the console, without a trailing slash: console.url, else https:// and the
console's first hostname. Empty when neither is set.
*/}}
{{- define "midstream.consoleUrl" -}}
{{- if .Values.console.url -}}
{{- .Values.console.url | trimSuffix "/" -}}
{{- else if and .Values.route.enabled .Values.route.consoleHostnames -}}
https://{{ first .Values.route.consoleHostnames }}
{{- else if and .Values.ingress.enabled .Values.ingress.consoleHost -}}
https://{{ .Values.ingress.consoleHost }}
{{- end -}}
{{- end -}}

{{/*
The single sign-on a domain accepts (handbook 18 §18.17): each provider's identity, which must match the
console's client, as AUTH_SSO_*, and the deployment's values for the Sign-in settings, which the
Settings page can override, as CONTROL_PLANE_*. An empty identity takes console.sso's, in a release that
has both.
*/}}
{{- define "midstream.ssoEnv" -}}
{{- $sso := .Values.auth.sso -}}
{{- $console := .Values.console.sso -}}
{{- $entraTenant := $sso.entra.tenantId | default $console.entra.tenantId -}}
{{- $entraClient := $sso.entra.clientId | default $console.entra.clientId -}}
{{- if $sso.entra.enabled }}
{{- if not (and $entraTenant $entraClient) }}
{{- fail "auth.sso.entra needs tenantId and clientId (or console.sso.entra's in the same release)" }}
{{- end }}
- name: AUTH_SSO_ENTRA_TENANT_ID
  value: {{ $entraTenant | quote }}
- name: AUTH_SSO_ENTRA_CLIENT_ID
  value: {{ $entraClient | quote }}
{{- end }}
{{- $githubClient := $sso.github.clientId | default $console.github.clientId -}}
{{- $githubSecret := $sso.github.clientSecretSecret | default $console.github.clientSecretSecret | default dict -}}
{{- if $sso.github.enabled }}
{{- if not (and $githubClient $githubSecret.name $githubSecret.key) }}
{{- fail "auth.sso.github needs clientId and clientSecretSecret {name, key} (or console.sso.github's in the same release): a domain checks a GitHub token with the App's secret" }}
{{- end }}
- name: AUTH_SSO_GITHUB_CLIENT_ID
  value: {{ $githubClient | quote }}
- name: AUTH_SSO_GITHUB_CLIENT_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ $githubSecret.name }}
      key: {{ $githubSecret.key }}
{{- end }}
{{- $googleClient := $sso.google.clientId | default $console.google.clientId -}}
{{- if $sso.google.enabled }}
{{- if not $googleClient }}
{{- fail "auth.sso.google needs clientId (or console.sso.google's in the same release)" }}
{{- end }}
- name: AUTH_SSO_GOOGLE_CLIENT_ID
  value: {{ $googleClient | quote }}
- name: AUTH_SSO_GOOGLE_HOSTED_DOMAINS
  value: {{ join "," $sso.google.hostedDomains | quote }}
{{- end }}
{{- $oidcIssuer := $sso.oidc.issuer | default $console.oidc.issuer -}}
{{- $oidcClient := $sso.oidc.clientId | default $console.oidc.clientId -}}
{{- if $sso.oidc.enabled }}
{{- if not (and $oidcIssuer $oidcClient) }}
{{- fail "auth.sso.oidc needs issuer and clientId (or console.sso.oidc's in the same release)" }}
{{- end }}
- name: AUTH_SSO_OIDC_ISSUER
  value: {{ $oidcIssuer | trimSuffix "/" | quote }}
- name: AUTH_SSO_OIDC_CLIENT_ID
  value: {{ $oidcClient | quote }}
- name: AUTH_SSO_OIDC_LABEL
  value: {{ $sso.oidc.label | default $console.oidc.label | quote }}
- name: AUTH_SSO_OIDC_TRUST_EMAILS
  value: {{ $sso.oidc.trustEmails | quote }}
{{- end }}
- name: CONTROL_PLANE_SSO_ENTRA_ENABLED
  value: {{ $sso.entra.enabled | quote }}
- name: CONTROL_PLANE_SSO_GITHUB_ENABLED
  value: {{ $sso.github.enabled | quote }}
- name: CONTROL_PLANE_SSO_GOOGLE_ENABLED
  value: {{ $sso.google.enabled | quote }}
- name: CONTROL_PLANE_SSO_OIDC_ENABLED
  value: {{ $sso.oidc.enabled | quote }}
- name: CONTROL_PLANE_SSO_FIRST_SIGN_IN
  value: {{ $sso.firstSignIn | quote }}
- name: CONTROL_PLANE_SSO_ALLOWED_EMAIL_DOMAINS
  value: {{ join "," $sso.allowedEmailDomains | quote }}
- name: CONTROL_PLANE_PASSWORD_SIGN_IN_ENABLED
  value: {{ .Values.auth.passwordSignIn | quote }}
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
git+{{ .Values.package.git.repository }}@{{ .Values.package.git.ref }}#egg=midstream-platform&subdirectory=python
{{- else if eq .Values.package.source "index" -}}
midstream-platform=={{ default .Chart.AppVersion .Values.package.index.version }}
{{- else -}}
{{- fail (printf "package.source must be git or index, not %q" .Values.package.source) -}}
{{- end -}}
{{- end -}}

{{/*
A KasprApp's `config`: the keys the chart relies on, then kaspr.config for the rest. The chart's
keys win, so an installation cannot turn off what the apps depend on (handbook 02 §2.3).
*/}}
{{- define "midstream.kasprConfig" -}}
{{- toYaml (merge .fixed (deepCopy .root.Values.kaspr.config)) -}}
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
{{- with include "midstream.kafkaTls" . }}
{{ . }}
{{- end }}
storage:
  {{- include "midstream.storage" (dict "root" . "size" .Values.controlPlane.settings.defaultRuntimeStorageSize) | nindent 2 }}
resources:
  {{- toYaml .Values.controlPlane.runtime.resources | nindent 2 }}
verticalPodAutoscaler:
  {{- toYaml .Values.controlPlane.runtime.verticalPodAutoscaler | nindent 2 }}
config:
  {{- toYaml (merge (deepCopy .Values.controlPlane.runtime.config) .Values.kaspr.config) | nindent 2 }}
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

{{/*
TLS to the brokers: kaspr.tls, unless unset or null, is every KasprApp's `tls`, the runtimes' included,
as the CRD spells it. `{}` turns TLS on; kaspr-operator reads no field of it.
*/}}
{{- define "midstream.kafkaTls" -}}
{{- if not (kindIs "invalid" .Values.kaspr.tls) -}}
tls: {{ toJson .Values.kaspr.tls }}
{{- end -}}
{{- end -}}
