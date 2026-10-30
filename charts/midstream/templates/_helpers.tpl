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

{{/*
The protection key data protection hashes with (handbook 22), from the auth Secret: the control plane,
the tail app, the maintenance app and every runtime read it. Optional, so a deployment without it runs:
creates that would hash are refused until it is added.
*/}}
{{- define "midstream.protectionKeyEnv" -}}
- name: PROTECTION_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "midstream.authSecret" . }}
      key: protection-key
      optional: true
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

{{/*
Labels or annotations given as values (kaspr.podLabels, kaspr.serviceAccountAnnotations and the console's),
one "key": "value" line each, every value a string as Kubernetes takes it. Labels may not take a key the
chart sets itself (midstream.labels, app.kubernetes.io/component). Called with (dict "map" <the value>
"what" <its name>), and "root" $ for labels.
*/}}
{{- define "midstream.metadataMap" -}}
{{- if not (kindIs "map" .map) -}}
{{- fail (printf "%s must be a map of keys to values" .what) -}}
{{- end -}}
{{- $own := dict -}}
{{- with .root -}}
{{- $own = merge (include "midstream.labels" . | fromYaml) (dict "app.kubernetes.io/component" "") -}}
{{- end -}}
{{- range $key, $value := .map }}
{{- if hasKey $own $key }}
{{- fail (printf "%s: %q is a label the chart sets itself" $.what $key) }}
{{- end }}
{{ $key | quote }}: {{ ternary "" (toString $value) (kindIs "invalid" $value) | quote }}
{{- end }}
{{- end -}}

{{/*
The cloud identity every Kaspr pod runs as, the runtimes' included, for a storage backend without a Secret
(kaspr.podLabels, kaspr.serviceAccountAnnotations): IRSA's role, or AKS workload identity. The labels go
under a KasprApp's template.pod, the annotations under its template.serviceAccount: the ServiceAccount
kaspr-operator creates for each app and each runtime, named <app>-app.
*/}}
{{- define "midstream.kasprPodMetadata" -}}
{{- with .Values.kaspr.podLabels }}
metadata:
  labels:
    {{- include "midstream.metadataMap" (dict "map" . "what" "kaspr.podLabels" "root" $) | trim | nindent 4 }}
{{- end }}
{{- end -}}

{{- define "midstream.kasprServiceAccount" -}}
{{- with .Values.kaspr.serviceAccountAnnotations }}
serviceAccount:
  metadata:
    annotations:
      {{- include "midstream.metadataMap" (dict "map" . "what" "kaspr.serviceAccountAnnotations") | trim | nindent 6 }}
{{- end }}
{{- end -}}

{{/*
The lake's object storage (storage.*; handbook 11), for the control plane, the maintenance and tail apps
and every runtime: each backend's credentials as Secret references, in the variables its SDK reads
itself, and Midstream's own STORAGE_* settings. A backend without a Secret renders no credential: its
SDK's default chain finds the pod's identity. A rendered manifest names Secrets, never a value.
*/}}
{{- define "midstream.storageEnv" -}}
{{- $storage := .Values.storage | default dict -}}
{{- $gcs := $storage.gcs | default dict -}}
{{- $s3 := $storage.s3 | default dict -}}
{{- $azure := $storage.azure | default dict -}}
{{- if $gcs.credentialsSecret }}
- name: GOOGLE_APPLICATION_CREDENTIALS
  value: /etc/midstream/gcs/service-account.json
{{- end }}
{{- with $s3.credentialsSecret }}
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ . }}
      key: access-key-id
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ . }}
      key: secret-access-key
- name: AWS_SESSION_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ . }}
      key: session-token
      optional: true
{{- end }}
{{- with $s3.region }}
- name: STORAGE_S3_REGION
  value: {{ . | quote }}
{{- end }}
{{- with include "midstream.endpoint" (dict "value" $s3.endpoint "what" "storage.s3.endpoint") }}
- name: STORAGE_S3_ENDPOINT
  value: {{ . | quote }}
{{- end }}
{{- if eq (toString $s3.pathStyle) "true" }}
- name: STORAGE_S3_PATH_STYLE
  value: "true"
{{- end }}
{{- with $azure.credentialsSecret }}
{{- $secret := . }}
{{- range $pair := list (list "AZURE_STORAGE_CONNECTION_STRING" "connection-string") (list "AZURE_STORAGE_ACCOUNT_KEY" "account-key") (list "AZURE_CLIENT_ID" "client-id") (list "AZURE_TENANT_ID" "tenant-id") (list "AZURE_CLIENT_SECRET" "client-secret") }}
- name: {{ index $pair 0 }}
  valueFrom:
    secretKeyRef:
      name: {{ $secret }}
      key: {{ index $pair 1 }}
      optional: true
{{- end }}
{{- end }}
{{- with include "midstream.endpoint" (dict "value" $azure.endpoint "what" "storage.azure.endpoint") }}
- name: STORAGE_AZURE_ENDPOINT
  value: {{ . | quote }}
{{- end }}
{{- end -}}

{{/*
An S3-compatible server's or an Azure emulator's endpoint (storage.s3.endpoint, storage.azure.endpoint, a
console profile's endpoint) as the stores and the console take it: an http:// or https:// URL naming the
server alone, no path and no user, trimmed of surrounding whitespace and trailing slashes. A bare
host:port, DuckDB's own spelling, would pass the install and fail every store's first operation. Called
with (dict "value" <the value> "what" <where it is set>); renders nothing for an empty value.
*/}}
{{- define "midstream.endpoint" -}}
{{- $given := .value | default "" | toString | trim -}}
{{- if $given -}}
{{- if not (regexMatch `^(?i:https?)://[^/?#@\s:][^/?#@\s]*/*$` $given) -}}
{{- fail (printf "%s %q must be an http:// or https:// URL naming the server alone: http(s)://host[:port], with no path" .what $given) -}}
{{- end -}}
{{- regexReplaceAll "/+$" $given "" -}}
{{- end -}}
{{- end -}}

{{/* The GCS service-account key, when storage.gcs names one: a volume, and its mount at /etc/midstream/gcs. */}}
{{- define "midstream.storageVolumes" -}}
{{- with (.Values.storage | default dict).gcs | default dict }}
{{- with .credentialsSecret }}
volumes:
  - name: gcs-credentials
    secret:
      secretName: {{ . }}
      items:
        - key: service-account.json
          path: service-account.json
{{- end }}
{{- end }}
{{- end -}}

{{- define "midstream.storageVolumeMounts" -}}
{{- with (.Values.storage | default dict).gcs | default dict }}
{{- if .credentialsSecret }}
volumeMounts:
  - name: gcs-credentials
    mountPath: /etc/midstream/gcs
    readOnly: true
{{- end }}
{{- end }}
{{- end -}}

{{/*
The destination new materializations' tables go under (controlPlane.settings.defaultDestinationUri), as
given: empty, or one of the three forms engines read, which name the backend for good (handbook 11).
The patterns are the control plane's own, and so is the trimming: surrounding whitespace, and trailing
slashes before the patterns are checked. file:// serves the tests alone, never a deployment.
*/}}
{{- define "midstream.defaultDestinationUri" -}}
{{- $given := .Values.controlPlane.settings.defaultDestinationUri | default "" | toString | trim -}}
{{- $uri := regexReplaceAll "/+$" $given "" -}}
{{- if $given -}}
{{- $forms := "gs://bucket[/prefix], s3://bucket[/prefix] or abfss://container@account.dfs.core.windows.net[/prefix]" -}}
{{- $what := printf "controlPlane.settings.defaultDestinationUri %q" $given -}}
{{- if hasPrefix "file://" $given -}}
{{- fail (printf "%s: file:// destinations are for tests only; a deployment writes to %s" $what $forms) -}}
{{- end -}}
{{- if not (or (hasPrefix "gs://" $given) (hasPrefix "s3://" $given) (hasPrefix "abfss://" $given)) -}}
{{- fail (printf "%s must be %s" $what $forms) -}}
{{- end -}}
{{- if gt (len $uri) 1024 -}}
{{- fail (printf "%s is longer than 1024 characters" $what) -}}
{{- end -}}
{{- if hasPrefix "gs://" $given -}}
{{- if not (regexMatch `^gs://[a-z0-9][a-z0-9._-]*[a-z0-9](/\S*)?$` $uri) -}}
{{- fail (printf "%s is not gs://bucket[/prefix]: a bucket name is lower case letters, digits, dots, hyphens and underscores" $what) -}}
{{- end -}}
{{- else if hasPrefix "s3://" $given -}}
{{- if not (regexMatch `^s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9](/\S*)?$` $uri) -}}
{{- fail (printf "%s is not s3://bucket[/prefix]: a bucket name is 3 to 63 lower case letters, digits, dots and hyphens" $what) -}}
{{- end -}}
{{- else -}}
{{- $container := regexReplaceAll `^abfss://([^@/]*)@.*$` $uri "${1}" -}}
{{- if not (and (regexMatch `^abfss://[a-z0-9-]{3,63}@[a-z0-9]{3,24}\.dfs\.core\.windows\.net(/\S*)?$` $uri) (regexMatch `^[a-z0-9]+(-[a-z0-9]+)*$` $container)) -}}
{{- fail (printf "%s is not abfss://container@account.dfs.core.windows.net[/prefix]: a container is 3 to 63 lower case letters, digits and single hyphens, an account 3 to 24 lower case letters and digits" $what) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $given -}}
{{- end -}}

{{/*
The directory under the destination this installation's tables go in (controlPlane.settings.storagePrefix),
trimmed as the control plane trims it, of surrounding whitespace and then of slashes; the release name
when that leaves nothing. The control plane gets it as rendered here, and NOTES.txt checks it.
*/}}
{{- define "midstream.storagePrefix" -}}
{{- $given := .Values.controlPlane.settings.storagePrefix | default "" | toString | trim | trimAll "/" -}}
{{- default (include "midstream.fullname" .) $given -}}
{{- end -}}

{{/*
The console's storage profiles (console.storage; console/docs/07), as JSON, every rule checked: one per
destination prefix, matched to every object by the longest scope. Credentials never reach the file: they
come from each profile's Secret as CONSOLE_STORAGE_<NAME>_* (midstream.consoleStorageEnv) and, for gcs,
as the key file mounted at /etc/midstream-storage/<name>, named by the profile's keyFile.
*/}}
{{- define "midstream.consoleStorage" -}}
{{- $schemes := dict "gcs" "gs://" "s3" "s3://" "azure" "abfss://" -}}
{{- $forms := dict "gcs" "gs://bucket/[prefix/]" "s3" "s3://bucket/[prefix/]" "azure" "abfss://container@account.dfs.core.windows.net/[prefix/]" -}}
{{- $fields := list "name" "scope" "kind" "endpoint" "region" "pathStyle" "access" "credentialsSecret" -}}
{{- $out := list -}}
{{- $names := dict -}}
{{- $scopes := dict -}}
{{- range $index, $profile := .Values.console.storage | default list -}}
{{- $where := printf "console.storage[%d]" $index -}}
{{- if not (kindIs "map" $profile) -}}
{{- fail (printf "%s must be a profile: {name, scope, kind, ...}" $where) -}}
{{- end -}}
{{- $name := $profile.name | default "" | toString -}}
{{- if not (regexMatch `^[a-z][a-z0-9-]{0,39}$` $name) -}}
{{- fail (printf "%s: name %q must be a lower case letter followed by up to 39 lower case letters, digits or hyphens" $where $name) -}}
{{- end -}}
{{- $where = printf "console.storage[%d] (%s)" $index $name -}}
{{- if hasKey $names $name -}}
{{- fail (printf "%s: the name is used twice; every profile's name must be unique" $where) -}}
{{- end -}}
{{- $_ := set $names $name true -}}
{{- range $key, $value := $profile -}}
{{- if not (has $key $fields) -}}
{{- fail (printf "%s: unknown field %q; a profile takes %s" $where $key (join ", " $fields)) -}}
{{- end -}}
{{- end -}}
{{- $kind := $profile.kind | default "" | toString -}}
{{- if not (hasKey $schemes $kind) -}}
{{- fail (printf "%s: kind %q must be gcs, s3 or azure" $where $kind) -}}
{{- end -}}
{{- $scheme := get $schemes $kind -}}
{{- $scope := $profile.scope | default "" | toString -}}
{{- if not (hasSuffix "/" $scope) -}}
{{- fail (printf "%s: scope %q must end with '/': it is the destination prefix the profile serves" $where $scope) -}}
{{- end -}}
{{- if not (hasPrefix $scheme $scope) -}}
{{- fail (printf "%s: scope %q does not start with %s, the scheme of kind %s" $where $scope $scheme $kind) -}}
{{- end -}}
{{- /* As the console parses a location: a bucket, or a container at an account in the one Azure form; never . or .. */ -}}
{{- $authority := ternary `[^/@\s]+@[a-z0-9]{3,24}\.dfs\.core\.windows\.net` `[^/@\s]+` (eq $kind "azure") -}}
{{- $container := index (splitList "@" (index (splitList "/" (trimPrefix $scheme $scope)) 0)) 0 -}}
{{- if or (not (regexMatch (printf `^%s%s/\S*$` (regexQuoteMeta $scheme) $authority) $scope)) (has $container (list "." "..")) -}}
{{- fail (printf "%s: scope %q must name a %s and hold no whitespace: %s" $where $scope (ternary "container and its account" "bucket" (eq $kind "azure")) (get $forms $kind)) -}}
{{- end -}}
{{- if hasKey $scopes $scope -}}
{{- fail (printf "%s: scope %q is already served by profile %s" $where $scope (get $scopes $scope)) -}}
{{- end -}}
{{- $_ := set $scopes $scope $name -}}
{{- $entry := dict "name" $name "scope" $scope "kind" $kind -}}
{{- with include "midstream.endpoint" (dict "value" $profile.endpoint "what" (printf "%s: endpoint" $where)) -}}
{{- $_ := set $entry "endpoint" . -}}
{{- end -}}
{{- with $profile.region -}}
{{- $_ := set $entry "region" (toString .) -}}
{{- end -}}
{{- if not (kindIs "invalid" $profile.pathStyle) -}}
{{- if not (kindIs "bool" $profile.pathStyle) -}}
{{- fail (printf "%s: pathStyle must be true or false" $where) -}}
{{- end -}}
{{- $_ := set $entry "pathStyle" $profile.pathStyle -}}
{{- end -}}
{{- with $profile.access -}}
{{- if not (has (toString .) (list "direct" "cached")) -}}
{{- fail (printf "%s: access %q must be direct or cached" $where (toString .)) -}}
{{- end -}}
{{- $_ := set $entry "access" (toString .) -}}
{{- end -}}
{{- with $profile.credentialsSecret -}}
{{- if not (kindIs "string" .) -}}
{{- fail (printf "%s: credentialsSecret must be the name of a Secret" $where) -}}
{{- end -}}
{{- if eq $kind "gcs" -}}
{{- $_ := set $entry "keyFile" (printf "/etc/midstream-storage/%s/service-account.json" $name) -}}
{{- end -}}
{{- end -}}
{{- $out = append $out $entry -}}
{{- end -}}
{{- toJson $out -}}
{{- end -}}

{{/*
Each console storage profile's credentials, from its Secret, as CONSOLE_STORAGE_<NAME>_<KEY> (the name
in upper case, `-` as `_`): every key optional, so a profile uses what its Secret holds.
*/}}
{{- define "midstream.consoleStorageEnv" -}}
{{- $keys := dict -}}
{{- $_ := set $keys "gcs" (list (list "GCS_HMAC_KEY_ID" "hmac-key-id") (list "GCS_HMAC_SECRET" "hmac-secret")) -}}
{{- $_ := set $keys "s3" (list (list "ACCESS_KEY_ID" "access-key-id") (list "SECRET_ACCESS_KEY" "secret-access-key") (list "SESSION_TOKEN" "session-token")) -}}
{{- $_ := set $keys "azure" (list (list "AZURE_CONNECTION_STRING" "connection-string") (list "AZURE_ACCOUNT_KEY" "account-key") (list "AZURE_CLIENT_ID" "client-id") (list "AZURE_TENANT_ID" "tenant-id") (list "AZURE_CLIENT_SECRET" "client-secret")) -}}
{{- range $profile := .Values.console.storage | default list }}
{{- with $profile.credentialsSecret }}
{{- $secret := . }}
{{- $prefix := printf "CONSOLE_STORAGE_%s" ($profile.name | toString | upper | replace "-" "_") }}
{{- range $pair := get $keys (toString $profile.kind) | default list }}
- name: {{ $prefix }}_{{ index $pair 0 }}
  valueFrom:
    secretKeyRef:
      name: {{ $secret }}
      key: {{ index $pair 1 }}
      optional: true
{{- end }}
{{- end }}
{{- end }}
{{- end -}}

{{/* A gcs console profile's key file, when it names a Secret: its volume's name. */}}
{{- define "midstream.consoleStorageVolume" -}}
storage-{{ . }}-key
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
  {{- include "midstream.kasprServiceAccount" . | nindent 2 }}
  pod:
    {{- include "midstream.kasprPodMetadata" . | nindent 4 }}
    {{- include "midstream.podScheduling" (dict "root" . "own" .Values.controlPlane.runtime) | nindent 4 }}
    {{- include "midstream.storageVolumes" . | nindent 4 }}
  kasprContainer:
    env:
      - name: RUNTIME_MEMBER_ID
        valueFrom:
          fieldRef:
            fieldPath: metadata.name
      {{- include "midstream.storageEnv" . | nindent 6 }}
      {{- include "midstream.protectionKeyEnv" . | nindent 6 }}
      - name: PROTOCOL_LOGGING_ENABLED
        value: {{ .Values.controlPlane.runtime.protocolLogging | quote }}
      {{- with .Values.controlPlane.runtime.extraEnv }}
      {{- toYaml . | nindent 6 }}
      {{- end }}
    {{- include "midstream.storageVolumeMounts" . | nindent 4 }}
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
