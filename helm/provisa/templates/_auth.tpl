{{/*
REQ-1265: the `auth` block of the API's provisa.yaml, from .Values.auth.

Every credential is an ${env:...} reference; the Deployment gives the API each variable from the
operator's Kubernetes Secret (see provisa.authEnv below). Nothing secret is rendered here.
Renders nothing for provider `none`.
*/}}
{{- define "provisa.authSessionSecretRef" -}}
{{- if .Values.auth.sessionSecret.existingSecret }}
jwt_secret: ${env:PROVISA_AUTH_SESSION_SECRET}
{{- end }}
{{- end -}}

{{- define "provisa.authRequireSessionSecret" -}}
{{- if not .Values.auth.sessionSecret.existingSecret -}}
{{- fail (printf "provisa: auth.provider=%s signs browser sessions, so auth.sessionSecret.existingSecret must name a Secret holding the signing key (kubectl create secret generic provisa-session --from-literal=session-secret=\"$(openssl rand -base64 48)\")." .Values.auth.provider) -}}
{{- end -}}
{{- end -}}

{{- define "provisa.auth" -}}
{{- $a := .Values.auth -}}
{{- $choices := list "none" "oidc" "saml" "ldap" "local" -}}
{{- if not $a.provider -}}
{{- fail "provisa: auth.provider is not set. Choose who may sign in: oidc (a corporate OpenID Connect issuer), saml, ldap (a directory), local (the break-glass account alone), or none (no authentication: every request is trusted; development only). e.g. --set auth.provider=oidc" -}}
{{- end -}}
{{- if not (has $a.provider $choices) -}}
{{- fail (printf "provisa: auth.provider is %q; it must be one of: none, oidc, saml, ldap, local." (toString $a.provider)) -}}
{{- end -}}
{{- $bg := $a.breakGlass -}}
{{- if and (or $bg.username $bg.existingSecret) (not (and $bg.username $bg.existingSecret)) -}}
{{- fail "provisa: auth.breakGlass.username and auth.breakGlass.existingSecret are set together or not at all." -}}
{{- end -}}
{{- if and $bg.username (eq $a.provider "none") -}}
{{- fail "provisa: auth.breakGlass needs an auth provider; with auth.provider=none the deployment is unsecured. Use auth.provider=local for the break-glass account alone." -}}
{{- end -}}
{{- if ne $a.provider "none" }}
auth:
  {{- if eq $a.provider "oidc" }}
  provider: oidc
  oidc:
    discovery_url: {{ printf "%s/.well-known/openid-configuration" (trimSuffix "/" (required "provisa: auth.provider=oidc needs auth.oidc.issuerUrl (the issuer's base URL)." $a.oidc.issuerUrl)) | quote }}
    client_id: {{ required "provisa: auth.provider=oidc needs auth.oidc.clientId." $a.oidc.clientId | quote }}
    {{- with $a.oidc.audience }}
    audience: {{ . | quote }}
    {{- end }}
    {{- with $a.oidc.roleClaim }}
    role_claim: {{ . | quote }}
    {{- end }}
  {{- else if eq $a.provider "ldap" }}
  {{- include "provisa.authRequireSessionSecret" . }}
  provider: ldap
  ldap:
    server_url: {{ required "provisa: auth.provider=ldap needs auth.ldap.server (ldap://host:389 or ldaps://host:636)." $a.ldap.server | quote }}
    start_tls: {{ $a.ldap.startTls }}
    {{- with $a.ldap.caCertFile }}
    ca_cert_file: {{ . | quote }}
    {{- end }}
    bind_dn: {{ required "provisa: auth.provider=ldap needs auth.ldap.bindDn (the service account that searches the directory)." $a.ldap.bindDn | quote }}
    {{- if not $a.ldap.bindPassword.existingSecret }}
    {{- fail "provisa: auth.provider=ldap needs auth.ldap.bindPassword.existingSecret (a Secret holding the service account's password)." }}
    {{- end }}
    bind_password: ${env:PROVISA_AUTH_LDAP_BIND_PASSWORD}
    user_base_dn: {{ required "provisa: auth.provider=ldap needs auth.ldap.baseDn (the subtree users are searched under)." $a.ldap.baseDn | quote }}
    user_filter: {{ required "provisa: auth.provider=ldap needs auth.ldap.userFilter, e.g. (uid={username}) or (sAMAccountName={username})." $a.ldap.userFilter | quote }}
    user_id_attribute: {{ required "provisa: auth.provider=ldap needs auth.ldap.userIdAttribute, e.g. uid." $a.ldap.userIdAttribute | quote }}
    {{- with $a.ldap.emailAttribute }}
    email_attribute: {{ . | quote }}
    {{- end }}
    {{- with $a.ldap.displayNameAttribute }}
    display_name_attribute: {{ . | quote }}
    {{- end }}
    {{- with $a.ldap.groupBaseDn }}
    group_base_dn: {{ . | quote }}
    {{- end }}
    {{- with $a.ldap.groupFilter }}
    group_filter: {{ . | quote }}
    {{- end }}
    {{- with $a.ldap.groupNameAttribute }}
    group_name_attribute: {{ . | quote }}
    {{- end }}
  {{- else if eq $a.provider "saml" }}
  {{- include "provisa.authRequireSessionSecret" . }}
  {{- $public := trimSuffix "/" (required "provisa: auth.provider=saml needs auth.saml.publicUrl, the address browsers reach Provisa at (e.g. https://provisa.example.com)." $a.saml.publicUrl) }}
  provider: saml
  saml:
    idp_metadata_url: {{ required "provisa: auth.provider=saml needs auth.saml.idpMetadataUrl (the identity provider's metadata URL)." $a.saml.idpMetadataUrl | quote }}
    sp_entity_id: {{ printf "%s/auth/saml/metadata" $public | quote }}
    acs_url: {{ printf "%s/auth/saml/acs" $public | quote }}
    login_page_url: {{ printf "%s/login" $public | quote }}
    {{- with $a.saml.userIdAttribute }}
    user_id_attribute: {{ . | quote }}
    {{- end }}
    {{- with $a.saml.emailAttribute }}
    email_attribute: {{ . | quote }}
    {{- end }}
    {{- with $a.saml.displayNameAttribute }}
    display_name_attribute: {{ . | quote }}
    {{- end }}
    {{- with $a.saml.groupsAttribute }}
    groups_attribute: {{ . | quote }}
    {{- end }}
  {{- else if eq $a.provider "local" }}
  {{- include "provisa.authRequireSessionSecret" . }}
  {{- if not $bg.username }}
  {{- fail "provisa: auth.provider=local is the break-glass account alone, so auth.breakGlass.username and auth.breakGlass.existingSecret are required." }}
  {{- end }}
  provider: basic
  # REQ-1265: the break-glass account is the only sign-in; no other account can be created.
  allow_registration: false
  {{- end }}
  {{- include "provisa.authSessionSecretRef" . | indent 2 }}
  {{- if $bg.username }}
  {{- include "provisa.authRequireSessionSecret" . }}
  superuser:
    username: {{ $bg.username | quote }}
    password: ${env:PROVISA_AUTH_BREAK_GLASS_PASSWORD}
  {{- end }}
  {{- with $a.defaultRole }}
  default_role: {{ . | quote }}
  {{- end }}
  {{- with $a.roleMapping }}
  role_mapping:
    {{- toYaml . | nindent 4 }}
  {{- end }}
{{- end }}
{{- end -}}

{{/*
The env entries that carry the auth credentials from the operator's Secrets to the API.
*/}}
{{- define "provisa.authEnv" -}}
{{- $a := .Values.auth -}}
{{- if and $a.provider (ne $a.provider "none") }}
{{- if $a.sessionSecret.existingSecret }}
- name: PROVISA_AUTH_SESSION_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ $a.sessionSecret.existingSecret }}
      key: {{ $a.sessionSecret.secretKey }}
{{- end }}
{{- if $a.breakGlass.existingSecret }}
- name: PROVISA_AUTH_BREAK_GLASS_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ $a.breakGlass.existingSecret }}
      key: {{ $a.breakGlass.secretKey }}
{{- end }}
{{- if and (eq $a.provider "ldap") $a.ldap.bindPassword.existingSecret }}
- name: PROVISA_AUTH_LDAP_BIND_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ $a.ldap.bindPassword.existingSecret }}
      key: {{ $a.ldap.bindPassword.secretKey }}
{{- end }}
{{- end }}
{{- end -}}
