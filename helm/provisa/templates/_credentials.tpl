{{/*
The deployment's store credentials: a Secret the operator creates, never one the chart makes, so
no password has a default. Keys: pg-password, redis-password, minio-access-key, minio-secret-key,
grafana-admin-password (the last only with observability.enabled).
*/}}
{{- define "provisa.credentialsSecret" -}}
{{- if not .Values.credentials.existingSecret -}}
{{- fail "provisa: credentials.existingSecret must name a Secret holding the stores' credentials. Create one, e.g. kubectl create secret generic provisa-credentials --from-literal=pg-password=\"$(openssl rand -hex 24)\" --from-literal=redis-password=\"$(openssl rand -hex 24)\" --from-literal=minio-access-key=provisa --from-literal=minio-secret-key=\"$(openssl rand -hex 24)\" --from-literal=grafana-admin-password=\"$(openssl rand -hex 24)\"" -}}
{{- end -}}
{{- .Values.credentials.existingSecret -}}
{{- end -}}
