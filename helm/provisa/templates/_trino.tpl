{{/*
REQ-2072: the coordinator's pinned address for Trino's internal TLS, required when it is on.
*/}}
{{- define "provisa.trinoCoordinatorIP" -}}
{{- required "provisa: trino.internalTls needs trino.coordinatorClusterIP, an unused address in the cluster's Service range: Trino 481 addresses the coordinator by IP alone for internal TLS." .Values.trino.coordinatorClusterIP -}}
{{- end }}
