{{/*
REQ-2072: Trino's internal TLS. The coordinator Service's DNS names, which the nodes' certificate
carries and the workers discover the coordinator at.
*/}}
{{- define "provisa.trinoCoordinatorHost" -}}
{{ .Release.Name }}-trino-coordinator
{{- end }}

{{/* REQ-2072: where each Trino pod mounts the internal TLS Secret. */}}
{{- define "provisa.trinoInternalTlsDir" -}}
/etc/trino/internal-tls
{{- end }}

{{/*
REQ-2072: a Trino node's lines for internal TLS. Each node is addressed by its pod DNS name, which
the chart's certificate covers, so the nodes verify each other's names with one certificate.
*/}}
{{- define "provisa.trinoInternalTlsConfig" -}}
node.internal-address=${ENV:PROVISA_TRINO_NODE_ADDRESS}
internal-communication.https.required=true
internal-communication.https.keystore.path={{ include "provisa.trinoInternalTlsDir" . }}/keystore.pem
internal-communication.https.truststore.path={{ include "provisa.trinoInternalTlsDir" . }}/truststore.pem
http-server.https.enabled=true
http-server.https.port=8443
http-server.https.keystore.path={{ include "provisa.trinoInternalTlsDir" . }}/keystore.pem
discovery.uri=https://{{ include "provisa.trinoCoordinatorHost" . }}:8443
{{- end }}

{{/*
REQ-2072: a Trino container's start under internal TLS. Its pod DNS name (the pod IP dashed, under
the namespace's pod domain) is known only once the pod has an IP, so it is set before Trino starts.
*/}}
{{- define "provisa.trinoInternalTlsStart" -}}
command:
  - /bin/bash
  - -c
  - export PROVISA_TRINO_NODE_ADDRESS="${POD_IP//./-}.${POD_NAMESPACE}.pod.cluster.local"; exec /usr/lib/trino/bin/run-trino
{{- end }}

{{/* REQ-2072: the env the start above reads. */}}
{{- define "provisa.trinoInternalTlsEnv" -}}
- name: POD_IP
  valueFrom:
    fieldRef:
      fieldPath: status.podIP
- name: POD_NAMESPACE
  valueFrom:
    fieldRef:
      fieldPath: metadata.namespace
{{- end }}
