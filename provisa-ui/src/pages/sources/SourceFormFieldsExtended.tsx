// Copyright (c) 2026 Kenneth Stott
// Canary: 55ecea78-4131-4bb4-8a93-71e1bdacea84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useTranslation } from "react-i18next";
import {
  Accordion,
  Button,
  Checkbox,
  Group,
  NumberInput,
  PasswordInput,
  Select,
  Stack,
  Text,
  TextInput,
  Tooltip,
} from "@mantine/core";
import { MultiSelect } from "../../components/MultiSelect";
import { cdcTransportApplicable, sourceChangeSignals } from "../../liveCapability";
import {
  API_AUTH_TYPES,
  CHANGE_SIGNAL_LABELS,
  FILE_TRANSPORTS,
  GOVDATA_SUBJECTS,
  KAFKA_AUTH_TYPES,
  NAMING_CONVENTIONS,
} from "./constants";
import { getCategory } from "./sourceHelpers";
import { AuthUserPass } from "./AuthUserPass";
import { OpenApiFormSection } from "./OpenApiFormSection";
import type { SourceFormFieldsProps } from "./SourceFormFields";

export function SourceFormFieldsExtended({
  form,
  setForm,
  authType,
  setAuthType,
  authFields,
  setAuthFields,
  editingSourceId,
  cdc,
  setCdc,
  domainsEnabled,
  domains,
  spAuthType,
  setSpAuthType,
  spCertPath,
  setSpCertPath,
  spCertPassword,
  setSpCertPassword,
  spUsername,
  setSpUsername,
  spPassword,
  setSpPassword,
  splunkDisableSsl,
  setSplunkDisableSsl,
  filesTransport,
  setFilesTransport,
  filesAuthMode,
  setFilesAuthMode,
  filesCertPath,
  setFilesCertPath,
  filesCertPassword,
  setFilesCertPassword,
  govdataSubjects,
  setGovdataSubjects,
  submitting,
  openapiSpecPath,
  setOpenapiSpecPath,
  openapiSpecInline,
  setOpenapiSpecInline,
  openapiSpecMode,
  setOpenapiSpecMode,
  openapiBaseUrl,
  setOpenapiBaseUrl,
  openapiCacheTtl,
  setOpenapiCacheTtl,
  openapiPreview,
  openapiPreviewing,
  openapiPreviewError,
  onOpenapiPreview,
  gqlNamespace,
  setGqlNamespace,
  gqlCacheTtl,
  setGqlCacheTtl,
  grpcProtoPath,
  setGrpcProtoPath,
  grpcServerAddress,
  setGrpcServerAddress,
  grpcNamespace,
  setGrpcNamespace,
  grpcTls,
  setGrpcTls,
  grpcImportPaths,
  setGrpcImportPaths,
  grpcCacheTtl,
  setGrpcCacheTtl,
}: SourceFormFieldsProps) {
  const { t } = useTranslation();
  const isKafka = getCategory(form.type) === "Streaming";
  const simpleAuthData = [
    { value: "none", label: t("sourceFormFieldsExtended.authNone") },
    { value: "basic", label: t("sourceFormFieldsExtended.authBasic") },
    { value: "bearer", label: t("sourceFormFieldsExtended.authBearer") },
  ];
  return (
    <>
      {form.type === "prometheus" && (
        <>
          <TextInput
            required
            label={t("sourceFormFieldsExtended.url")}
            value={form.host}
            onChange={(e) => setForm({ ...form, host: e.target.value })}
            placeholder="http://prometheus:9090"
            style={{ gridColumn: "1 / -1" }}
            data-testid="prometheus-url-input"
          />
          <Select
            label={t("sourceFormFieldsExtended.authentication")}
            value={authType}
            onChange={(v) => {
              setAuthType(v ?? "");
              setAuthFields({});
            }}
            data={simpleAuthData}
            allowDeselect={false}
            style={{ gridColumn: "1 / -1" }}
            data-testid="prometheus-auth-type-select"
          />
          {authType === "basic" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
          {authType === "bearer" && (
            <TextInput
              required
              label={t("sourceFormFieldsExtended.token")}
              value={authFields.token ?? ""}
              onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })}
              style={{ gridColumn: "1 / -1" }}
              data-testid="prometheus-token-input"
            />
          )}
        </>
      )}
      {form.type === "google_sheets" && (
        <>
          <TextInput
            required
            label={t("sourceFormFieldsExtended.credentialsJsonPath")}
            value={authFields.credentials_json ?? ""}
            onChange={(e) => setAuthFields({ ...authFields, credentials_json: e.target.value })}
            placeholder={t("sourceFormFieldsExtended.credentialsJsonPathPlaceholder")}
            style={{ gridColumn: "1 / -1" }}
            data-testid="google-sheets-credentials-input"
          />
          <TextInput
            label={t("sourceFormFieldsExtended.metadataSheetId")}
            value={form.database}
            onChange={(e) => setForm({ ...form, database: e.target.value })}
            placeholder={t("sourceFormFieldsExtended.metadataSheetIdPlaceholder")}
            style={{ gridColumn: "1 / -1" }}
            data-testid="google-sheets-sheet-id-input"
          />
        </>
      )}
      {form.type === "openapi" && (
        <OpenApiFormSection
          openapiSpecMode={openapiSpecMode}
          setOpenapiSpecMode={setOpenapiSpecMode}
          openapiSpecPath={openapiSpecPath}
          setOpenapiSpecPath={setOpenapiSpecPath}
          openapiSpecInline={openapiSpecInline}
          setOpenapiSpecInline={setOpenapiSpecInline}
          openapiBaseUrl={openapiBaseUrl}
          setOpenapiBaseUrl={setOpenapiBaseUrl}
          openapiCacheTtl={openapiCacheTtl}
          setOpenapiCacheTtl={setOpenapiCacheTtl}
          authType={authType}
          setAuthType={setAuthType}
          authFields={authFields}
          setAuthFields={setAuthFields}
          onOpenapiPreview={onOpenapiPreview}
          openapiPreviewing={openapiPreviewing}
          openapiPreviewError={openapiPreviewError}
          openapiPreview={openapiPreview}
        />
      )}
      {form.type === "graphql" && (
        <>
          <TextInput
            required
            label={t("sourceFormFieldsExtended.endpointUrl")}
            value={form.host}
            onChange={(e) => setForm({ ...form, host: e.target.value })}
            placeholder="https://api.example.com/graphql"
            style={{ gridColumn: "1 / -1" }}
            data-testid="graphql-endpoint-input"
          />
          <TextInput
            required
            label={t("sourceFormFieldsExtended.namespace")}
            value={gqlNamespace}
            onChange={(e) => setGqlNamespace(e.target.value)}
            placeholder="myapi"
            data-testid="graphql-namespace-input"
          />
          <NumberInput
            label={t("sourceFormFieldsExtended.cacheTtlSeconds")}
            min={0}
            value={gqlCacheTtl === "" ? "" : Number(gqlCacheTtl)}
            onChange={(v) => setGqlCacheTtl(v === "" ? "" : String(v))}
            placeholder="300"
            data-testid="graphql-cache-ttl-input"
          />
          <Select
            label={t("sourceFormFieldsExtended.authentication")}
            value={authType}
            onChange={(v) => {
              setAuthType(v ?? "");
              setAuthFields({});
            }}
            data={[
              { value: "none", label: t("sourceFormFieldsExtended.authNone") },
              { value: "bearer", label: t("sourceFormFieldsExtended.authBearer") },
              { value: "basic", label: t("sourceFormFieldsExtended.authBasic") },
            ]}
            allowDeselect={false}
            style={{ gridColumn: "1 / -1" }}
            data-testid="graphql-auth-type-select"
          />
          {authType === "bearer" && (
            <TextInput
              required
              label={t("sourceFormFieldsExtended.token")}
              value={authFields.token ?? ""}
              onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })}
              placeholder="${env:GQL_TOKEN}"
              style={{ gridColumn: "1 / -1" }}
              data-testid="graphql-token-input"
            />
          )}
          {authType === "basic" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
        </>
      )}
      {form.type === "grpc" && (
        <>
          <TextInput
            required
            label={t("sourceFormFieldsExtended.protoPathOrUrl")}
            value={grpcProtoPath}
            onChange={(e) => setGrpcProtoPath(e.target.value)}
            placeholder="https://api.example.com/service.proto or ./service.proto"
            style={{ gridColumn: "1 / -1" }}
            data-testid="grpc-proto-path-input"
          />
          <TextInput
            required
            label={t("sourceFormFieldsExtended.serverAddress")}
            value={grpcServerAddress}
            onChange={(e) => setGrpcServerAddress(e.target.value)}
            placeholder="api.example.com:50051"
            style={{ gridColumn: "1 / -1" }}
            data-testid="grpc-server-address-input"
          />
          <TextInput
            label={t("sourceFormFieldsExtended.namespace")}
            value={grpcNamespace}
            onChange={(e) => setGrpcNamespace(e.target.value)}
            placeholder="mygrpc"
            data-testid="grpc-namespace-input"
          />
          <NumberInput
            label={t("sourceFormFieldsExtended.cacheTtlSeconds")}
            min={0}
            value={grpcCacheTtl === "" ? "" : Number(grpcCacheTtl)}
            onChange={(v) => setGrpcCacheTtl(v === "" ? "" : String(v))}
            placeholder="300"
            data-testid="grpc-cache-ttl-input"
          />
          <Checkbox
            label={t("sourceFormFieldsExtended.tls")}
            checked={grpcTls}
            onChange={(e) => setGrpcTls(e.currentTarget.checked)}
            mt="1.5rem"
            data-testid="grpc-tls-checkbox"
          />
          <TextInput
            label={
              <>
                {t("sourceFormFieldsExtended.importPaths")}{" "}
                <Text component="span" c="dimmed" size="xs">
                  {t("sourceFormFieldsExtended.importPathsHint")}
                </Text>
              </>
            }
            value={grpcImportPaths}
            onChange={(e) => setGrpcImportPaths(e.target.value)}
            placeholder="/path/to/protos,/another/path"
            style={{ gridColumn: "1 / -1" }}
            data-testid="grpc-import-paths-input"
          />
          <Select
            label={t("sourceFormFieldsExtended.authentication")}
            value={authType}
            onChange={(v) => {
              setAuthType(v ?? "");
              setAuthFields({});
            }}
            data={API_AUTH_TYPES.map((a) => ({ value: a.value, label: a.label }))}
            allowDeselect={false}
            style={{ gridColumn: "1 / -1" }}
            data-testid="grpc-auth-type-select"
          />
          {authType === "bearer" && (
            <TextInput
              required
              label={t("sourceFormFieldsExtended.token")}
              value={authFields.token ?? ""}
              onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })}
              placeholder="${env:GRPC_TOKEN}"
              style={{ gridColumn: "1 / -1" }}
              data-testid="grpc-token-input"
            />
          )}
          {authType === "basic" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
          {authType === "api_key" && (
            <>
              <TextInput
                required
                label={t("sourceFormFieldsExtended.headerName")}
                value={authFields.header_name ?? "X-API-Key"}
                onChange={(e) => setAuthFields({ ...authFields, header_name: e.target.value })}
                placeholder="X-API-Key"
                data-testid="grpc-header-name-input"
              />
              <TextInput
                required
                label={t("sourceFormFieldsExtended.apiKey")}
                value={authFields.api_key ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, api_key: e.target.value })}
                placeholder="${env:API_KEY}"
                data-testid="grpc-api-key-input"
              />
            </>
          )}
        </>
      )}
      {form.type === "neo4j" && (
        <>
          <TextInput
            required
            label={t("sourceFormFieldsExtended.host")}
            value={form.host}
            onChange={(e) => setForm({ ...form, host: e.target.value })}
            placeholder="localhost"
            data-testid="neo4j-host-input"
          />
          <NumberInput
            required
            label={t("sourceFormFieldsExtended.port")}
            value={form.port}
            onChange={(v) => setForm({ ...form, port: Number(v) || 0 })}
            data-testid="neo4j-port-input"
          />
          <TextInput
            label={t("sourceFormFieldsExtended.database")}
            value={form.database}
            onChange={(e) => setForm({ ...form, database: e.target.value })}
            placeholder="neo4j"
            data-testid="neo4j-database-input"
          />
          <Checkbox
            label={t("sourceFormFieldsExtended.https")}
            checked={authFields.use_https === "true"}
            onChange={(e) =>
              setAuthFields({ ...authFields, use_https: e.currentTarget.checked ? "true" : "false" })
            }
            mt="1.5rem"
            data-testid="neo4j-https-checkbox"
          />
          <TextInput
            label={t("sourceFormFieldsExtended.username")}
            value={form.username}
            onChange={(e) => setForm({ ...form, username: e.target.value })}
            placeholder="neo4j"
            data-testid="neo4j-username-input"
          />
          <PasswordInput
            label={t("sourceFormFieldsExtended.password")}
            value={form.password}
            onChange={(e) => setForm({ ...form, password: e.target.value })}
            data-testid="neo4j-password-input"
          />
        </>
      )}
      {form.type === "sparql" && (
        <>
          <TextInput
            required
            label={t("sourceFormFieldsExtended.endpointUrl")}
            value={form.host}
            onChange={(e) => setForm({ ...form, host: e.target.value })}
            placeholder="https://dbpedia.org/sparql"
            style={{ gridColumn: "1 / -1" }}
            data-testid="sparql-endpoint-input"
          />
          <Select
            label={t("sourceFormFieldsExtended.authentication")}
            value={authType}
            onChange={(v) => {
              setAuthType(v ?? "");
              setAuthFields({});
            }}
            data={[
              { value: "none", label: t("sourceFormFieldsExtended.authNone") },
              { value: "bearer", label: t("sourceFormFieldsExtended.authBearer") },
              { value: "basic", label: t("sourceFormFieldsExtended.authBasic") },
            ]}
            allowDeselect={false}
            style={{ gridColumn: "1 / -1" }}
            data-testid="sparql-auth-type-select"
          />
          {authType === "bearer" && (
            <TextInput
              required
              label={t("sourceFormFieldsExtended.token")}
              value={authFields.token ?? ""}
              onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })}
              placeholder="${env:SPARQL_TOKEN}"
              style={{ gridColumn: "1 / -1" }}
              data-testid="sparql-token-input"
            />
          )}
          {authType === "basic" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
        </>
      )}
      {form.type === "govdata" && (
        <>
          <Checkbox.Group
            label={t("sourceFormFieldsExtended.dataSubjects")}
            value={govdataSubjects}
            onChange={setGovdataSubjects}
            style={{ gridColumn: "1 / -1" }}
          >
            <Group gap="sm" mt="0.25rem">
              {GOVDATA_SUBJECTS.map((subj) => (
                <Checkbox
                  key={subj.value}
                  value={subj.value}
                  label={subj.label}
                  data-testid={`govdata-subject-${subj.value}`}
                />
              ))}
            </Group>
          </Checkbox.Group>
          <Group style={{ gridColumn: "1 / -1" }} gap="sm" align="flex-end">
            <TextInput
              label={t("sourceFormFieldsExtended.askAmericaApiKey")}
              value={form.username}
              onChange={(e) => setForm({ ...form, username: e.target.value })}
              placeholder="${ASKAMERICA_API_KEY}"
              style={{ flex: 1 }}
              data-testid="govdata-api-key-input"
            />
            <Button
              type="button"
              variant="default"
              component="a"
              href="https://askamerica.ai"
              target="_blank"
              rel="noreferrer"
              data-testid="govdata-get-api-key-link"
            >
              {t("sourceFormFieldsExtended.getApiKey")}
            </Button>
          </Group>
          {submitting && !editingSourceId && (
            <Text c="dimmed" size="sm" style={{ gridColumn: "1 / -1" }}>
              {t("sourceFormFieldsExtended.validatingApiKey")}
            </Text>
          )}
        </>
      )}
      {isKafka && (
        <>
          <TextInput
            required
            label={t("sourceFormFieldsExtended.bootstrapServers")}
            value={form.host}
            onChange={(e) => setForm({ ...form, host: e.target.value })}
            placeholder="kafka:9092"
            data-testid="kafka-bootstrap-servers-input"
          />
          <TextInput
            label={t("sourceFormFieldsExtended.schemaRegistryUrl")}
            value={form.database}
            onChange={(e) => setForm({ ...form, database: e.target.value })}
            placeholder="http://schema-registry:8081"
            data-testid="kafka-schema-registry-input"
          />
          <Select
            label={t("sourceFormFieldsExtended.authentication")}
            value={authType}
            onChange={(v) => {
              setAuthType(v ?? "");
              setAuthFields({});
            }}
            data={KAFKA_AUTH_TYPES.map((a) => ({ value: a.value, label: a.label }))}
            allowDeselect={false}
            style={{ gridColumn: "1 / -1" }}
            data-testid="kafka-auth-type-select"
          />
          {authType !== "none" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
        </>
      )}
      {form.type === "sharepoint" && (
        <>
          <TextInput
            required
            label={t("sourceFormFieldsExtended.siteUrl")}
            value={form.host}
            onChange={(e) => setForm({ ...form, host: e.target.value })}
            placeholder="https://contoso.sharepoint.com/sites/mysite"
            style={{ gridColumn: "1 / -1" }}
            data-testid="sharepoint-site-url-input"
          />
          <TextInput
            required
            label={t("sourceFormFieldsExtended.tenantId")}
            value={form.database}
            onChange={(e) => setForm({ ...form, database: e.target.value })}
            placeholder={t("sourceFormFieldsExtended.tenantIdPlaceholder")}
            data-testid="sharepoint-tenant-id-input"
          />
          <Select
            label={t("sourceFormFieldsExtended.authType")}
            value={spAuthType}
            onChange={(v) => setSpAuthType(v ?? "")}
            data={[
              { value: "CLIENT_CREDENTIALS", label: t("sourceFormFieldsExtended.clientCredentials") },
              { value: "USERNAME_PASSWORD", label: t("sourceFormFieldsExtended.usernamePassword") },
              { value: "CERTIFICATE", label: t("sourceFormFieldsExtended.certificate") },
            ]}
            allowDeselect={false}
            data-testid="sharepoint-auth-type-select"
          />
          <TextInput
            required
            label={t("sourceFormFieldsExtended.clientId")}
            value={form.username}
            onChange={(e) => setForm({ ...form, username: e.target.value })}
            placeholder={t("sourceFormFieldsExtended.clientIdPlaceholder")}
            data-testid="sharepoint-client-id-input"
          />
          {spAuthType === "CLIENT_CREDENTIALS" && (
            <PasswordInput
              label={t("sourceFormFieldsExtended.clientSecret")}
              value={form.password}
              onChange={(e) => setForm({ ...form, password: e.target.value })}
              placeholder={t("sourceFormFieldsExtended.clientSecretPlaceholder")}
              data-testid="sharepoint-client-secret-input"
            />
          )}
          {spAuthType === "USERNAME_PASSWORD" && (
            <>
              <TextInput
                required
                label={t("sourceFormFieldsExtended.spUsername")}
                value={spUsername}
                onChange={(e) => setSpUsername(e.target.value)}
                placeholder="user@contoso.com"
                data-testid="sharepoint-sp-username-input"
              />
              <PasswordInput
                required
                label={t("sourceFormFieldsExtended.spPassword")}
                value={spPassword}
                onChange={(e) => setSpPassword(e.target.value)}
                data-testid="sharepoint-sp-password-input"
              />
            </>
          )}
          {spAuthType === "CERTIFICATE" && (
            <>
              <PasswordInput
                label={t("sourceFormFieldsExtended.clientSecret")}
                value={form.password}
                onChange={(e) => setForm({ ...form, password: e.target.value })}
                placeholder={t("sourceFormFieldsExtended.clientSecretCertPlaceholder")}
                data-testid="sharepoint-cert-client-secret-input"
              />
              <TextInput
                required
                label={t("sourceFormFieldsExtended.certificatePath")}
                value={spCertPath}
                onChange={(e) => setSpCertPath(e.target.value)}
                placeholder="/certs/sharepoint.pfx"
                style={{ gridColumn: "1 / -1" }}
                data-testid="sharepoint-cert-path-input"
              />
              <PasswordInput
                label={t("sourceFormFieldsExtended.certificatePassword")}
                value={spCertPassword}
                onChange={(e) => setSpCertPassword(e.target.value)}
                data-testid="sharepoint-cert-password-input"
              />
            </>
          )}
        </>
      )}
      {form.type === "splunk" && (
        <>
          <TextInput
            required
            label={t("sourceFormFieldsExtended.host")}
            value={form.host}
            onChange={(e) => setForm({ ...form, host: e.target.value })}
            placeholder="splunk.example.com"
            data-testid="splunk-host-input"
          />
          <NumberInput
            label={t("sourceFormFieldsExtended.port")}
            value={form.port}
            onChange={(v) => setForm({ ...form, port: Number(v) || 0 })}
            data-testid="splunk-port-input"
          />
          <PasswordInput
            required
            label={t("sourceFormFieldsExtended.authToken")}
            value={form.password}
            onChange={(e) => setForm({ ...form, password: e.target.value })}
            placeholder={t("sourceFormFieldsExtended.authToken")}
            style={{ gridColumn: "1 / -1" }}
            data-testid="splunk-auth-token-input"
          />
          <TextInput
            label={t("sourceFormFieldsExtended.appOptional")}
            value={form.database}
            onChange={(e) => setForm({ ...form, database: e.target.value })}
            placeholder="Splunk_SA_CIM"
            style={{ gridColumn: "1 / -1" }}
            data-testid="splunk-app-input"
          />
          <Checkbox
            label={t("sourceFormFieldsExtended.disableSslValidation")}
            checked={splunkDisableSsl}
            onChange={(e) => setSplunkDisableSsl(e.currentTarget.checked)}
            data-testid="splunk-disable-ssl-checkbox"
          />
        </>
      )}
      {form.type === "files" &&
        (() => {
          const transportMeta = FILE_TRANSPORTS.find((t2) => t2.value === filesTransport)!;
          return (
            <>
              <Select
                label={t("sourceFormFieldsExtended.transport")}
                value={filesTransport}
                onChange={(v) => {
                  setFilesTransport(v ?? "");
                  setForm({ ...form, username: "", password: "" });
                }}
                data={FILE_TRANSPORTS.map((tr) => ({ value: tr.value, label: tr.label }))}
                allowDeselect={false}
                data-testid="files-transport-select"
              />
              <TextInput
                required
                label={
                  <>
                    {t("sourceFormFieldsExtended.pathGlob")}{" "}
                    <Text component="span" c="dimmed" size="xs">
                      {t("sourceFormFieldsExtended.pathGlobHint")}
                    </Text>
                  </>
                }
                value={form.path}
                onChange={(e) => setForm({ ...form, path: e.target.value })}
                placeholder={filesTransport === "file://" ? "/data/files/**" : "bucket/prefix/**"}
                data-testid="files-path-input"
              />
              {transportMeta.needsAuth === "s3" && (
                <>
                  <TextInput
                    label={t("sourceFormFieldsExtended.accessKeyId")}
                    value={form.username}
                    onChange={(e) => setForm({ ...form, username: e.target.value })}
                    data-testid="files-access-key-input"
                  />
                  <PasswordInput
                    label={t("sourceFormFieldsExtended.secretAccessKey")}
                    value={form.password}
                    onChange={(e) => setForm({ ...form, password: e.target.value })}
                    data-testid="files-secret-key-input"
                  />
                </>
              )}
              {transportMeta.needsAuth === "userpass" && (
                <>
                  <TextInput
                    label={t("sourceFormFieldsExtended.username")}
                    value={form.username}
                    onChange={(e) => setForm({ ...form, username: e.target.value })}
                    data-testid="files-username-input"
                  />
                  <PasswordInput
                    label={t("sourceFormFieldsExtended.password")}
                    value={form.password}
                    onChange={(e) => setForm({ ...form, password: e.target.value })}
                    data-testid="files-password-input"
                  />
                </>
              )}
              {transportMeta.needsAuth === "cert-or-userpass" && (
                <>
                  <Select
                    label={t("sourceFormFieldsExtended.authMethod")}
                    value={filesAuthMode}
                    onChange={(v) => {
                      setFilesAuthMode(v as "userpass" | "certificate");
                      setForm({ ...form, username: "", password: "" });
                      setFilesCertPath("");
                      setFilesCertPassword("");
                    }}
                    data={[
                      { value: "userpass", label: t("sourceFormFieldsExtended.usernamePassword") },
                      { value: "certificate", label: t("sourceFormFieldsExtended.certificatePfx") },
                    ]}
                    allowDeselect={false}
                    data-testid="files-auth-method-select"
                  />
                  {filesAuthMode === "userpass" && (
                    <>
                      <TextInput
                        label={t("sourceFormFieldsExtended.username")}
                        value={form.username}
                        onChange={(e) => setForm({ ...form, username: e.target.value })}
                        data-testid="files-userpass-username-input"
                      />
                      <PasswordInput
                        label={t("sourceFormFieldsExtended.password")}
                        value={form.password}
                        onChange={(e) => setForm({ ...form, password: e.target.value })}
                        data-testid="files-userpass-password-input"
                      />
                    </>
                  )}
                  {filesAuthMode === "certificate" && (
                    <>
                      <TextInput
                        label={t("sourceFormFieldsExtended.pfxCertificatePath")}
                        value={filesCertPath}
                        onChange={(e) => setFilesCertPath(e.target.value)}
                        placeholder="/path/to/cert.pfx"
                        data-testid="files-cert-path-input"
                      />
                      <PasswordInput
                        label={t("sourceFormFieldsExtended.certificatePassword")}
                        value={filesCertPassword}
                        onChange={(e) => setFilesCertPassword(e.target.value)}
                        data-testid="files-cert-password-input"
                      />
                    </>
                  )}
                </>
              )}
            </>
          );
        })()}
      {editingSourceId && (
        <>
          <Select
            label={t("sourceFormFieldsExtended.namingConvention")}
            value={form.gqlNamingConvention}
            onChange={(v) => setForm({ ...form, gqlNamingConvention: v ?? "" })}
            data={NAMING_CONVENTIONS.map((nc) => ({ value: nc.value, label: nc.label }))}
            allowDeselect={false}
            data-testid="naming-convention-select"
          />
          <Group gap="lg" style={{ gridColumn: "1 / -1" }} wrap="wrap">
            <Checkbox
              label={t("sourceFormFieldsExtended.cacheEnabled")}
              checked={form.cacheEnabled}
              onChange={(e) => setForm({ ...form, cacheEnabled: e.currentTarget.checked })}
              data-testid="cache-enabled-checkbox"
            />
            <Tooltip label={t("sourceFormFieldsExtended.preferMaterializedTooltip")} multiline w={280}>
              <Checkbox
                label={t("sourceFormFieldsExtended.preferMaterialized")}
                checked={form.preferMaterialized}
                onChange={(e) => setForm({ ...form, preferMaterialized: e.currentTarget.checked })}
                data-testid="prefer-materialized-checkbox"
              />
            </Tooltip>
          </Group>
          <NumberInput
            label={t("sourceFormFieldsExtended.cacheTtlSeconds")}
            min={0}
            value={form.cacheTtl === "" ? "" : Number(form.cacheTtl)}
            onChange={(v) => setForm({ ...form, cacheTtl: v === "" ? "" : String(v) })}
            placeholder={t("sourceFormFieldsExtended.cacheTtlPlaceholder")}
            data-testid="cache-ttl-input"
          />
          <Tooltip label={t("sourceFormFieldsExtended.changeSignalTooltip")} multiline w={280}>
            <Select
              label={t("sourceFormFieldsExtended.changeSignal")}
              value={form.changeSignal}
              onChange={(v) => setForm({ ...form, changeSignal: v ?? "" })}
              data={sourceChangeSignals(form.type).map((cs) => ({
                value: cs,
                label: CHANGE_SIGNAL_LABELS[cs] ?? cs,
              }))}
              allowDeselect={false}
              data-testid="change-signal-select"
            />
          </Tooltip>
          {/* REQ-1141: source-level load protection (scheduled-refresh-only, off-peak window). */}
          <Group gap="lg" style={{ gridColumn: "1 / -1" }} wrap="wrap" align="flex-end">
            <Tooltip label={t("sourceFormFieldsExtended.loadProtectedTooltip")} multiline w={280}>
              <Checkbox
                label={t("sourceFormFieldsExtended.loadProtected")}
                checked={form.loadProtected}
                onChange={(e) => setForm({ ...form, loadProtected: e.currentTarget.checked })}
                data-testid="load-protected-checkbox"
              />
            </Tooltip>
            {form.loadProtected && (
              <>
                <TextInput
                  label={t("sourceFormFieldsExtended.offPeakWindow")}
                  value={form.offPeakWindow}
                  onChange={(e) => setForm({ ...form, offPeakWindow: e.currentTarget.value })}
                  placeholder="01:00-05:00"
                  data-testid="off-peak-window-input"
                />
                <TextInput
                  label={t("sourceFormFieldsExtended.offPeakTz")}
                  value={form.offPeakTz}
                  onChange={(e) => setForm({ ...form, offPeakTz: e.currentTarget.value })}
                  placeholder="UTC"
                  data-testid="off-peak-tz-input"
                />
              </>
            )}
          </Group>
          {domainsEnabled && (
            <Stack gap={4} style={{ gridColumn: "1 / -1" }}>
              <MultiSelect
                label={`${t("sourceFormFieldsExtended.allowedDomains")} ${t("sourceFormFieldsExtended.allowedDomainsHint")}`}
                options={domains.map((d) => ({ id: d.id, label: d.id }))}
                value={form.allowedDomains
                  .split(",")
                  .map((s) => s.trim())
                  .filter(Boolean)}
                onChange={(selected) => setForm({ ...form, allowedDomains: selected.join(", ") })}
              />
            </Stack>
          )}
          {cdcTransportApplicable(form.type) && (
            <Accordion
              variant="separated"
              style={{ gridColumn: "1 / -1" }}
              data-testid="cdc-transport-accordion"
            >
              <Accordion.Item value="cdc">
                <Accordion.Control>{t("sourceFormFieldsExtended.cdcTransportHeading")}</Accordion.Control>
                <Accordion.Panel>
                  <Stack gap="sm">
                    <Text size="sm" c="dimmed">
                      {t("sourceFormFieldsExtended.cdcTransportDescription")}
                    </Text>
                    <TextInput
                      label={t("sourceFormFieldsExtended.bootstrapServers")}
                      value={cdc.bootstrapServers}
                      onChange={(e) => setCdc({ ...cdc, bootstrapServers: e.target.value })}
                      placeholder="broker1:9092,broker2:9092"
                      data-testid="cdc-bootstrap-servers-input"
                    />
                    <TextInput
                      label={t("sourceFormFieldsExtended.topicPrefix")}
                      value={cdc.topicPrefix}
                      onChange={(e) => setCdc({ ...cdc, topicPrefix: e.target.value })}
                      placeholder={t("sourceFormFieldsExtended.topicPrefixPlaceholder")}
                      data-testid="cdc-topic-prefix-input"
                    />
                    <TextInput
                      label={t("sourceFormFieldsExtended.schemaRegistryUrl")}
                      value={cdc.schemaRegistryUrl}
                      onChange={(e) => setCdc({ ...cdc, schemaRegistryUrl: e.target.value })}
                      placeholder={t("sourceFormFieldsExtended.schemaRegistryUrlCdcPlaceholder")}
                      data-testid="cdc-schema-registry-input"
                    />
                    <TextInput
                      label={t("sourceFormFieldsExtended.consumerGroupId")}
                      value={cdc.consumerGroupId}
                      onChange={(e) => setCdc({ ...cdc, consumerGroupId: e.target.value })}
                      placeholder={t("sourceFormFieldsExtended.consumerGroupIdPlaceholder")}
                      data-testid="cdc-consumer-group-input"
                    />
                  </Stack>
                </Accordion.Panel>
              </Accordion.Item>
            </Accordion>
          )}
        </>
      )}
    </>
  );
}
