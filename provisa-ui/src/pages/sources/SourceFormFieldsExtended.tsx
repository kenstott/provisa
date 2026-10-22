// Copyright (c) 2026 Kenneth Stott
// Canary: 3327d3ae-6bcf-4a29-8985-a227ce882282
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { NEVER_EXPIRES, cacheTtlHint } from "../../components/form/cacheTtl";
import { useRegionChoices } from "../../hooks/useRegionQueries";
import { useTranslation } from "react-i18next";
import {
  Button,
  Checkbox,
  Group,
  NumberInput,
  PasswordInput,
  Select,
  Text,
  TextInput,
} from "@mantine/core";
import { MultiSelect } from "../../components/MultiSelect";
import { cdcTransportApplicable } from "../../liveCapability";
import { SchemaRegistryAuthFields } from "./SchemaRegistryAuthFields";
import {
  API_AUTH_TYPES,
  BRAND_CARRIER,
  BRAND_AUTH,
  FILE_TRANSPORTS,
  KAFKA_AUTH_TYPES,
} from "./constants";
import { namingConventionLabel, useNamingConventions } from "../../hooks/useNamingConventions";
import { AuthUserPass } from "./AuthUserPass";
import { OpenApiFormSection } from "./OpenApiFormSection";
import { KaggleFormSection } from "./KaggleFormSection";
import { PushFeedFormSection } from "./PushFeedFormSection";
import { ProfilerFormSection } from "./ProfilerFormSection";
import { SalesforceFields } from "./SalesforceFields";
import { ServiceNowFields } from "./ServiceNowFields";
import { JiraFields } from "./JiraFields";
import { CloudopsFields } from "./CloudopsFields";
import { GoogleWorkspaceFields } from "./GoogleWorkspaceFields";
import { Microsoft365Fields } from "./Microsoft365Fields";
import { CodeSourceFields } from "./CodeSourceFields";
import { CODE_SOURCE_TYPES } from "./codeSource";
import { CosmosDbFields } from "./CosmosDbFields";
import { MOTHERDUCK, MotherDuckFields } from "./MotherDuckFields";
import { COSMOSDB } from "./cosmosdb";
import { QdrantFields } from "./QdrantFields";
import { QDRANT } from "./qdrant";
import { WikipediaFields } from "./WikipediaFields";
import { LogsFields } from "./LogsFields";
import { LOGS } from "./logs";
import { SparqlFormSection } from "./SparqlFormSection";
import { SharepointFields } from "./SharepointFields";
import { SplunkFields } from "./SplunkFields";
import { SourceLoadManagementPanel } from "./SourceLoadManagementPanel";
import { RegionSelect } from "../../components/admin/RegionSelect";
import type { SourceFormFieldsProps } from "./SourceFormFields";
import { useCacheTtlRequired } from "./sourceKind";
import {
  FieldGroup,
  FormChecks,
  FormField,
  FormFold,
  FormFolds,
  FormSection,
} from "../../components/form/FormLayout";
import { assignableDomainIds } from "../../lib/internalDomains";

export function SourceFormFieldsExtended({
  form,
  setForm,
  authType,
  setAuthType,
  authFields,
  setAuthFields,
  editingSourceId,
  essential,
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
  splunkAuthMode,
  setSplunkAuthMode,
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
  govdataSubjectOptions,
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
  wsUseSsl,
  setWsUseSsl,
  wsSubscribePayload,
  setWsSubscribePayload,
  wsEventPath,
  setWsEventPath,
  wsReconnectInterval,
  setWsReconnectInterval,
  rssFeedUrl,
  setRssFeedUrl,
  rssPollInterval,
  setRssPollInterval,
  rssUseSsl,
  setRssUseSsl,
  onKaggleSourcesRegistered,
}: SourceFormFieldsProps) {
  // A kind whose tables land cannot be created without its Cache TTL (sourceKind).
  const landsWithTtl = useCacheTtlRequired(form.type);
  const { t } = useTranslation();
  const namingConventions = useNamingConventions();
  const regionChoices = useRegionChoices();
  // REQ-1739: was `getCategory(form.type) === "Streaming"`, which was correct only while kafka was
  // the sole Streaming member — websocket/rss/ingest now share the category and each needs its own
  // fields, so this must key on the type itself.
  const isKafka = form.type === "kafka";
  const simpleAuthData = [
    { value: "none", label: t("sourceFormFieldsExtended.authNone") },
    { value: "basic", label: t("sourceFormFieldsExtended.authBasic") },
    { value: "bearer", label: t("sourceFormFieldsExtended.authBearer") },
  ];
  return (
    <>
      {form.type === "prometheus" && (
        <>
          <FormField size="long">
            <TextInput
              required
              label={t("sourceFormFieldsExtended.url")}
              value={form.host}
              onChange={(e) => setForm({ ...form, host: e.target.value })}
              placeholder="http://prometheus:9090"
              data-testid="prometheus-url-input"
            />
          </FormField>
          <FormField section="authentication" size="medium">
            <Select
              label={t("sourceFormFieldsExtended.authentication")}
              value={authType}
              onChange={(v) => {
                setAuthType(v ?? "");
                setAuthFields({});
              }}
              data={simpleAuthData}
              allowDeselect={false}
              data-testid="prometheus-auth-type-select"
            />
          </FormField>
          {authType === "basic" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
          {authType === "bearer" && (
            <FormField section="authentication" size="long">
              <PasswordInput
                required
                label={t("sourceFormFieldsExtended.token")}
                value={authFields.token ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })}
                data-testid="prometheus-token-input"
              />
            </FormField>
          )}
        </>
      )}
      {form.type === "google_sheets" && (
        <>
          <FormField section="authentication" size="long">
            <TextInput
              required
              label={t("sourceFormFieldsExtended.credentialsJsonPath")}
              value={authFields.credentials_json ?? ""}
              onChange={(e) => setAuthFields({ ...authFields, credentials_json: e.target.value })}
              placeholder={t("sourceFormFieldsExtended.credentialsJsonPathPlaceholder")}
              data-testid="google-sheets-credentials-input"
            />
          </FormField>
          <FormField size="medium">
            <TextInput
              label={t("sourceFormFieldsExtended.metadataSheetId")}
              value={form.database}
              onChange={(e) => setForm({ ...form, database: e.target.value })}
              placeholder={t("sourceFormFieldsExtended.metadataSheetIdPlaceholder")}
              data-testid="google-sheets-sheet-id-input"
            />
          </FormField>
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
      {form.type === "kaggle" && (
        <KaggleFormSection
          sourceIdHint={form.id}
          onSourcesRegistered={onKaggleSourcesRegistered ?? (() => {})}
        />
      )}
      {/* REQ-1923: a brand asks for the credential it states (BRAND_AUTH), never one by default. */}
      {BRAND_AUTH[form.type] === "token" && (
        <FormField
          section="authentication"
          hint={t("sourceFormFieldsExtended.accessTokenHint")}
          size="long"
        >
          <PasswordInput
            required
            label={t("sourceFormFieldsExtended.accessToken")}
            value={authFields.token ?? ""}
            onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })}
            data-testid="brand-token-input"
          />
        </FormField>
      )}
      {/* A table name prefix is the remote GraphQL source's; an OpenAPI table is named for its
          operation. */}
      {BRAND_CARRIER[form.type] === "graphql" && (
        <FormField hint={t("sourceFormFieldsExtended.brandNamespaceHint")} size="medium">
          <TextInput
            label={t("sourceFormFieldsExtended.namespace")}
            value={gqlNamespace}
            onChange={(e) => setGqlNamespace(e.target.value)}
            data-testid="brand-namespace-input"
          />
        </FormField>
      )}
      {form.type === "graphql" && (
        <>
          <FormField size="long">
            <TextInput
              required
              label={t("sourceFormFieldsExtended.endpointUrl")}
              value={form.host}
              onChange={(e) => setForm({ ...form, host: e.target.value })}
              placeholder="https://api.example.com/graphql"
              data-testid="graphql-endpoint-input"
            />
          </FormField>
          <FormField size="medium">
            <TextInput
              label={t("sourceFormFieldsExtended.namespace")}
              value={gqlNamespace}
              onChange={(e) => setGqlNamespace(e.target.value)}
              placeholder="myapi"
              data-testid="graphql-namespace-input"
            />
          </FormField>
          <FormField hint={cacheTtlHint(gqlCacheTtl, t)} size="short">
            <NumberInput
              min={NEVER_EXPIRES}
              label={t("sourceFormFieldsExtended.cacheTtlSeconds")}
              value={gqlCacheTtl === "" ? "" : Number(gqlCacheTtl)}
              onChange={(v) => setGqlCacheTtl(v === "" ? "" : String(v))}
              placeholder="300"
              data-testid="graphql-cache-ttl-input"
            />
          </FormField>
          <FormField section="authentication" size="medium">
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
              data-testid="graphql-auth-type-select"
            />
          </FormField>
          {authType === "bearer" && (
            <FormField section="authentication" size="long">
              <PasswordInput
                required
                label={t("sourceFormFieldsExtended.token")}
                value={authFields.token ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })}
                placeholder="${env:GQL_TOKEN}"
                data-testid="graphql-token-input"
              />
            </FormField>
          )}
          {authType === "basic" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
        </>
      )}
      {form.type === "grpc" && (
        <>
          <FormField size="long">
            <TextInput
              required
              label={t("sourceFormFieldsExtended.protoPathOrUrl")}
              value={grpcProtoPath}
              onChange={(e) => setGrpcProtoPath(e.target.value)}
              placeholder="https://api.example.com/service.proto or ./service.proto"
              data-testid="grpc-proto-path-input"
            />
          </FormField>
          <FormField size="long">
            <TextInput
              required
              label={t("sourceFormFieldsExtended.serverAddress")}
              value={grpcServerAddress}
              onChange={(e) => setGrpcServerAddress(e.target.value)}
              placeholder="api.example.com:50051"
              data-testid="grpc-server-address-input"
            />
          </FormField>
          <FormField size="medium">
            <TextInput
              label={t("sourceFormFieldsExtended.namespace")}
              value={grpcNamespace}
              onChange={(e) => setGrpcNamespace(e.target.value)}
              placeholder="mygrpc"
              data-testid="grpc-namespace-input"
            />
          </FormField>
          <FormField hint={cacheTtlHint(grpcCacheTtl, t)} size="short">
            <NumberInput
              min={NEVER_EXPIRES}
              label={t("sourceFormFieldsExtended.cacheTtlSeconds")}
              value={grpcCacheTtl === "" ? "" : Number(grpcCacheTtl)}
              onChange={(v) => setGrpcCacheTtl(v === "" ? "" : String(v))}
              placeholder="300"
              data-testid="grpc-cache-ttl-input"
            />
          </FormField>
          <FormChecks>
            <FormField size="short">
              <Checkbox
                label={t("sourceFormFieldsExtended.tls")}
                checked={grpcTls}
                onChange={(e) => setGrpcTls(e.currentTarget.checked)}
                mt="1.5rem"
                data-testid="grpc-tls-checkbox"
              />
            </FormField>
          </FormChecks>
          <FormField size="long">
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
              data-testid="grpc-import-paths-input"
            />
          </FormField>
          <FormField section="authentication" size="medium">
            <Select
              label={t("sourceFormFieldsExtended.authentication")}
              value={authType}
              onChange={(v) => {
                setAuthType(v ?? "");
                setAuthFields({});
              }}
              data={API_AUTH_TYPES.map((a) => ({ value: a.value, label: a.label }))}
              allowDeselect={false}
              data-testid="grpc-auth-type-select"
            />
          </FormField>
          {authType === "bearer" && (
            <FormField section="authentication" size="long">
              <PasswordInput
                required
                label={t("sourceFormFieldsExtended.token")}
                value={authFields.token ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })}
                placeholder="${env:GRPC_TOKEN}"
                data-testid="grpc-token-input"
              />
            </FormField>
          )}
          {authType === "basic" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
          {authType === "api_key" && (
            <>
              <FormField size="medium">
                <TextInput
                  required
                  label={t("sourceFormFieldsExtended.headerName")}
                  value={authFields.header_name ?? "X-API-Key"}
                  onChange={(e) => setAuthFields({ ...authFields, header_name: e.target.value })}
                  placeholder="X-API-Key"
                  data-testid="grpc-header-name-input"
                />
              </FormField>
              <FormField section="authentication" size="long">
                <PasswordInput
                  required
                  label={t("sourceFormFieldsExtended.apiKey")}
                  value={authFields.api_key ?? ""}
                  onChange={(e) => setAuthFields({ ...authFields, api_key: e.target.value })}
                  placeholder="${env:API_KEY}"
                  data-testid="grpc-api-key-input"
                />
              </FormField>
            </>
          )}
        </>
      )}
      {form.type === "neo4j" && (
        <>
          <FieldGroup>
            <FormField size="medium">
              <TextInput
                required
                label={t("sourceFormFieldsExtended.host")}
                value={form.host}
                onChange={(e) => setForm({ ...form, host: e.target.value })}
                placeholder="localhost"
                data-testid="neo4j-host-input"
              />
            </FormField>
            <FormField size="short">
              <NumberInput
                required
                label={t("sourceFormFieldsExtended.port")}
                value={form.port}
                onChange={(v) => setForm({ ...form, port: Number(v) || 0 })}
                data-testid="neo4j-port-input"
              />
            </FormField>
          </FieldGroup>
          <FormField size="medium">
            <TextInput
              label={t("sourceFormFieldsExtended.database")}
              value={form.database}
              onChange={(e) => setForm({ ...form, database: e.target.value })}
              placeholder="neo4j"
              data-testid="neo4j-database-input"
            />
          </FormField>
          <FormChecks>
            <FormField size="short">
              <Checkbox
                label={t("sourceFormFieldsExtended.https")}
                checked={authFields.use_https === "true"}
                onChange={(e) =>
                  setAuthFields({
                    ...authFields,
                    use_https: e.currentTarget.checked ? "true" : "false",
                  })
                }
                mt="1.5rem"
                data-testid="neo4j-https-checkbox"
              />
            </FormField>
          </FormChecks>
          <FieldGroup>
            <FormField section="authentication" size="medium">
              <TextInput
                label={t("sourceFormFieldsExtended.username")}
                value={form.username}
                onChange={(e) => setForm({ ...form, username: e.target.value })}
                placeholder="neo4j"
                data-testid="neo4j-username-input"
              />
            </FormField>
            <FormField section="authentication" size="medium">
              <PasswordInput
                label={t("sourceFormFieldsExtended.password")}
                value={form.password}
                onChange={(e) => setForm({ ...form, password: e.target.value })}
                data-testid="neo4j-password-input"
              />
            </FormField>
          </FieldGroup>
        </>
      )}
      {form.type === "sparql" && (
        <SparqlFormSection
          form={form}
          setForm={setForm}
          authType={authType}
          setAuthType={setAuthType}
          authFields={authFields}
          setAuthFields={setAuthFields}
        />
      )}
      {form.type === "govdata" && (
        <>
          <FormField size="full">
            <Checkbox.Group
              label={t("sourceFormFieldsExtended.dataSubjects")}
              value={govdataSubjects}
              onChange={setGovdataSubjects}
            >
              <Group gap="sm" mt="0.25rem">
                {(govdataSubjectOptions ?? []).map((subj) => (
                  <Checkbox
                    key={subj.value}
                    value={subj.value}
                    label={subj.label}
                    data-testid={`govdata-subject-${subj.value}`}
                  />
                ))}
              </Group>
            </Checkbox.Group>
          </FormField>
          <FieldGroup>
            <FormField section="authentication" size="long">
              <PasswordInput
                label={t("sourceFormFieldsExtended.askAmericaApiKey")}
                value={form.username}
                onChange={(e) => setForm({ ...form, username: e.target.value })}
                placeholder="${ASKAMERICA_API_KEY}"
                data-testid="govdata-api-key-input"
              />
            </FormField>
            <Button
              type="button"
              variant="default"
              component="a"
              href="https://askamerica.ai"
              target="_blank"
              rel="noreferrer"
              style={{ alignSelf: "flex-end" }}
              data-testid="govdata-get-api-key-link"
            >
              {t("sourceFormFieldsExtended.getApiKey")}
            </Button>
          </FieldGroup>
          {submitting && !editingSourceId && (
            <FormField size="full">
              <Text c="dimmed" size="sm">
                {t("sourceFormFieldsExtended.validatingApiKey")}
              </Text>
            </FormField>
          )}
        </>
      )}
      {isKafka && (
        <>
          <FormField size="long">
            <TextInput
              required
              label={t("sourceFormFieldsExtended.bootstrapServers")}
              value={form.host}
              onChange={(e) => setForm({ ...form, host: e.target.value })}
              placeholder="kafka:9092"
              data-testid="kafka-bootstrap-servers-input"
            />
          </FormField>
          <FormField size="long">
            <TextInput
              label={t("sourceFormFieldsExtended.schemaRegistryUrl")}
              value={form.database}
              onChange={(e) => setForm({ ...form, database: e.target.value })}
              placeholder="http://schema-registry:8081"
              data-testid="kafka-schema-registry-input"
            />
          </FormField>
          <FormField section="authentication" size="medium">
            <Select
              label={t("sourceFormFieldsExtended.authentication")}
              value={authType}
              onChange={(v) => {
                setAuthType(v ?? "");
                setAuthFields({});
              }}
              data={KAFKA_AUTH_TYPES.map((a) => ({ value: a.value, label: a.label }))}
              allowDeselect={false}
              data-testid="kafka-auth-type-select"
            />
          </FormField>
          {authType !== "none" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
        </>
      )}
      {form.type === "data_profiler" && (
        <ProfilerFormSection authFields={authFields} setAuthFields={setAuthFields} />
      )}
      {(form.type === "websocket" || form.type === "rss") && (
        <PushFeedFormSection
          formType={form.type}
          wsUseSsl={wsUseSsl}
          setWsUseSsl={setWsUseSsl}
          wsSubscribePayload={wsSubscribePayload}
          setWsSubscribePayload={setWsSubscribePayload}
          wsEventPath={wsEventPath}
          setWsEventPath={setWsEventPath}
          wsReconnectInterval={wsReconnectInterval}
          setWsReconnectInterval={setWsReconnectInterval}
          rssFeedUrl={rssFeedUrl}
          setRssFeedUrl={setRssFeedUrl}
          rssPollInterval={rssPollInterval}
          setRssPollInterval={setRssPollInterval}
          rssUseSsl={rssUseSsl}
          setRssUseSsl={setRssUseSsl}
        />
      )}
      {form.type === "google_workspace" && ( // REQ-1923
        <GoogleWorkspaceFields sourceId={form.id} fields={authFields} setFields={setAuthFields} />
      )}
      {form.type === "microsoft_365" && ( // REQ-1923
        <Microsoft365Fields sourceId={form.id} fields={authFields} setFields={setAuthFields} />
      )}
      {CODE_SOURCE_TYPES.has(form.type) && ( // REQ-1984, REQ-1993
        <CodeSourceFields
          type={form.type}
          fields={authFields}
          setFields={setAuthFields}
          sourceId={editingSourceId ?? undefined}
        />
      )}
      {form.type === MOTHERDUCK && <MotherDuckFields form={form} setForm={setForm} />}
      {form.type === COSMOSDB && ( // REQ-1982
        <CosmosDbFields
          form={form}
          setForm={setForm}
          fields={authFields}
          setFields={setAuthFields}
        />
      )}
      {form.type === QDRANT && ( // REQ-1983
        <QdrantFields form={form} setForm={setForm} fields={authFields} setFields={setAuthFields} />
      )}
      {form.type === "wikipedia" && (
        <WikipediaFields fields={authFields} setFields={setAuthFields} /> // REQ-1960
      )}
      {form.type === LOGS && <LogsFields fields={authFields} setFields={setAuthFields} />}
      {form.type === "cloudops" && <CloudopsFields fields={authFields} setFields={setAuthFields} />}
      {form.type === "salesforce" && (
        <SalesforceFields
          form={form}
          setForm={setForm}
          fields={authFields}
          setFields={setAuthFields}
        />
      )}
      {form.type === "servicenow" && <ServiceNowFields form={form} setForm={setForm} />}
      {form.type === "jira" && <JiraFields form={form} setForm={setForm} />}
      {form.type === "sharepoint" && (
        <SharepointFields
          form={form}
          setForm={setForm}
          spAuthType={spAuthType}
          setSpAuthType={setSpAuthType}
          spCertPath={spCertPath}
          setSpCertPath={setSpCertPath}
          spCertPassword={spCertPassword}
          setSpCertPassword={setSpCertPassword}
          spUsername={spUsername}
          setSpUsername={setSpUsername}
          spPassword={spPassword}
          setSpPassword={setSpPassword}
        />
      )}
      {form.type === "splunk" && (
        <SplunkFields
          form={form}
          setForm={setForm}
          splunkDisableSsl={splunkDisableSsl}
          splunkAuthMode={splunkAuthMode}
          setSplunkAuthMode={setSplunkAuthMode}
          setSplunkDisableSsl={setSplunkDisableSsl}
        />
      )}
      {form.type === "files" &&
        (() => {
          const transportMeta = FILE_TRANSPORTS.find((t2) => t2.value === filesTransport)!;
          return (
            <>
              <FormField size="medium">
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
              </FormField>
              <FormField size="medium">
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
              </FormField>
              {transportMeta.needsAuth === "s3" && (
                <>
                  <FieldGroup>
                    <FormField section="authentication" size="medium">
                      <TextInput
                        label={t("sourceFormFieldsExtended.accessKeyId")}
                        value={form.username}
                        onChange={(e) => setForm({ ...form, username: e.target.value })}
                        data-testid="files-access-key-input"
                      />
                    </FormField>
                    <FormField section="authentication" size="medium">
                      <PasswordInput
                        label={t("sourceFormFieldsExtended.secretAccessKey")}
                        value={form.password}
                        onChange={(e) => setForm({ ...form, password: e.target.value })}
                        data-testid="files-secret-key-input"
                      />
                    </FormField>
                  </FieldGroup>
                </>
              )}
              {transportMeta.needsAuth === "userpass" && (
                <>
                  <FieldGroup>
                    <FormField section="authentication" size="medium">
                      <TextInput
                        label={t("sourceFormFieldsExtended.username")}
                        value={form.username}
                        onChange={(e) => setForm({ ...form, username: e.target.value })}
                        data-testid="files-username-input"
                      />
                    </FormField>
                    <FormField section="authentication" size="medium">
                      <PasswordInput
                        label={t("sourceFormFieldsExtended.password")}
                        value={form.password}
                        onChange={(e) => setForm({ ...form, password: e.target.value })}
                        data-testid="files-password-input"
                      />
                    </FormField>
                  </FieldGroup>
                </>
              )}
              {transportMeta.needsAuth === "cert-or-userpass" && (
                <>
                  <FormField section="authentication" size="medium">
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
                        {
                          value: "userpass",
                          label: t("sourceFormFieldsExtended.usernamePassword"),
                        },
                        {
                          value: "certificate",
                          label: t("sourceFormFieldsExtended.certificatePfx"),
                        },
                      ]}
                      allowDeselect={false}
                      data-testid="files-auth-method-select"
                    />
                  </FormField>
                  {filesAuthMode === "userpass" && (
                    <>
                      <FieldGroup>
                        <FormField section="authentication" size="medium">
                          <TextInput
                            label={t("sourceFormFieldsExtended.username")}
                            value={form.username}
                            onChange={(e) => setForm({ ...form, username: e.target.value })}
                            data-testid="files-userpass-username-input"
                          />
                        </FormField>
                        <FormField section="authentication" size="medium">
                          <PasswordInput
                            label={t("sourceFormFieldsExtended.password")}
                            value={form.password}
                            onChange={(e) => setForm({ ...form, password: e.target.value })}
                            data-testid="files-userpass-password-input"
                          />
                        </FormField>
                      </FieldGroup>
                    </>
                  )}
                  {filesAuthMode === "certificate" && (
                    <>
                      <FormField section="authentication" size="long">
                        <TextInput
                          label={t("sourceFormFieldsExtended.pfxCertificatePath")}
                          value={filesCertPath}
                          onChange={(e) => setFilesCertPath(e.target.value)}
                          placeholder="/path/to/cert.pfx"
                          data-testid="files-cert-path-input"
                        />
                      </FormField>
                      <FormField section="authentication" size="medium">
                        <PasswordInput
                          label={t("sourceFormFieldsExtended.certificatePassword")}
                          value={filesCertPassword}
                          onChange={(e) => setFilesCertPassword(e.target.value)}
                          data-testid="files-cert-password-input"
                        />
                      </FormField>
                    </>
                  )}
                </>
              )}
            </>
          );
        })()}
      {/* REQ-1907: the load and timeliness controls are offered at create as well as edit — a
          source whose tables land needs its cache TTL before a table is registered. */}
      {/* The short step asks for them only where the source cannot exist without one: a kind
          whose tables land must state its Cache TTL. */}
      {(!essential || landsWithTtl) && (
        <FormSection id="load">
          <SourceLoadManagementPanel form={form} setForm={setForm} />
        </FormSection>
      )}
      {!essential && (
        <FormField size="medium" section="load">
          <RegionSelect
            value={form.region}
            onChange={(region) => setForm({ ...form, region })}
            regions={regionChoices.regions}
            scope="source"
            testId="source-region-select"
          />
        </FormField>
      )}
      {editingSourceId && (
        <>
          <FormField size="medium">
            <Select
              label={t("sourceFormFieldsExtended.namingConvention")}
              value={form.gqlNamingConvention}
              onChange={(v) => setForm({ ...form, gqlNamingConvention: v ?? "" })}
              // REQ-416: the server's conventions; none of its own is the deployment's.
              data={[
                { value: "", label: t("sourceFormFieldsExtended.namingInherit") },
                ...namingConventions.map((c) => ({ value: c, label: namingConventionLabel(t, c) })),
              ]}
              allowDeselect={false}
              data-testid="naming-convention-select"
            />
          </FormField>
          {domainsEnabled && (
            <FormField size="long" hint={t("sourceFormFieldsExtended.allowedDomainsHint")}>
              <MultiSelect
                label={t("sourceFormFieldsExtended.allowedDomains")}
                options={assignableDomainIds(domains.map((d) => d.id)).map((id) => ({
                  id,
                  label: id,
                }))}
                value={form.allowedDomains
                  .split(",")
                  .map((s) => s.trim())
                  .filter(Boolean)}
                onChange={(selected) => setForm({ ...form, allowedDomains: selected.join(", ") })}
              />
            </FormField>
          )}
          {cdcTransportApplicable(form.type) && (
            <FormField size="full" section="load">
              <FormFolds id="cdc-transport-accordion">
                <FormFold
                  value="cdc"
                  title={t("sourceFormFieldsExtended.cdcTransportHeading")}
                  note={t("sourceFormFieldsExtended.cdcTransportDescription")}
                >
                  <FormField size="long">
                    <TextInput
                      label={t("sourceFormFieldsExtended.bootstrapServers")}
                      value={cdc.bootstrapServers}
                      onChange={(e) => setCdc({ ...cdc, bootstrapServers: e.target.value })}
                      placeholder="broker1:9092,broker2:9092"
                      data-testid="cdc-bootstrap-servers-input"
                    />
                  </FormField>
                  <FormField size="medium">
                    <TextInput
                      label={t("sourceFormFieldsExtended.topicPrefix")}
                      value={cdc.topicPrefix}
                      onChange={(e) => setCdc({ ...cdc, topicPrefix: e.target.value })}
                      placeholder={t("sourceFormFieldsExtended.topicPrefixPlaceholder")}
                      data-testid="cdc-topic-prefix-input"
                    />
                  </FormField>
                  <FormField size="long">
                    <TextInput
                      label={t("sourceFormFieldsExtended.schemaRegistryUrl")}
                      value={cdc.schemaRegistryUrl}
                      onChange={(e) => setCdc({ ...cdc, schemaRegistryUrl: e.target.value })}
                      placeholder={t("sourceFormFieldsExtended.schemaRegistryUrlCdcPlaceholder")}
                      data-testid="cdc-schema-registry-input"
                    />
                  </FormField>
                  {cdc.schemaRegistryUrl && <SchemaRegistryAuthFields cdc={cdc} setCdc={setCdc} />}
                  <FormField size="medium">
                    <TextInput
                      label={t("sourceFormFieldsExtended.consumerGroupId")}
                      value={cdc.consumerGroupId}
                      onChange={(e) => setCdc({ ...cdc, consumerGroupId: e.target.value })}
                      placeholder={t("sourceFormFieldsExtended.consumerGroupIdPlaceholder")}
                      data-testid="cdc-consumer-group-input"
                    />
                  </FormField>
                </FormFold>
              </FormFolds>
            </FormField>
          )}
        </>
      )}
    </>
  );
}
