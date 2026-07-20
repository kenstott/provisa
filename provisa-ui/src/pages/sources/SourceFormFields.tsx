// Copyright (c) 2026 Kenneth Stott
// Canary: 55ecea78-4131-4bb4-8a93-71e1bdacea84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { NumberInput, PasswordInput, Select, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { Domain } from "../../types/admin";
import {
  DATA_LAKE,
  FILE_SOURCES,
  SIMPLE_RDBMS,
} from "./constants";
import { AuthUserPass } from "./AuthUserPass";
import { SourceFormFieldsExtended } from "./SourceFormFieldsExtended";

export interface SourceFormState {
  id: string;
  type: string;
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
  gqlNamingConvention: string;
  cacheTtl: string;
  cacheEnabled: boolean;
  preferMaterialized: boolean;
  loadProtected: boolean; // REQ-1141: scheduled-refresh-only load protection (source default)
  offPeakWindow: string; // REQ-1141: "HH:MM-HH:MM" maintenance window ("" = none)
  offPeakTz: string; // REQ-1141: IANA zone for the window
  changeSignal: string;
  path: string;
  allowedDomains: string;
  description: string;
}

export interface CdcState {
  bootstrapServers: string;
  topicPrefix: string;
  schemaRegistryUrl: string;
  consumerGroupId: string;
}

export interface SourceFormFieldsProps {
  form: SourceFormState;
  setForm: (f: SourceFormState) => void;
  authType: string;
  setAuthType: (v: string) => void;
  authFields: Record<string, string>;
  setAuthFields: (f: Record<string, string>) => void;
  editingSourceId: string | null;
  cdc: CdcState;
  setCdc: (v: CdcState) => void;
  domainsEnabled: boolean;
  domains: Domain[];
  spAuthType: string;
  setSpAuthType: (v: string) => void;
  spCertPath: string;
  setSpCertPath: (v: string) => void;
  spCertPassword: string;
  setSpCertPassword: (v: string) => void;
  spUsername: string;
  setSpUsername: (v: string) => void;
  spPassword: string;
  setSpPassword: (v: string) => void;
  splunkDisableSsl: boolean;
  setSplunkDisableSsl: (v: boolean) => void;
  filesTransport: string;
  setFilesTransport: (v: string) => void;
  filesAuthMode: "userpass" | "certificate";
  setFilesAuthMode: (v: "userpass" | "certificate") => void;
  filesCertPath: string;
  setFilesCertPath: (v: string) => void;
  filesCertPassword: string;
  setFilesCertPassword: (v: string) => void;
  govdataSubjects: string[];
  setGovdataSubjects: (v: string[]) => void;
  submitting: boolean;
  openapiSpecPath: string;
  setOpenapiSpecPath: (v: string) => void;
  openapiSpecInline: string;
  setOpenapiSpecInline: (v: string) => void;
  openapiSpecMode: "path" | "inline";
  setOpenapiSpecMode: (v: "path" | "inline") => void;
  openapiBaseUrl: string;
  setOpenapiBaseUrl: (v: string) => void;
  openapiCacheTtl: string;
  setOpenapiCacheTtl: (v: string) => void;
  openapiPreview: {
    queries: { operation_id: string }[];
    mutations: { operation_id: string }[];
    spec_description?: string;
  } | null;
  openapiPreviewing: boolean;
  openapiPreviewError: string | null;
  onOpenapiPreview: () => void;
  gqlNamespace: string;
  setGqlNamespace: (v: string) => void;
  gqlCacheTtl: string;
  setGqlCacheTtl: (v: string) => void;
  grpcProtoPath: string;
  setGrpcProtoPath: (v: string) => void;
  grpcServerAddress: string;
  setGrpcServerAddress: (v: string) => void;
  grpcNamespace: string;
  setGrpcNamespace: (v: string) => void;
  grpcTls: boolean;
  setGrpcTls: (v: boolean) => void;
  grpcImportPaths: string;
  setGrpcImportPaths: (v: string) => void;
  grpcCacheTtl: string;
  setGrpcCacheTtl: (v: string) => void;
}

export function SourceFormFields(props: SourceFormFieldsProps) {
  const { form, setForm, authType, setAuthType, authFields, setAuthFields } = props;
  const { t } = useTranslation();
  const isFile = FILE_SOURCES.has(form.type);
  const isSimpleRdbms = SIMPLE_RDBMS.has(form.type);
  const isDataLake = DATA_LAKE.has(form.type);

  return (
    <>
      <TextInput
        style={{ gridColumn: "1 / -1" }}
        label={t("sourceFormFields.description")}
        value={form.description}
        onChange={(e) => setForm({ ...form, description: e.currentTarget.value })}
        placeholder={t("sourceFormFields.descriptionPlaceholder")}
      />
      {isSimpleRdbms && (
        <>
          <TextInput
            label={t("sourceFormFields.host")}
            required
            value={form.host}
            onChange={(e) => setForm({ ...form, host: e.currentTarget.value })}
            placeholder="localhost"
          />
          <NumberInput
            label={t("sourceFormFields.port")}
            required
            value={form.port}
            onChange={(v) => setForm({ ...form, port: typeof v === "number" ? v : 0 })}
            hideControls
          />
          <div style={{ display: "flex", gap: "0.75rem" }}>
            <TextInput
              style={{ flex: 1 }}
              label={t("sourceFormFields.username")}
              value={form.username}
              onChange={(e) => setForm({ ...form, username: e.currentTarget.value })}
            />
            <PasswordInput
              style={{ flex: 1 }}
              label={t("sourceFormFields.password")}
              value={form.password}
              onChange={(e) => setForm({ ...form, password: e.currentTarget.value })}
            />
          </div>
          <TextInput
            label={t("sourceFormFields.database")}
            required
            value={form.database}
            onChange={(e) => setForm({ ...form, database: e.currentTarget.value })}
          />
        </>
      )}
      {form.type === "duckdb" && (
        <TextInput
          style={{ gridColumn: "1 / -1" }}
          label={t("sourceFormFields.filePath")}
          required
          value={form.database}
          onChange={(e) => setForm({ ...form, database: e.currentTarget.value })}
          placeholder={t("sourceFormFields.filePathPlaceholder")}
        />
      )}
      {isFile && (
        <TextInput
          style={{ gridColumn: "1 / -1" }}
          label={
            form.type === "sqlite"
              ? t("sourceFormFields.sqliteFilePath")
              : form.type === "csv"
                ? t("sourceFormFields.csvFilePath")
                : t("sourceFormFields.parquetFilePath")
          }
          required
          value={form.path}
          onChange={(e) => setForm({ ...form, path: e.currentTarget.value })}
          placeholder={
            form.type === "sqlite"
              ? "./demo/files/orders.sqlite"
              : form.type === "csv"
                ? "./demo/files/customers.csv"
                : "./demo/files/products.parquet"
          }
        />
      )}
      {form.type === "snowflake" && (
        <>
          <TextInput
            label={t("sourceFormFields.accountUrl")}
            required
            value={form.host}
            onChange={(e) => setForm({ ...form, host: e.currentTarget.value })}
            placeholder="org-account.snowflakecomputing.com"
          />
          <TextInput
            label={t("sourceFormFields.warehouseDatabase")}
            required
            value={form.database}
            onChange={(e) => setForm({ ...form, database: e.currentTarget.value })}
            placeholder="COMPUTE_WH/MY_DB"
          />
          <Select
            style={{ gridColumn: "1 / -1" }}
            label={t("sourceFormFields.authentication")}
            data={[
              { value: "password", label: t("sourceFormFields.usernamePassword") },
              { value: "key_pair", label: t("sourceFormFields.keyPair") },
              { value: "oauth", label: t("sourceFormFields.oauthToken") },
            ]}
            value={authType}
            onChange={(v) => {
              setAuthType(v ?? "");
              setAuthFields({});
            }}
            allowDeselect={false}
          />
          {authType === "password" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
          {authType === "key_pair" && (
            <>
              <TextInput
                label={t("sourceFormFields.username")}
                required
                value={authFields.username ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, username: e.currentTarget.value })}
              />
              <TextInput
                label={t("sourceFormFields.privateKeyPath")}
                required
                value={authFields.private_key_path ?? ""}
                onChange={(e) =>
                  setAuthFields({ ...authFields, private_key_path: e.currentTarget.value })
                }
                placeholder="/path/to/rsa_key.p8"
              />
              <PasswordInput
                label={t("sourceFormFields.passphrase")}
                value={authFields.passphrase ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, passphrase: e.currentTarget.value })}
                placeholder="optional"
              />
            </>
          )}
          {authType === "oauth" && (
            <TextInput
              style={{ gridColumn: "1 / -1" }}
              label={t("sourceFormFields.token")}
              required
              value={authFields.token ?? ""}
              onChange={(e) => setAuthFields({ ...authFields, token: e.currentTarget.value })}
              placeholder="${env:SNOWFLAKE_TOKEN}"
            />
          )}
        </>
      )}
      {form.type === "bigquery" && (
        <>
          <TextInput
            style={{ gridColumn: "1 / -1" }}
            label={t("sourceFormFields.projectId")}
            required
            value={form.database}
            onChange={(e) => setForm({ ...form, database: e.currentTarget.value })}
            placeholder="my-gcp-project"
          />
          <Select
            style={{ gridColumn: "1 / -1" }}
            label={t("sourceFormFields.authentication")}
            data={[
              { value: "service_account", label: t("sourceFormFields.serviceAccountKey") },
              {
                value: "application_default",
                label: t("sourceFormFields.applicationDefaultCredentials"),
              },
            ]}
            value={authType}
            onChange={(v) => {
              setAuthType(v ?? "");
              setAuthFields({});
            }}
            allowDeselect={false}
          />
          {authType === "service_account" && (
            <TextInput
              style={{ gridColumn: "1 / -1" }}
              label={t("sourceFormFields.credentialsJsonPath")}
              required
              value={authFields.credentials_json ?? ""}
              onChange={(e) =>
                setAuthFields({ ...authFields, credentials_json: e.currentTarget.value })
              }
              placeholder="/path/to/service-account.json"
            />
          )}
        </>
      )}
      {form.type === "databricks" && (
        <>
          <TextInput
            label={t("sourceFormFields.workspaceUrl")}
            required
            value={form.host}
            onChange={(e) => setForm({ ...form, host: e.currentTarget.value })}
            placeholder="https://dbc-xxxxx.cloud.databricks.com"
          />
          <TextInput
            label={t("sourceFormFields.catalog")}
            required
            value={form.database}
            onChange={(e) => setForm({ ...form, database: e.currentTarget.value })}
            placeholder="main"
          />
          <Select
            style={{ gridColumn: "1 / -1" }}
            label={t("sourceFormFields.authentication")}
            data={[
              { value: "token", label: t("sourceFormFields.personalAccessToken") },
              { value: "oauth", label: t("sourceFormFields.oauth2M2M") },
            ]}
            value={authType}
            onChange={(v) => {
              setAuthType(v ?? "");
              setAuthFields({});
            }}
            allowDeselect={false}
          />
          {authType === "token" && (
            <TextInput
              style={{ gridColumn: "1 / -1" }}
              label={t("sourceFormFields.accessToken")}
              required
              value={authFields.access_token ?? ""}
              onChange={(e) => setAuthFields({ ...authFields, access_token: e.currentTarget.value })}
              placeholder="${env:DATABRICKS_TOKEN}"
            />
          )}
          {authType === "oauth" && (
            <>
              <TextInput
                label={t("sourceFormFields.clientId")}
                required
                value={authFields.client_id ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, client_id: e.currentTarget.value })}
              />
              <PasswordInput
                label={t("sourceFormFields.clientSecret")}
                required
                value={authFields.client_secret ?? ""}
                onChange={(e) =>
                  setAuthFields({ ...authFields, client_secret: e.currentTarget.value })
                }
              />
              <TextInput
                style={{ gridColumn: "1 / -1" }}
                label={t("sourceFormFields.tokenUrl")}
                required
                value={authFields.token_url ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, token_url: e.currentTarget.value })}
              />
            </>
          )}
        </>
      )}
      {form.type === "redshift" && (
        <>
          <TextInput
            label={t("sourceFormFields.host")}
            required
            value={form.host}
            onChange={(e) => setForm({ ...form, host: e.currentTarget.value })}
            placeholder="cluster.xxxxx.region.redshift.amazonaws.com"
          />
          <NumberInput
            label={t("sourceFormFields.port")}
            required
            value={form.port}
            onChange={(v) => setForm({ ...form, port: typeof v === "number" ? v : 0 })}
            hideControls
          />
          <TextInput
            label={t("sourceFormFields.database")}
            required
            value={form.database}
            onChange={(e) => setForm({ ...form, database: e.currentTarget.value })}
            placeholder="dev"
          />
          <Select
            style={{ gridColumn: "1 / -1" }}
            label={t("sourceFormFields.authentication")}
            data={[
              { value: "password", label: t("sourceFormFields.usernamePassword") },
              { value: "iam", label: t("sourceFormFields.iamCredentials") },
            ]}
            value={authType}
            onChange={(v) => {
              setAuthType(v ?? "");
              setAuthFields({});
            }}
            allowDeselect={false}
          />
          {authType === "password" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
          {authType === "iam" && (
            <>
              <TextInput
                label={t("sourceFormFields.accessKeyId")}
                required
                value={authFields.access_key_id ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, access_key_id: e.currentTarget.value })}
                placeholder="${env:AWS_ACCESS_KEY_ID}"
              />
              <PasswordInput
                label={t("sourceFormFields.secretAccessKey")}
                required
                value={authFields.secret_access_key ?? ""}
                onChange={(e) =>
                  setAuthFields({ ...authFields, secret_access_key: e.currentTarget.value })
                }
                placeholder="${env:AWS_SECRET_ACCESS_KEY}"
              />
              <TextInput
                label={t("sourceFormFields.region")}
                value={authFields.region ?? "us-east-1"}
                onChange={(e) => setAuthFields({ ...authFields, region: e.currentTarget.value })}
              />
            </>
          )}
        </>
      )}
      {form.type === "elasticsearch" && (
        <>
          <TextInput
            label={t("sourceFormFields.host")}
            required
            value={form.host}
            onChange={(e) => setForm({ ...form, host: e.currentTarget.value })}
            placeholder="https://localhost:9200"
          />
          <NumberInput
            label={t("sourceFormFields.port")}
            required
            value={form.port}
            onChange={(v) => setForm({ ...form, port: typeof v === "number" ? v : 0 })}
            hideControls
          />
          <Select
            style={{ gridColumn: "1 / -1" }}
            label={t("sourceFormFields.authentication")}
            data={[
              { value: "none", label: t("sourceFormFields.noAuth") },
              { value: "basic", label: t("sourceFormFields.basicAuth") },
              { value: "api_key", label: t("sourceFormFields.apiKey") },
              { value: "bearer", label: t("sourceFormFields.bearerToken") },
            ]}
            value={authType}
            onChange={(v) => {
              setAuthType(v ?? "");
              setAuthFields({});
            }}
            allowDeselect={false}
          />
          {authType === "basic" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
          {authType === "api_key" && (
            <TextInput
              style={{ gridColumn: "1 / -1" }}
              label={t("sourceFormFields.apiKeyBase64")}
              required
              value={authFields.api_key ?? ""}
              onChange={(e) => setAuthFields({ ...authFields, api_key: e.currentTarget.value })}
              placeholder="${env:ES_API_KEY}"
            />
          )}
          {authType === "bearer" && (
            <TextInput
              style={{ gridColumn: "1 / -1" }}
              label={t("sourceFormFields.token")}
              required
              value={authFields.token ?? ""}
              onChange={(e) => setAuthFields({ ...authFields, token: e.currentTarget.value })}
            />
          )}
        </>
      )}
      {isDataLake && (
        <>
          {form.type === "hive" && (
            <>
              <TextInput
                label={t("sourceFormFields.metastoreUri")}
                required
                value={form.host}
                onChange={(e) => setForm({ ...form, host: e.currentTarget.value })}
                placeholder="thrift://hive-metastore:9083"
              />
              <TextInput
                label={t("sourceFormFields.warehousePath")}
                required
                value={form.database}
                onChange={(e) => setForm({ ...form, database: e.currentTarget.value })}
                placeholder="s3://bucket/warehouse"
              />
            </>
          )}
          {(form.type === "delta_lake" || form.type === "iceberg") && (
            <>
              <TextInput
                label={t("sourceFormFields.metastoreUri")}
                value={form.host}
                onChange={(e) => setForm({ ...form, host: e.currentTarget.value })}
                placeholder="thrift://hive-metastore:9083 (optional)"
              />
              <TextInput
                label={t("sourceFormFields.warehousePath")}
                required
                value={form.database}
                onChange={(e) => setForm({ ...form, database: e.currentTarget.value })}
                placeholder="s3://bucket/warehouse"
              />
            </>
          )}
          <Select
            style={{ gridColumn: "1 / -1" }}
            label={t("sourceFormFields.storageAuthentication")}
            data={[
              { value: "none", label: t("sourceFormFields.noneInstanceRoleLocal") },
              { value: "aws", label: t("sourceFormFields.awsS3AccessKey") },
              { value: "azure", label: t("sourceFormFields.azureAdls") },
              { value: "gcs", label: t("sourceFormFields.googleCloudStorage") },
            ]}
            value={authType}
            onChange={(v) => {
              setAuthType(v ?? "");
              setAuthFields({});
            }}
            allowDeselect={false}
          />
          {authType === "aws" && (
            <>
              <TextInput
                label={t("sourceFormFields.accessKeyId")}
                required
                value={authFields.access_key_id ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, access_key_id: e.currentTarget.value })}
                placeholder="${env:AWS_ACCESS_KEY_ID}"
              />
              <PasswordInput
                label={t("sourceFormFields.secretAccessKey")}
                required
                value={authFields.secret_access_key ?? ""}
                onChange={(e) =>
                  setAuthFields({ ...authFields, secret_access_key: e.currentTarget.value })
                }
                placeholder="${env:AWS_SECRET_ACCESS_KEY}"
              />
              <TextInput
                label={t("sourceFormFields.region")}
                value={authFields.region ?? "us-east-1"}
                onChange={(e) => setAuthFields({ ...authFields, region: e.currentTarget.value })}
              />
              <TextInput
                label={t("sourceFormFields.s3EndpointMinio")}
                value={authFields.endpoint ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, endpoint: e.currentTarget.value })}
                placeholder="optional — for S3-compatible"
              />
            </>
          )}
          {authType === "azure" && (
            <>
              <TextInput
                label={t("sourceFormFields.storageAccount")}
                required
                value={authFields.storage_account ?? ""}
                onChange={(e) =>
                  setAuthFields({ ...authFields, storage_account: e.currentTarget.value })
                }
              />
              <PasswordInput
                label={t("sourceFormFields.accessKey")}
                value={authFields.access_key ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, access_key: e.currentTarget.value })}
                placeholder="shared key (or use SAS)"
              />
              <TextInput
                label={t("sourceFormFields.sasToken")}
                value={authFields.sas_token ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, sas_token: e.currentTarget.value })}
                placeholder="alternative to access key"
              />
            </>
          )}
          {authType === "gcs" && (
            <TextInput
              style={{ gridColumn: "1 / -1" }}
              label={t("sourceFormFields.credentialsJsonPath")}
              required
              value={authFields.credentials_json ?? ""}
              onChange={(e) =>
                setAuthFields({ ...authFields, credentials_json: e.currentTarget.value })
              }
              placeholder="/path/to/service-account.json"
            />
          )}
        </>
      )}
      <SourceFormFieldsExtended {...props} />
    </>
  );
}
