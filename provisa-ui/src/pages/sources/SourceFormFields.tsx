// Copyright (c) 2026 Kenneth Stott
// Canary: 55ecea78-4131-4bb4-8a93-71e1bdacea84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

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
  const isFile = FILE_SOURCES.has(form.type);
  const isSimpleRdbms = SIMPLE_RDBMS.has(form.type);
  const isDataLake = DATA_LAKE.has(form.type);

  return (
    <>
      <label style={{ gridColumn: "1 / -1" }}>
        Description
        <input
          value={form.description}
          onChange={(e) => setForm({ ...form, description: e.target.value })}
          placeholder="Brief description of this data source"
        />
      </label>
      {isSimpleRdbms && (
        <>
          <label>
            Host{" "}
            <input
              required
              value={form.host}
              onChange={(e) => setForm({ ...form, host: e.target.value })}
              placeholder="localhost"
            />
          </label>
          <label>
            Port{" "}
            <input
              type="number"
              required
              value={form.port}
              onChange={(e) => setForm({ ...form, port: +e.target.value })}
            />
          </label>
          <label>
            Database{" "}
            <input
              required
              value={form.database}
              onChange={(e) => setForm({ ...form, database: e.target.value })}
            />
          </label>
          <label>
            Username{" "}
            <input
              value={form.username}
              onChange={(e) => setForm({ ...form, username: e.target.value })}
            />
          </label>
          <label>
            Password{" "}
            <input
              type="password"
              value={form.password}
              onChange={(e) => setForm({ ...form, password: e.target.value })}
            />
          </label>
        </>
      )}
      {form.type === "duckdb" && (
        <label style={{ gridColumn: "1 / -1" }}>
          File Path{" "}
          <input
            required
            value={form.database}
            onChange={(e) => setForm({ ...form, database: e.target.value })}
            placeholder="/path/to/db.duckdb"
          />
        </label>
      )}
      {isFile && (
        <label style={{ gridColumn: "1 / -1" }}>
          {form.type === "sqlite"
            ? "SQLite File Path"
            : form.type === "csv"
              ? "CSV File Path or URL"
              : "Parquet File Path or URL"}
          <input
            required
            value={form.path}
            onChange={(e) => setForm({ ...form, path: e.target.value })}
            placeholder={
              form.type === "sqlite"
                ? "./demo/files/orders.sqlite"
                : form.type === "csv"
                  ? "./demo/files/customers.csv"
                  : "./demo/files/products.parquet"
            }
          />
        </label>
      )}
      {form.type === "snowflake" && (
        <>
          <label>
            Account URL{" "}
            <input
              required
              value={form.host}
              onChange={(e) => setForm({ ...form, host: e.target.value })}
              placeholder="org-account.snowflakecomputing.com"
            />
          </label>
          <label>
            Warehouse / Database{" "}
            <input
              required
              value={form.database}
              onChange={(e) => setForm({ ...form, database: e.target.value })}
              placeholder="COMPUTE_WH/MY_DB"
            />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            Authentication
            <select
              value={authType}
              onChange={(e) => {
                setAuthType(e.target.value);
                setAuthFields({});
              }}
            >
              <option value="password">Username / Password</option>
              <option value="key_pair">Key Pair</option>
              <option value="oauth">OAuth Token</option>
            </select>
          </label>
          {authType === "password" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
          {authType === "key_pair" && (
            <>
              <label>
                Username{" "}
                <input
                  required
                  value={authFields.username ?? ""}
                  onChange={(e) => setAuthFields({ ...authFields, username: e.target.value })}
                />
              </label>
              <label>
                Private Key Path{" "}
                <input
                  required
                  value={authFields.private_key_path ?? ""}
                  onChange={(e) =>
                    setAuthFields({ ...authFields, private_key_path: e.target.value })
                  }
                  placeholder="/path/to/rsa_key.p8"
                />
              </label>
              <label>
                Passphrase{" "}
                <input
                  type="password"
                  value={authFields.passphrase ?? ""}
                  onChange={(e) => setAuthFields({ ...authFields, passphrase: e.target.value })}
                  placeholder="optional"
                />
              </label>
            </>
          )}
          {authType === "oauth" && (
            <label style={{ gridColumn: "1 / -1" }}>
              Token{" "}
              <input
                required
                value={authFields.token ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })}
                placeholder="${env:SNOWFLAKE_TOKEN}"
              />
            </label>
          )}
        </>
      )}
      {form.type === "bigquery" && (
        <>
          <label style={{ gridColumn: "1 / -1" }}>
            Project ID{" "}
            <input
              required
              value={form.database}
              onChange={(e) => setForm({ ...form, database: e.target.value })}
              placeholder="my-gcp-project"
            />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            Authentication
            <select
              value={authType}
              onChange={(e) => {
                setAuthType(e.target.value);
                setAuthFields({});
              }}
            >
              <option value="service_account">Service Account Key</option>
              <option value="application_default">Application Default Credentials</option>
            </select>
          </label>
          {authType === "service_account" && (
            <label style={{ gridColumn: "1 / -1" }}>
              Credentials JSON Path{" "}
              <input
                required
                value={authFields.credentials_json ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, credentials_json: e.target.value })}
                placeholder="/path/to/service-account.json"
              />
            </label>
          )}
        </>
      )}
      {form.type === "databricks" && (
        <>
          <label>
            Workspace URL{" "}
            <input
              required
              value={form.host}
              onChange={(e) => setForm({ ...form, host: e.target.value })}
              placeholder="https://dbc-xxxxx.cloud.databricks.com"
            />
          </label>
          <label>
            Catalog{" "}
            <input
              required
              value={form.database}
              onChange={(e) => setForm({ ...form, database: e.target.value })}
              placeholder="main"
            />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            Authentication
            <select
              value={authType}
              onChange={(e) => {
                setAuthType(e.target.value);
                setAuthFields({});
              }}
            >
              <option value="token">Personal Access Token</option>
              <option value="oauth">OAuth2 (M2M)</option>
            </select>
          </label>
          {authType === "token" && (
            <label style={{ gridColumn: "1 / -1" }}>
              Access Token{" "}
              <input
                required
                value={authFields.access_token ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, access_token: e.target.value })}
                placeholder="${env:DATABRICKS_TOKEN}"
              />
            </label>
          )}
          {authType === "oauth" && (
            <>
              <label>
                Client ID{" "}
                <input
                  required
                  value={authFields.client_id ?? ""}
                  onChange={(e) => setAuthFields({ ...authFields, client_id: e.target.value })}
                />
              </label>
              <label>
                Client Secret{" "}
                <input
                  type="password"
                  required
                  value={authFields.client_secret ?? ""}
                  onChange={(e) => setAuthFields({ ...authFields, client_secret: e.target.value })}
                />
              </label>
              <label style={{ gridColumn: "1 / -1" }}>
                Token URL{" "}
                <input
                  required
                  value={authFields.token_url ?? ""}
                  onChange={(e) => setAuthFields({ ...authFields, token_url: e.target.value })}
                />
              </label>
            </>
          )}
        </>
      )}
      {form.type === "redshift" && (
        <>
          <label>
            Host{" "}
            <input
              required
              value={form.host}
              onChange={(e) => setForm({ ...form, host: e.target.value })}
              placeholder="cluster.xxxxx.region.redshift.amazonaws.com"
            />
          </label>
          <label>
            Port{" "}
            <input
              type="number"
              required
              value={form.port}
              onChange={(e) => setForm({ ...form, port: +e.target.value })}
            />
          </label>
          <label>
            Database{" "}
            <input
              required
              value={form.database}
              onChange={(e) => setForm({ ...form, database: e.target.value })}
              placeholder="dev"
            />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            Authentication
            <select
              value={authType}
              onChange={(e) => {
                setAuthType(e.target.value);
                setAuthFields({});
              }}
            >
              <option value="password">Username / Password</option>
              <option value="iam">IAM Credentials</option>
            </select>
          </label>
          {authType === "password" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
          {authType === "iam" && (
            <>
              <label>
                Access Key ID{" "}
                <input
                  required
                  value={authFields.access_key_id ?? ""}
                  onChange={(e) => setAuthFields({ ...authFields, access_key_id: e.target.value })}
                  placeholder="${env:AWS_ACCESS_KEY_ID}"
                />
              </label>
              <label>
                Secret Access Key{" "}
                <input
                  type="password"
                  required
                  value={authFields.secret_access_key ?? ""}
                  onChange={(e) =>
                    setAuthFields({ ...authFields, secret_access_key: e.target.value })
                  }
                  placeholder="${env:AWS_SECRET_ACCESS_KEY}"
                />
              </label>
              <label>
                Region{" "}
                <input
                  value={authFields.region ?? "us-east-1"}
                  onChange={(e) => setAuthFields({ ...authFields, region: e.target.value })}
                />
              </label>
            </>
          )}
        </>
      )}
      {form.type === "elasticsearch" && (
        <>
          <label>
            Host{" "}
            <input
              required
              value={form.host}
              onChange={(e) => setForm({ ...form, host: e.target.value })}
              placeholder="https://localhost:9200"
            />
          </label>
          <label>
            Port{" "}
            <input
              type="number"
              required
              value={form.port}
              onChange={(e) => setForm({ ...form, port: +e.target.value })}
            />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            Authentication
            <select
              value={authType}
              onChange={(e) => {
                setAuthType(e.target.value);
                setAuthFields({});
              }}
            >
              <option value="none">No Auth</option>
              <option value="basic">Basic Auth</option>
              <option value="api_key">API Key</option>
              <option value="bearer">Bearer Token</option>
            </select>
          </label>
          {authType === "basic" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
          {authType === "api_key" && (
            <label style={{ gridColumn: "1 / -1" }}>
              API Key (base64 id:key){" "}
              <input
                required
                value={authFields.api_key ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, api_key: e.target.value })}
                placeholder="${env:ES_API_KEY}"
              />
            </label>
          )}
          {authType === "bearer" && (
            <label style={{ gridColumn: "1 / -1" }}>
              Token{" "}
              <input
                required
                value={authFields.token ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })}
              />
            </label>
          )}
        </>
      )}
      {isDataLake && (
        <>
          {form.type === "hive" && (
            <>
              <label>
                Metastore URI{" "}
                <input
                  required
                  value={form.host}
                  onChange={(e) => setForm({ ...form, host: e.target.value })}
                  placeholder="thrift://hive-metastore:9083"
                />
              </label>
              <label>
                Warehouse Path{" "}
                <input
                  required
                  value={form.database}
                  onChange={(e) => setForm({ ...form, database: e.target.value })}
                  placeholder="s3://bucket/warehouse"
                />
              </label>
            </>
          )}
          {(form.type === "delta_lake" || form.type === "iceberg") && (
            <>
              <label>
                Metastore URI{" "}
                <input
                  value={form.host}
                  onChange={(e) => setForm({ ...form, host: e.target.value })}
                  placeholder="thrift://hive-metastore:9083 (optional)"
                />
              </label>
              <label>
                Warehouse Path{" "}
                <input
                  required
                  value={form.database}
                  onChange={(e) => setForm({ ...form, database: e.target.value })}
                  placeholder="s3://bucket/warehouse"
                />
              </label>
            </>
          )}
          <label style={{ gridColumn: "1 / -1" }}>
            Storage Authentication
            <select
              value={authType}
              onChange={(e) => {
                setAuthType(e.target.value);
                setAuthFields({});
              }}
            >
              <option value="none">None (instance role / local)</option>
              <option value="aws">AWS S3 (Access Key)</option>
              <option value="azure">Azure ADLS</option>
              <option value="gcs">Google Cloud Storage</option>
            </select>
          </label>
          {authType === "aws" && (
            <>
              <label>
                Access Key ID{" "}
                <input
                  required
                  value={authFields.access_key_id ?? ""}
                  onChange={(e) => setAuthFields({ ...authFields, access_key_id: e.target.value })}
                  placeholder="${env:AWS_ACCESS_KEY_ID}"
                />
              </label>
              <label>
                Secret Access Key{" "}
                <input
                  type="password"
                  required
                  value={authFields.secret_access_key ?? ""}
                  onChange={(e) =>
                    setAuthFields({ ...authFields, secret_access_key: e.target.value })
                  }
                  placeholder="${env:AWS_SECRET_ACCESS_KEY}"
                />
              </label>
              <label>
                Region{" "}
                <input
                  value={authFields.region ?? "us-east-1"}
                  onChange={(e) => setAuthFields({ ...authFields, region: e.target.value })}
                />
              </label>
              <label>
                S3 Endpoint (MinIO){" "}
                <input
                  value={authFields.endpoint ?? ""}
                  onChange={(e) => setAuthFields({ ...authFields, endpoint: e.target.value })}
                  placeholder="optional — for S3-compatible"
                />
              </label>
            </>
          )}
          {authType === "azure" && (
            <>
              <label>
                Storage Account{" "}
                <input
                  required
                  value={authFields.storage_account ?? ""}
                  onChange={(e) =>
                    setAuthFields({ ...authFields, storage_account: e.target.value })
                  }
                />
              </label>
              <label>
                Access Key{" "}
                <input
                  type="password"
                  value={authFields.access_key ?? ""}
                  onChange={(e) => setAuthFields({ ...authFields, access_key: e.target.value })}
                  placeholder="shared key (or use SAS)"
                />
              </label>
              <label>
                SAS Token{" "}
                <input
                  value={authFields.sas_token ?? ""}
                  onChange={(e) => setAuthFields({ ...authFields, sas_token: e.target.value })}
                  placeholder="alternative to access key"
                />
              </label>
            </>
          )}
          {authType === "gcs" && (
            <label style={{ gridColumn: "1 / -1" }}>
              Credentials JSON Path{" "}
              <input
                required
                value={authFields.credentials_json ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, credentials_json: e.target.value })}
                placeholder="/path/to/service-account.json"
              />
            </label>
          )}
        </>
      )}
      <SourceFormFieldsExtended {...props} />
    </>
  );
}
