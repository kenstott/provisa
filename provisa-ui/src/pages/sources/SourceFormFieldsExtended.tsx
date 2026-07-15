// Copyright (c) 2026 Kenneth Stott
// Canary: 55ecea78-4131-4bb4-8a93-71e1bdacea84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

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
  const isKafka = getCategory(form.type) === "Streaming";
  return (
    <>
      {form.type === "prometheus" && (
        <>
          <label style={{ gridColumn: "1 / -1" }}>
            URL{" "}
            <input
              required
              value={form.host}
              onChange={(e) => setForm({ ...form, host: e.target.value })}
              placeholder="http://prometheus:9090"
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
              <option value="bearer">Bearer Token</option>
            </select>
          </label>
          {authType === "basic" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
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
      {form.type === "google_sheets" && (
        <>
          <label style={{ gridColumn: "1 / -1" }}>
            Credentials JSON Path{" "}
            <input
              required
              value={authFields.credentials_json ?? ""}
              onChange={(e) => setAuthFields({ ...authFields, credentials_json: e.target.value })}
              placeholder="/path/to/service-account.json"
            />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            Metadata Sheet ID{" "}
            <input
              value={form.database}
              onChange={(e) => setForm({ ...form, database: e.target.value })}
              placeholder="Sheet ID for table definitions"
            />
          </label>
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
          <label style={{ gridColumn: "1 / -1" }}>
            Endpoint URL{" "}
            <input
              required
              value={form.host}
              onChange={(e) => setForm({ ...form, host: e.target.value })}
              placeholder="https://api.example.com/graphql"
            />
          </label>
          <label>
            Namespace{" "}
            <input
              required
              value={gqlNamespace}
              onChange={(e) => setGqlNamespace(e.target.value)}
              placeholder="myapi"
            />
          </label>
          <label>
            Cache TTL (seconds){" "}
            <input
              type="number"
              min={0}
              value={gqlCacheTtl}
              onChange={(e) => setGqlCacheTtl(e.target.value)}
              placeholder="300"
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
              <option value="bearer">Bearer Token</option>
              <option value="basic">Basic Auth</option>
            </select>
          </label>
          {authType === "bearer" && (
            <label style={{ gridColumn: "1 / -1" }}>
              Token{" "}
              <input
                required
                value={authFields.token ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })}
                placeholder="${env:GQL_TOKEN}"
              />
            </label>
          )}
          {authType === "basic" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
        </>
      )}
      {form.type === "grpc" && (
        <>
          <label style={{ gridColumn: "1 / -1" }}>
            Proto Path or URL
            <input
              required
              value={grpcProtoPath}
              onChange={(e) => setGrpcProtoPath(e.target.value)}
              placeholder="https://api.example.com/service.proto or ./service.proto"
            />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            Server Address
            <input
              required
              value={grpcServerAddress}
              onChange={(e) => setGrpcServerAddress(e.target.value)}
              placeholder="api.example.com:50051"
            />
          </label>
          <label>
            Namespace
            <input
              value={grpcNamespace}
              onChange={(e) => setGrpcNamespace(e.target.value)}
              placeholder="mygrpc"
            />
          </label>
          <label>
            Cache TTL (seconds)
            <input
              type="number"
              min={0}
              value={grpcCacheTtl}
              onChange={(e) => setGrpcCacheTtl(e.target.value)}
              placeholder="300"
            />
          </label>
          <label
            style={{
              flexDirection: "row",
              alignItems: "center",
              gap: "0.5rem",
              whiteSpace: "nowrap",
            }}
          >
            <input
              type="checkbox"
              checked={grpcTls}
              onChange={(e) => setGrpcTls(e.target.checked)}
              style={{ width: "auto" }}
            />
            TLS
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            Import Paths{" "}
            <span style={{ color: "var(--text-muted)", fontSize: "0.8rem" }}>
              (comma-separated, optional)
            </span>
            <input
              value={grpcImportPaths}
              onChange={(e) => setGrpcImportPaths(e.target.value)}
              placeholder="/path/to/protos,/another/path"
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
              {API_AUTH_TYPES.map((a) => (
                <option key={a.value} value={a.value}>
                  {a.label}
                </option>
              ))}
            </select>
          </label>
          {authType === "bearer" && (
            <label style={{ gridColumn: "1 / -1" }}>
              Token{" "}
              <input
                required
                value={authFields.token ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })}
                placeholder="${env:GRPC_TOKEN}"
              />
            </label>
          )}
          {authType === "basic" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
          {authType === "api_key" && (
            <>
              <label>
                Header Name{" "}
                <input
                  required
                  value={authFields.header_name ?? "X-API-Key"}
                  onChange={(e) => setAuthFields({ ...authFields, header_name: e.target.value })}
                  placeholder="X-API-Key"
                />
              </label>
              <label>
                API Key{" "}
                <input
                  required
                  value={authFields.api_key ?? ""}
                  onChange={(e) => setAuthFields({ ...authFields, api_key: e.target.value })}
                  placeholder="${env:API_KEY}"
                />
              </label>
            </>
          )}
        </>
      )}
      {form.type === "neo4j" && (
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
              value={form.database}
              onChange={(e) => setForm({ ...form, database: e.target.value })}
              placeholder="neo4j"
            />
          </label>
          <label
            style={{
              flexDirection: "row",
              alignItems: "center",
              gap: "0.5rem",
              whiteSpace: "nowrap",
            }}
          >
            <input
              type="checkbox"
              checked={authFields.use_https === "true"}
              onChange={(e) =>
                setAuthFields({ ...authFields, use_https: e.target.checked ? "true" : "false" })
              }
              style={{ width: "auto" }}
            />
            HTTPS
          </label>
          <label>
            Username{" "}
            <input
              value={form.username}
              onChange={(e) => setForm({ ...form, username: e.target.value })}
              placeholder="neo4j"
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
      {form.type === "sparql" && (
        <>
          <label style={{ gridColumn: "1 / -1" }}>
            Endpoint URL{" "}
            <input
              required
              value={form.host}
              onChange={(e) => setForm({ ...form, host: e.target.value })}
              placeholder="https://dbpedia.org/sparql"
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
              <option value="bearer">Bearer Token</option>
              <option value="basic">Basic Auth</option>
            </select>
          </label>
          {authType === "bearer" && (
            <label style={{ gridColumn: "1 / -1" }}>
              Token{" "}
              <input
                required
                value={authFields.token ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })}
                placeholder="${env:SPARQL_TOKEN}"
              />
            </label>
          )}
          {authType === "basic" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
        </>
      )}
      {form.type === "govdata" && (
        <>
          <div style={{ gridColumn: "1 / -1" }}>
            <label style={{ marginBottom: "0.5rem" }}>Data Subjects</label>
            <div style={{ display: "flex", flexWrap: "wrap", gap: "0.5rem", marginTop: "0.25rem" }}>
              {GOVDATA_SUBJECTS.map((subj) => (
                <label
                  key={subj.value}
                  style={{
                    flexDirection: "row",
                    alignItems: "center",
                    gap: "0.4rem",
                    whiteSpace: "nowrap",
                    fontWeight: "normal",
                  }}
                >
                  <input
                    type="checkbox"
                    style={{ width: "auto" }}
                    checked={govdataSubjects.includes(subj.value)}
                    onChange={(e) =>
                      setGovdataSubjects(
                        e.target.checked
                          ? [...govdataSubjects, subj.value]
                          : govdataSubjects.filter((x) => x !== subj.value),
                      )
                    }
                  />
                  {subj.label}
                </label>
              ))}
            </div>
          </div>
          <div
            style={{ gridColumn: "1 / -1", display: "flex", gap: "0.5rem", alignItems: "flex-end" }}
          >
            <label style={{ flex: 1 }}>
              AskAmerica API Key
              <input
                value={form.username}
                onChange={(e) => setForm({ ...form, username: e.target.value })}
                placeholder="${ASKAMERICA_API_KEY}"
              />
            </label>
            <button type="button" onClick={() => window.open("https://askamerica.ai", "_blank")}>
              Get API Key →
            </button>
          </div>
          {submitting && !editingSourceId && (
            <div style={{ gridColumn: "1 / -1", color: "var(--text-muted)", fontSize: "0.85rem" }}>
              Validating API key and pre-loading metadata… this may take up to 30 seconds on first
              use.
            </div>
          )}
        </>
      )}
      {isKafka && (
        <>
          <label>
            Bootstrap Servers{" "}
            <input
              required
              value={form.host}
              onChange={(e) => setForm({ ...form, host: e.target.value })}
              placeholder="kafka:9092"
            />
          </label>
          <label>
            Schema Registry URL{" "}
            <input
              value={form.database}
              onChange={(e) => setForm({ ...form, database: e.target.value })}
              placeholder="http://schema-registry:8081"
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
              {KAFKA_AUTH_TYPES.map((a) => (
                <option key={a.value} value={a.value}>
                  {a.label}
                </option>
              ))}
            </select>
          </label>
          {authType !== "none" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
        </>
      )}
      {form.type === "sharepoint" && (
        <>
          <label style={{ gridColumn: "1 / -1" }}>
            Site URL{" "}
            <input
              required
              value={form.host}
              onChange={(e) => setForm({ ...form, host: e.target.value })}
              placeholder="https://contoso.sharepoint.com/sites/mysite"
            />
          </label>
          <label>
            Tenant ID{" "}
            <input
              required
              value={form.database}
              onChange={(e) => setForm({ ...form, database: e.target.value })}
              placeholder="Azure AD tenant ID"
            />
          </label>
          <label>
            Auth Type{" "}
            <select
              value={spAuthType}
              onChange={(e) => setSpAuthType(e.target.value)}
            >
              <option value="CLIENT_CREDENTIALS">Client Credentials</option>
              <option value="USERNAME_PASSWORD">Username / Password</option>
              <option value="CERTIFICATE">Certificate</option>
            </select>
          </label>
          <label>
            Client ID{" "}
            <input
              required
              value={form.username}
              onChange={(e) => setForm({ ...form, username: e.target.value })}
              placeholder="Azure AD app client ID"
            />
          </label>
          {spAuthType === "CLIENT_CREDENTIALS" && (
            <label>
              Client Secret{" "}
              <input
                type="password"
                value={form.password}
                onChange={(e) => setForm({ ...form, password: e.target.value })}
                placeholder="Client secret"
              />
            </label>
          )}
          {spAuthType === "USERNAME_PASSWORD" && (
            <>
              <label>
                SP Username{" "}
                <input
                  required
                  value={spUsername}
                  onChange={(e) => setSpUsername(e.target.value)}
                  placeholder="user@contoso.com"
                />
              </label>
              <label>
                SP Password{" "}
                <input
                  type="password"
                  required
                  value={spPassword}
                  onChange={(e) => setSpPassword(e.target.value)}
                />
              </label>
            </>
          )}
          {spAuthType === "CERTIFICATE" && (
            <>
              <label>
                Client Secret{" "}
                <input
                  type="password"
                  value={form.password}
                  onChange={(e) => setForm({ ...form, password: e.target.value })}
                  placeholder="Client secret (if required alongside cert)"
                />
              </label>
              <label style={{ gridColumn: "1 / -1" }}>
                Certificate Path{" "}
                <input
                  required
                  value={spCertPath}
                  onChange={(e) => setSpCertPath(e.target.value)}
                  placeholder="/certs/sharepoint.pfx"
                />
              </label>
              <label>
                Certificate Password{" "}
                <input
                  type="password"
                  value={spCertPassword}
                  onChange={(e) => setSpCertPassword(e.target.value)}
                />
              </label>
            </>
          )}
        </>
      )}
      {form.type === "splunk" && (
        <>
          <label>
            Host{" "}
            <input
              required
              value={form.host}
              onChange={(e) => setForm({ ...form, host: e.target.value })}
              placeholder="splunk.example.com"
            />
          </label>
          <label>
            Port{" "}
            <input
              type="number"
              value={form.port}
              onChange={(e) => setForm({ ...form, port: +e.target.value })}
            />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            Auth Token{" "}
            <input
              type="password"
              required
              value={form.password}
              onChange={(e) => setForm({ ...form, password: e.target.value })}
              placeholder="Splunk authentication token"
            />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            App (optional){" "}
            <input
              value={form.database}
              onChange={(e) => setForm({ ...form, database: e.target.value })}
              placeholder="Splunk_SA_CIM"
            />
          </label>
          <label style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem" }}>
            <input
              type="checkbox"
              checked={splunkDisableSsl}
              onChange={(e) => setSplunkDisableSsl(e.target.checked)}
            />
            Disable SSL Validation
          </label>
        </>
      )}
      {form.type === "files" && (() => {
        const transportMeta = FILE_TRANSPORTS.find((t) => t.value === filesTransport)!;
        return (
          <>
            <label>
              Transport{" "}
              <select
                value={filesTransport}
                onChange={(e) => {
                  setFilesTransport(e.target.value);
                  setForm({ ...form, username: "", password: "" });
                }}
              >
                {FILE_TRANSPORTS.map((t) => (
                  <option key={t.value} value={t.value}>{t.label}</option>
                ))}
              </select>
            </label>
            <label>
              Path / Glob{" "}
              <input
                required
                value={form.path}
                onChange={(e) => setForm({ ...form, path: e.target.value })}
                placeholder={filesTransport === "file://" ? "/data/files/**" : "bucket/prefix/**"}
              />
              <small style={{ color: "var(--text-muted, #888)" }}>
                Glob pattern relative to transport root. Supports csv, parquet, arrow, json, xlsx, docx, pptx, md, html, .gz, .zip.
              </small>
            </label>
            {transportMeta.needsAuth === "s3" && (
              <>
                <label>
                  Access Key ID{" "}
                  <input
                    value={form.username}
                    onChange={(e) => setForm({ ...form, username: e.target.value })}
                  />
                </label>
                <label>
                  Secret Access Key{" "}
                  <input
                    type="password"
                    value={form.password}
                    onChange={(e) => setForm({ ...form, password: e.target.value })}
                  />
                </label>
              </>
            )}
            {transportMeta.needsAuth === "userpass" && (
              <>
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
            {transportMeta.needsAuth === "cert-or-userpass" && (
              <>
                <label>
                  Auth Method{" "}
                  <select
                    value={filesAuthMode}
                    onChange={(e) => {
                      setFilesAuthMode(e.target.value as "userpass" | "certificate");
                      setForm({ ...form, username: "", password: "" });
                      setFilesCertPath("");
                      setFilesCertPassword("");
                    }}
                  >
                    <option value="userpass">Username / Password</option>
                    <option value="certificate">Certificate (PFX)</option>
                  </select>
                </label>
                {filesAuthMode === "userpass" && (
                  <>
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
                {filesAuthMode === "certificate" && (
                  <>
                    <label>
                      PFX Certificate Path{" "}
                      <input
                        value={filesCertPath}
                        onChange={(e) => setFilesCertPath(e.target.value)}
                        placeholder="/path/to/cert.pfx"
                      />
                    </label>
                    <label>
                      Certificate Password{" "}
                      <input
                        type="password"
                        value={filesCertPassword}
                        onChange={(e) => setFilesCertPassword(e.target.value)}
                      />
                    </label>
                  </>
                )}
              </>
            )}
          </>
        );
      })()}
      {editingSourceId && (
        <>
          <label>
            Naming Convention
            <select
              value={form.gqlNamingConvention}
              onChange={(e) => setForm({ ...form, gqlNamingConvention: e.target.value })}
            >
              {NAMING_CONVENTIONS.map((nc) => (
                <option key={nc.value} value={nc.value}>
                  {nc.label}
                </option>
              ))}
            </select>
          </label>
          <div
            style={{ display: "flex", alignItems: "center", gap: "1.5rem", flexWrap: "wrap" }}
          >
            <label
              style={{
                flexDirection: "row",
                alignItems: "center",
                gap: "0.5rem",
                whiteSpace: "nowrap",
              }}
            >
              <input
                type="checkbox"
                checked={form.cacheEnabled}
                onChange={(e) => setForm({ ...form, cacheEnabled: e.target.checked })}
                style={{ width: "auto" }}
              />
              Cache Enabled
            </label>
            <label
              title="Force this source's tables to be materialized into the store and federated from there, instead of reached live. Use when the connector is a poor fit for your queries."
              style={{
                display: "flex",
                flexDirection: "row",
                alignItems: "center",
                gap: "0.5rem",
                whiteSpace: "nowrap",
              }}
            >
              <input
                type="checkbox"
                checked={form.preferMaterialized}
                onChange={(e) => setForm({ ...form, preferMaterialized: e.target.checked })}
                style={{ width: "auto" }}
              />
              Prefer Materialized
            </label>
          </div>
          <label>
            Cache TTL (seconds)
            <input
              type="number"
              min={0}
              value={form.cacheTtl}
              onChange={(e) => setForm({ ...form, cacheTtl: e.target.value })}
              placeholder="inherit global"
            />
          </label>
          <label title="Default change-detection signal inherited by this source's tables (each table can override it). Options are gated by the source type's capabilities.">
            Change Signal
            <select
              value={form.changeSignal}
              onChange={(e) => setForm({ ...form, changeSignal: e.target.value })}
            >
              {sourceChangeSignals(form.type).map((cs) => (
                <option key={cs} value={cs}>
                  {CHANGE_SIGNAL_LABELS[cs] ?? cs}
                </option>
              ))}
            </select>
          </label>
          {domainsEnabled && (
            <label>
              <span style={{ display: "flex", alignItems: "baseline", gap: "0.35rem" }}>
                Allowed Domains
                <span
                  style={{ fontWeight: "normal", color: "var(--text-muted)", fontSize: "0.75rem" }}
                >
                  (none selected = all)
                </span>
              </span>
              <MultiSelect
                options={domains.map((d) => ({ id: d.id, label: d.id }))}
                value={form.allowedDomains
                  .split(",")
                  .map((s) => s.trim())
                  .filter(Boolean)}
                onChange={(selected) =>
                  setForm({ ...form, allowedDomains: selected.join(", ") })
                }
              />
            </label>
          )}
          {cdcTransportApplicable(form.type) && (
            <details style={{ border: "1px solid #333", borderRadius: 4, padding: "0.75rem" }}>
              <summary style={{ cursor: "pointer", fontWeight: 600 }}>
                CDC Transport (Debezium)
              </summary>
              <p style={{ margin: "0.6rem 0", fontSize: "0.8rem", opacity: 0.7 }}>
                Entered once per source. Tables from this source only choose delivery=cdc; they
                inherit this transport.
              </p>
              <label>
                Bootstrap Servers
                <input
                  value={cdc.bootstrapServers}
                  onChange={(e) => setCdc({ ...cdc, bootstrapServers: e.target.value })}
                  placeholder="broker1:9092,broker2:9092"
                />
              </label>
              <label>
                Topic Prefix
                <input
                  value={cdc.topicPrefix}
                  onChange={(e) => setCdc({ ...cdc, topicPrefix: e.target.value })}
                  placeholder="dbserver1 (topics: {prefix}.{schema}.{table})"
                />
              </label>
              <label>
                Schema Registry URL
                <input
                  value={cdc.schemaRegistryUrl}
                  onChange={(e) => setCdc({ ...cdc, schemaRegistryUrl: e.target.value })}
                  placeholder="blank = JSON; set for Avro"
                />
              </label>
              <label>
                Consumer Group ID (optional)
                <input
                  value={cdc.consumerGroupId}
                  onChange={(e) => setCdc({ ...cdc, consumerGroupId: e.target.value })}
                  placeholder="inherit global (provisa-debezium)"
                />
              </label>
            </details>
          )}
        </>
      )}
    </>
  );
}
