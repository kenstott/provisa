// Copyright (c) 2026 Kenneth Stott
// Canary: 55ecea78-4131-4bb4-8a93-71e1bdacea84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { API_AUTH_TYPES } from "./constants";
import { AuthUserPass } from "./AuthUserPass";

interface OpenApiFormSectionProps {
  openapiSpecMode: "path" | "inline";
  setOpenapiSpecMode: (v: "path" | "inline") => void;
  openapiSpecPath: string;
  setOpenapiSpecPath: (v: string) => void;
  openapiSpecInline: string;
  setOpenapiSpecInline: (v: string) => void;
  openapiBaseUrl: string;
  setOpenapiBaseUrl: (v: string) => void;
  openapiCacheTtl: string;
  setOpenapiCacheTtl: (v: string) => void;
  authType: string;
  setAuthType: (v: string) => void;
  authFields: Record<string, string>;
  setAuthFields: (f: Record<string, string>) => void;
  onOpenapiPreview: () => void;
  openapiPreviewing: boolean;
  openapiPreviewError: string | null;
  openapiPreview: {
    queries: { operation_id: string }[];
    mutations: { operation_id: string }[];
    spec_description?: string;
  } | null;
}

export function OpenApiFormSection({
  openapiSpecMode,
  setOpenapiSpecMode,
  openapiSpecPath,
  setOpenapiSpecPath,
  openapiSpecInline,
  setOpenapiSpecInline,
  openapiBaseUrl,
  setOpenapiBaseUrl,
  openapiCacheTtl,
  setOpenapiCacheTtl,
  authType,
  setAuthType,
  authFields,
  setAuthFields,
  onOpenapiPreview,
  openapiPreviewing,
  openapiPreviewError,
  openapiPreview,
}: OpenApiFormSectionProps) {
  return (
    <>
    <div style={{ gridColumn: "1 / -1", display: "flex", gap: "1rem" }}>
      <label
        style={{
          flexDirection: "row",
          alignItems: "center",
          gap: "0.4rem",
          whiteSpace: "nowrap",
        }}
      >
        <input
          type="radio"
          name="openapiSpecMode"
          checked={openapiSpecMode === "path"}
          onChange={() => setOpenapiSpecMode("path")}
          style={{ width: "auto" }}
        />
        Spec path / URL
      </label>
      <label
        style={{
          flexDirection: "row",
          alignItems: "center",
          gap: "0.4rem",
          whiteSpace: "nowrap",
        }}
      >
        <input
          type="radio"
          name="openapiSpecMode"
          checked={openapiSpecMode === "inline"}
          onChange={() => setOpenapiSpecMode("inline")}
          style={{ width: "auto" }}
        />
        Write spec inline
      </label>
    </div>
    {openapiSpecMode === "path" ? (
      <label style={{ gridColumn: "1 / -1" }}>
        Spec Path or URL
        <input
          required
          value={openapiSpecPath}
          onChange={(e) => setOpenapiSpecPath(e.target.value)}
          placeholder="https://api.example.com/openapi.json or ./spec.yaml"
        />
      </label>
    ) : (
      <label style={{ gridColumn: "1 / -1" }}>
        OpenAPI Spec (YAML or JSON)
        <textarea
          required
          value={openapiSpecInline}
          onChange={(e) => setOpenapiSpecInline(e.target.value)}
          placeholder={
            "openapi: '3.0.0'\ninfo:\n  title: My API\n  version: '1.0'\npaths:\n  /items:\n    get:\n      operationId: listItems\n      responses:\n        '200':\n          description: OK"
          }
          style={{
            fontFamily: "monospace",
            fontSize: "0.8rem",
            minHeight: "200px",
            resize: "vertical",
          }}
        />
      </label>
    )}
    <label style={{ gridColumn: "1 / -1" }}>
      Base URL{" "}
      <span style={{ color: "var(--text-muted)", fontSize: "0.8rem" }}>
        (leave blank to use servers[0].url from spec)
      </span>
      <input
        value={openapiBaseUrl}
        onChange={(e) => setOpenapiBaseUrl(e.target.value)}
        placeholder="https://api.example.com (optional override)"
      />
    </label>
    <label>
      Cache TTL (seconds)
      <input
        type="number"
        min={0}
        value={openapiCacheTtl}
        onChange={(e) => setOpenapiCacheTtl(e.target.value)}
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
          placeholder="${env:API_TOKEN}"
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
    <div
      style={{ gridColumn: "1 / -1", display: "flex", gap: "0.5rem", alignItems: "center" }}
    >
      <button
        type="button"
        onClick={onOpenapiPreview}
        disabled={
          openapiPreviewing ||
          (openapiSpecMode === "path" ? !openapiSpecPath : !openapiSpecInline)
        }
      >
        {openapiPreviewing ? "Loading..." : "Preview"}
      </button>
      {openapiPreviewError && (
        <span className="error" style={{ fontSize: "0.85rem" }}>
          {openapiPreviewError}
        </span>
      )}
    </div>
    {openapiPreview && (
      <div style={{ gridColumn: "1 / -1", fontSize: "0.85rem", color: "var(--text-muted)" }}>
        <strong>{openapiPreview.queries.length} queries</strong>:{" "}
        {openapiPreview.queries.map((q) => q.operation_id).join(", ") || "none"}
        <br />
        <strong>{openapiPreview.mutations.length} mutations</strong>:{" "}
        {openapiPreview.mutations.map((m) => m.operation_id).join(", ") || "none"}
      </div>
    )}
    </>
  );
}
