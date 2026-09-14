// Copyright (c) 2026 Kenneth Stott
// Canary: a61b8a18-9f97-4100-b9f1-dd3e012b470d
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Extracted out of SourceFormFieldsExtended.tsx to keep that file under the eslint max-lines gate
// (same pattern as PushFeedFormSection.tsx).

import { Select, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { AuthUserPass } from "./AuthUserPass";
import type { SourceFormState } from "./SourceFormFields";

export function SparqlFormSection({
  form,
  setForm,
  authType,
  setAuthType,
  authFields,
  setAuthFields,
}: {
  form: SourceFormState;
  setForm: (f: SourceFormState) => void;
  authType: string;
  setAuthType: (v: string) => void;
  authFields: Record<string, string>;
  setAuthFields: (f: Record<string, string>) => void;
}) {
  const { t } = useTranslation();
  return (
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
      {/* REQ-1740: optional named-graph restriction, rides in federation_hints. */}
      <TextInput
        label={t("sourceFormFieldsExtended.defaultGraphUri")}
        value={authFields.default_graph_uri ?? ""}
        onChange={(e) => setAuthFields({ ...authFields, default_graph_uri: e.target.value })}
        placeholder="http://example.org/graph (optional)"
        style={{ gridColumn: "1 / -1" }}
        data-testid="sparql-default-graph-uri-input"
      />
      <Select
        label={t("sourceFormFieldsExtended.authentication")}
        value={authType}
        onChange={(v) => {
          setAuthType(v ?? "");
          // Keep default_graph_uri — it's orthogonal to auth mode.
          setAuthFields({ default_graph_uri: authFields.default_graph_uri ?? "" });
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
  );
}
