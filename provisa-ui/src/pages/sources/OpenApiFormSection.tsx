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
import { Alert, Button, Group, NumberInput, Radio, Select, Text, TextInput, Textarea } from "@mantine/core";
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
  const { t } = useTranslation();
  return (
    <>
      <Radio.Group
        name="openapiSpecMode"
        label={t("openApiFormSection.specModeLegend")}
        value={openapiSpecMode}
        onChange={(v) => setOpenapiSpecMode(v as "path" | "inline")}
        style={{ gridColumn: "1 / -1" }}
      >
        <Group mt="xs" gap="lg">
          <Radio value="path" label={t("openApiFormSection.specModePath")} data-testid="openapi-spec-mode-path" />
          <Radio value="inline" label={t("openApiFormSection.specModeInline")} data-testid="openapi-spec-mode-inline" />
        </Group>
      </Radio.Group>
      {openapiSpecMode === "path" ? (
        <TextInput
          required
          label={t("openApiFormSection.specPathLabel")}
          value={openapiSpecPath}
          onChange={(e) => setOpenapiSpecPath(e.target.value)}
          placeholder={t("openApiFormSection.specPathPlaceholder")}
          style={{ gridColumn: "1 / -1" }}
          data-testid="openapi-spec-path-input"
        />
      ) : (
        <Textarea
          required
          label={t("openApiFormSection.specInlineLabel")}
          value={openapiSpecInline}
          onChange={(e) => setOpenapiSpecInline(e.target.value)}
          placeholder={
            "openapi: '3.0.0'\ninfo:\n  title: My API\n  version: '1.0'\npaths:\n  /items:\n    get:\n      operationId: listItems\n      responses:\n        '200':\n          description: OK"
          }
          styles={{ input: { fontFamily: "monospace", fontSize: "0.8rem" } }}
          autosize
          minRows={8}
          style={{ gridColumn: "1 / -1" }}
          data-testid="openapi-spec-inline-input"
        />
      )}
      <TextInput
        label={
          <>
            {t("openApiFormSection.baseUrlLabel")}{" "}
            <Text component="span" c="dimmed" size="xs">
              {t("openApiFormSection.baseUrlHint")}
            </Text>
          </>
        }
        value={openapiBaseUrl}
        onChange={(e) => setOpenapiBaseUrl(e.target.value)}
        placeholder={t("openApiFormSection.baseUrlPlaceholder")}
        style={{ gridColumn: "1 / -1" }}
        data-testid="openapi-base-url-input"
      />
      <NumberInput
        label={t("openApiFormSection.cacheTtlLabel")}
        min={0}
        value={openapiCacheTtl === "" ? "" : Number(openapiCacheTtl)}
        onChange={(v) => setOpenapiCacheTtl(v === "" ? "" : String(v))}
        placeholder={t("openApiFormSection.cacheTtlPlaceholder")}
        data-testid="openapi-cache-ttl-input"
      />
      <Select
        label={t("openApiFormSection.authenticationLabel")}
        value={authType}
        onChange={(v) => {
          setAuthType(v ?? "");
          setAuthFields({});
        }}
        data={API_AUTH_TYPES.map((a) => ({ value: a.value, label: a.label }))}
        allowDeselect={false}
        style={{ gridColumn: "1 / -1" }}
        data-testid="openapi-auth-type-select"
      />
      {authType === "bearer" && (
        <TextInput
          required
          label={t("openApiFormSection.tokenLabel")}
          value={authFields.token ?? ""}
          onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })}
          placeholder={t("openApiFormSection.tokenPlaceholder")}
          style={{ gridColumn: "1 / -1" }}
          data-testid="openapi-bearer-token-input"
        />
      )}
      {authType === "basic" && (
        <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
      )}
      {authType === "api_key" && (
        <>
          <TextInput
            required
            label={t("openApiFormSection.headerNameLabel")}
            value={authFields.header_name ?? "X-API-Key"}
            onChange={(e) => setAuthFields({ ...authFields, header_name: e.target.value })}
            placeholder={t("openApiFormSection.headerNamePlaceholder")}
            data-testid="openapi-header-name-input"
          />
          <TextInput
            required
            label={t("openApiFormSection.apiKeyLabel")}
            value={authFields.api_key ?? ""}
            onChange={(e) => setAuthFields({ ...authFields, api_key: e.target.value })}
            placeholder={t("openApiFormSection.apiKeyPlaceholder")}
            data-testid="openapi-api-key-input"
          />
        </>
      )}
      <Group style={{ gridColumn: "1 / -1" }} gap="sm" align="center">
        <Button
          type="button"
          onClick={onOpenapiPreview}
          loading={openapiPreviewing}
          disabled={openapiSpecMode === "path" ? !openapiSpecPath : !openapiSpecInline}
          data-testid="openapi-preview-button"
        >
          {openapiPreviewing ? t("openApiFormSection.previewLoading") : t("openApiFormSection.previewButton")}
        </Button>
        {openapiPreviewError && (
          <Alert color="red" variant="light" py={4} px="sm" data-testid="openapi-preview-error">
            {openapiPreviewError}
          </Alert>
        )}
      </Group>
      {openapiPreview && (
        <Text size="sm" c="dimmed" style={{ gridColumn: "1 / -1" }} data-testid="openapi-preview-summary">
          <Text component="strong" size="sm">
            {t("openApiFormSection.queriesCount", { count: openapiPreview.queries.length })}
          </Text>
          {": "}
          {openapiPreview.queries.map((q) => q.operation_id).join(", ") || t("openApiFormSection.none")}
          <br />
          <Text component="strong" size="sm">
            {t("openApiFormSection.mutationsCount", { count: openapiPreview.mutations.length })}
          </Text>
          {": "}
          {openapiPreview.mutations.map((m) => m.operation_id).join(", ") || t("openApiFormSection.none")}
        </Text>
      )}
    </>
  );
}
