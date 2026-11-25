// Copyright (c) 2026 Kenneth Stott
// Canary: 4563e24c-5c6f-43d3-8633-7fe9872fb4ed
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { NEVER_EXPIRES, cacheTtlHint } from "../../components/form/cacheTtl";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Button,
  Group,
  PasswordInput,
  Radio,
  Select,
  Text,
  TextInput,
  NumberInput,
} from "@mantine/core";
import { API_AUTH_TYPES } from "./constants";
import { AuthUserPass } from "./AuthUserPass";
import { FormField } from "../../components/form/FormLayout";
import { CodeEditorField, type CodeLanguage } from "../../components/CodeEditorField";

/** REQ-1984: an OpenAPI document is a JSON object or YAML. JSON's text opens with its root object's
 * brace; any other text is YAML. */
function openApiSpecLanguage(spec: string): CodeLanguage {
  return spec.trimStart().startsWith("{") ? "json" : "yaml";
}

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
      <FormField size="full">
        <Radio.Group
          name="openapiSpecMode"
          label={t("openApiFormSection.specModeLegend")}
          value={openapiSpecMode}
          onChange={(v) => setOpenapiSpecMode(v as "path" | "inline")}
        >
          <Group mt="xs" gap="lg">
            <Radio
              value="path"
              label={t("openApiFormSection.specModePath")}
              data-testid="openapi-spec-mode-path"
            />
            <Radio
              value="inline"
              label={t("openApiFormSection.specModeInline")}
              data-testid="openapi-spec-mode-inline"
            />
          </Group>
        </Radio.Group>
      </FormField>
      {openapiSpecMode === "path" ? (
        <FormField size="long">
          <TextInput
            required
            label={t("openApiFormSection.specPathLabel")}
            value={openapiSpecPath}
            onChange={(e) => setOpenapiSpecPath(e.target.value)}
            placeholder={t("openApiFormSection.specPathPlaceholder")}
            data-testid="openapi-spec-path-input"
          />
        </FormField>
      ) : (
        <FormField size="full">
          <CodeEditorField
            required
            label={t("openApiFormSection.specInlineLabel")}
            value={openapiSpecInline}
            onChange={setOpenapiSpecInline}
            language={openApiSpecLanguage(openapiSpecInline)}
            placeholder={
              "openapi: '3.0.0'\ninfo:\n  title: My API\n  version: '1.0'\npaths:\n  /items:\n    get:\n      operationId: listItems\n      responses:\n        '200':\n          description: OK"
            }
            data-testid="openapi-spec-inline-input"
          />
        </FormField>
      )}
      <FormField size="long">
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
          data-testid="openapi-base-url-input"
        />
      </FormField>
      <FormField hint={cacheTtlHint(openapiCacheTtl, t)} size="short">
        <NumberInput
          min={NEVER_EXPIRES}
          label={t("openApiFormSection.cacheTtlLabel")}
          value={openapiCacheTtl === "" ? "" : Number(openapiCacheTtl)}
          onChange={(v) => setOpenapiCacheTtl(v === "" ? "" : String(v))}
          placeholder={t("openApiFormSection.cacheTtlPlaceholder")}
          data-testid="openapi-cache-ttl-input"
        />
      </FormField>
      <FormField section="authentication" size="medium">
        <Select
          label={t("openApiFormSection.authenticationLabel")}
          value={authType}
          onChange={(v) => {
            setAuthType(v ?? "");
            setAuthFields({});
          }}
          data={API_AUTH_TYPES.map((a) => ({ value: a.value, label: a.label }))}
          allowDeselect={false}
          data-testid="openapi-auth-type-select"
        />
      </FormField>
      {authType === "bearer" && (
        <FormField section="authentication" size="long">
          <PasswordInput
            required
            label={t("openApiFormSection.tokenLabel")}
            value={authFields.token ?? ""}
            onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })}
            placeholder={t("openApiFormSection.tokenPlaceholder")}
            data-testid="openapi-bearer-token-input"
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
              label={t("openApiFormSection.headerNameLabel")}
              value={authFields.header_name ?? "X-API-Key"}
              onChange={(e) => setAuthFields({ ...authFields, header_name: e.target.value })}
              placeholder={t("openApiFormSection.headerNamePlaceholder")}
              data-testid="openapi-header-name-input"
            />
          </FormField>
          <FormField section="authentication" size="long">
            <PasswordInput
              required
              label={t("openApiFormSection.apiKeyLabel")}
              value={authFields.api_key ?? ""}
              onChange={(e) => setAuthFields({ ...authFields, api_key: e.target.value })}
              placeholder={t("openApiFormSection.apiKeyPlaceholder")}
              data-testid="openapi-api-key-input"
            />
          </FormField>
        </>
      )}
      <FormField size="full">
        <Group gap="sm" align="center">
          <Button
            type="button"
            onClick={onOpenapiPreview}
            loading={openapiPreviewing}
            disabled={openapiSpecMode === "path" ? !openapiSpecPath : !openapiSpecInline}
            data-testid="openapi-preview-button"
          >
            {openapiPreviewing
              ? t("openApiFormSection.previewLoading")
              : t("openApiFormSection.previewButton")}
          </Button>
          {openapiPreviewError && (
            <Alert color="red" variant="light" py={4} px="sm" data-testid="openapi-preview-error">
              {openapiPreviewError}
            </Alert>
          )}
        </Group>
      </FormField>
      {openapiPreview && (
        <FormField size="full">
          <Text size="sm" c="dimmed" data-testid="openapi-preview-summary">
            <Text component="strong" size="sm">
              {t("openApiFormSection.queriesCount", { count: openapiPreview.queries.length })}
            </Text>
            {": "}
            {openapiPreview.queries.map((q) => q.operation_id).join(", ") ||
              t("openApiFormSection.none")}
            <br />
            <Text component="strong" size="sm">
              {t("openApiFormSection.mutationsCount", { count: openapiPreview.mutations.length })}
            </Text>
            {": "}
            {openapiPreview.mutations.map((m) => m.operation_id).join(", ") ||
              t("openApiFormSection.none")}
          </Text>
        </FormField>
      )}
    </>
  );
}
