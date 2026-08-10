// Copyright (c) 2026 Kenneth Stott
// Canary: 1b363436-3b0b-482b-90bb-59eecca7d20d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Alert,
  Autocomplete,
  Badge,
  Button,
  Checkbox,
  Group,
  Loader,
  NumberInput,
  PasswordInput,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { Check, Plus, Trash2, TriangleAlert } from "lucide-react";
import {
  fetchAiModels,
  fetchVendorModels,
  LLM_VENDORS,
  setAiModels,
  type AiModelAssignments,
  type AiModelsState,
  type VectorModel,
} from "../../api/aiModels";

// REQ-464, REQ-419, REQ-500, REQ-370: configure per-operation AI model assignments, the
// embedding-model registry, and the NL rate limit. All bind at startup — changes apply on restart.
const ROLE_KEYS: (keyof AiModelAssignments)[] = [
  "table_description",
  "column_description",
  "relationship_inference",
  "sql_generation",
  "table_selection",
];

const PROVIDER_OPTIONS = [
  { value: "openai", label: "openai" },
  { value: "ollama", label: "ollama" },
  { value: "huggingface", label: "huggingface" },
];

// REQ-1398: suggestions for the per-operation vendor field. LLM_VENDORS take an org API key;
// ollama/lmstudio are local, keyless endpoints — both are valid `vendor` values for
// ai_models.<op>, so the field is a free-text autocomplete, not a closed Select.
const OPERATION_VENDOR_OPTIONS = [...LLM_VENDORS, "ollama", "lmstudio"];

function roleVendor(v: string | Record<string, unknown>): string {
  if (typeof v === "string") return "anthropic";
  return typeof v.vendor === "string" ? v.vendor : "anthropic";
}

function roleModel(v: string | Record<string, unknown>): string {
  if (typeof v === "string") return v;
  return typeof v.model === "string" ? v.model : "";
}

// Roundtrips to the plain string form when nothing but vendor=anthropic/model is set, so a role
// left at its default still saves as a string (matches the blank-clears-override behavior the
// backend already implements for the string form).
function serializeRole(v: string | Record<string, unknown>): string | Record<string, unknown> {
  if (typeof v === "string") return v;
  const { vendor, model, ...rest } = v;
  if (Object.keys(rest).length === 0 && (vendor === "anthropic" || !vendor)) {
    return typeof model === "string" ? model : "";
  }
  return v;
}

const EMPTY_VECTOR_MODEL: VectorModel = {
  id: "",
  provider: "openai",
  dimensions: 1536,
  api_key_env: null,
  base_url: null,
  enabled: true,
};

export function AiModelsTab() {
  const { t } = useTranslation();
  const [s, setS] = useState<AiModelsState | null>(null);
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");
  const [error, setError] = useState("");
  // REQ-1395, REQ-1398: write-only per vendor — never populated from the fetched state, only
  // sent on save.
  const [apiKeyInputs, setApiKeyInputs] = useState<Record<string, string>>({});
  const [clearApiKeys, setClearApiKeys] = useState<Record<string, boolean>>({});

  // REQ-1409: the model picker's options, read live from each vendor's list-models API and kept
  // per vendor so switching a role's vendor swaps its catalog without refetching the others.
  const [vendorModels, setVendorModels] = useState<Record<string, string[]>>({});
  const [vendorModelErrors, setVendorModelErrors] = useState<Record<string, string>>({});
  const [loadingVendors, setLoadingVendors] = useState<Record<string, boolean>>({});
  const requestedVendors = useRef<Set<string>>(new Set());

  useEffect(() => {
    fetchAiModels()
      .then(setS)
      .catch((e) => setError(String(e)));
  }, []);

  // Vendors named by the five roles right now. Sorted+joined so the effect re-runs only when the
  // SET changes, not on every keystroke in a model field.
  const activeVendors = s
    ? [...new Set(ROLE_KEYS.map((k) => roleVendor(s.ai_models[k])))].sort().join(",")
    : "";

  useEffect(() => {
    for (const vendor of activeVendors.split(",").filter(Boolean)) {
      // The ref, not the state map, guards the fetch: state lands a render later, so two roles
      // sharing a vendor would each start a request before either result arrived.
      if (requestedVendors.current.has(vendor)) continue;
      requestedVendors.current.add(vendor);
      setLoadingVendors((p) => ({ ...p, [vendor]: true }));
      fetchVendorModels(vendor)
        .then((models) => setVendorModels((p) => ({ ...p, [vendor]: models })))
        .catch((e: unknown) => setVendorModelErrors((p) => ({ ...p, [vendor]: String(e) })))
        .finally(() => setLoadingVendors((p) => ({ ...p, [vendor]: false })));
    }
  }, [activeVendors]);

  const save = async () => {
    if (!s) return;
    setSaving(true);
    setMsg("");
    setError("");
    try {
      const ai_models: Partial<Record<keyof AiModelAssignments, string | Record<string, unknown>>> =
        {};
      for (const k of ROLE_KEYS) {
        ai_models[k] = serializeRole(s.ai_models[k]);
      }
      const api_keys: Record<string, string> = {};
      for (const vendor of LLM_VENDORS) {
        const val = apiKeyInputs[vendor]?.trim();
        if (val || clearApiKeys[vendor]) api_keys[vendor] = val ?? "";
      }
      const res = await setAiModels({
        ai_models,
        vector_models: s.vector_models,
        nl: s.nl,
        ...(Object.keys(api_keys).length ? { api_keys } : {}),
      });
      setMsg(
        res.restart_required ? t("aiModelsTab.savedRestartRequired") : t("aiModelsTab.saved"),
      );
      setS((prev) => {
        if (!prev) return prev;
        const api_keys_set = { ...prev.api_keys_set };
        for (const vendor of Object.keys(api_keys)) {
          api_keys_set[vendor] = !!api_keys[vendor];
        }
        return { ...prev, api_keys_set };
      });
      setApiKeyInputs({});
      setClearApiKeys({});
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  const setRoleVendor = (k: keyof AiModelAssignments, vendor: string) =>
    setS((prev) => {
      if (!prev) return prev;
      const v = prev.ai_models[k];
      const rest = typeof v === "string" ? {} : v;
      return {
        ...prev,
        ai_models: { ...prev.ai_models, [k]: { ...rest, vendor, model: roleModel(v) } },
      };
    });

  const setRoleModel = (k: keyof AiModelAssignments, model: string) =>
    setS((prev) => {
      if (!prev) return prev;
      const v = prev.ai_models[k];
      const rest = typeof v === "string" ? {} : v;
      return {
        ...prev,
        ai_models: { ...prev.ai_models, [k]: { ...rest, vendor: roleVendor(v), model } },
      };
    });

  const setVector = (i: number, patch: Partial<VectorModel>) =>
    setS((prev) =>
      prev
        ? {
            ...prev,
            vector_models: prev.vector_models.map((vm, idx) =>
              idx === i ? { ...vm, ...patch } : vm,
            ),
          }
        : prev,
    );

  const addVector = () =>
    setS((prev) =>
      prev ? { ...prev, vector_models: [...prev.vector_models, { ...EMPTY_VECTOR_MODEL }] } : prev,
    );

  const removeVector = (i: number) =>
    setS((prev) =>
      prev ? { ...prev, vector_models: prev.vector_models.filter((_, idx) => idx !== i) } : prev,
    );

  if (error && !s) return <Alert color="red">{error}</Alert>;
  if (!s)
    return (
      <Group gap="xs">
        <Loader size="sm" />
        <Text>{t("aiModelsTab.loading")}</Text>
      </Group>
    );

  return (
    <Stack maw={860} gap="md">
      <Title order={4}>{t("aiModelsTab.apiKeysHeading")}</Title>
      <Text c="dimmed" size="sm">
        {t("aiModelsTab.apiKeysIntro")}
      </Text>
      <Stack gap="sm">
        {LLM_VENDORS.map((vendor) => {
          const isSet = !!s.api_keys_set[vendor];
          const clearing = !!clearApiKeys[vendor];
          return (
            <Stack key={vendor} gap={4}>
              <Group gap="xs" align="center">
                <Text size="sm" fw={500} tt="capitalize">
                  {vendor}
                </Text>
                <Badge
                  color={isSet ? "green" : "gray"}
                  data-testid={`ai-models-${vendor}-key-status`}
                >
                  {isSet ? t("aiModelsTab.apiKeySet") : t("aiModelsTab.apiKeyNotSet")}
                </Badge>
              </Group>
              <Group gap="sm" align="flex-end">
                <PasswordInput
                  aria-label={t("aiModelsTab.apiKeyLabel", { vendor })}
                  placeholder={t("aiModelsTab.apiKeyPlaceholder")}
                  data-testid={`ai-models-${vendor}-key-input`}
                  value={apiKeyInputs[vendor] ?? ""}
                  onChange={(e) => {
                    const value = e.currentTarget.value;
                    setApiKeyInputs((prev) => ({ ...prev, [vendor]: value }));
                    setClearApiKeys((prev) => ({ ...prev, [vendor]: false }));
                  }}
                  style={{ flex: 1 }}
                />
                {isSet && (
                  <Button
                    variant="subtle"
                    color="red"
                    data-testid={`ai-models-${vendor}-key-clear`}
                    onClick={() => {
                      setApiKeyInputs((prev) => ({ ...prev, [vendor]: "" }));
                      setClearApiKeys((prev) => ({ ...prev, [vendor]: true }));
                    }}
                  >
                    {t("aiModelsTab.apiKeyClear")}
                  </Button>
                )}
              </Group>
              {clearing && (
                <Text c="orange" size="sm">
                  {t("aiModelsTab.apiKeyWillClear")}
                </Text>
              )}
            </Stack>
          );
        })}
      </Stack>

      <Title order={4}>{t("aiModelsTab.modelsHeading")}</Title>
      <Text c="dimmed" size="sm">
        {t("aiModelsTab.modelsIntro")}
      </Text>

      <Stack gap="sm">
        {ROLE_KEYS.map((k) => {
          const v = s.ai_models[k];
          const vendor = roleVendor(v);
          const model = roleModel(v);
          return (
            <Stack key={k} gap={4}>
              <Text size="sm" fw={500}>
                {t(`aiModelsTab.role_${k}`)}
              </Text>
              <Group gap="sm" align="flex-end">
                <Autocomplete
                  label={t("aiModelsTab.vendorLabel")}
                  data-testid={`ai-model-${k}-vendor`}
                  data={OPERATION_VENDOR_OPTIONS}
                  value={vendor}
                  onChange={(val) => setRoleVendor(k, val)}
                  style={{ width: 200 }}
                />
                <Autocomplete
                  label={t("aiModelsTab.modelLabel")}
                  data-testid={`ai-model-${k}`}
                  data={vendorModels[vendor] ?? []}
                  value={model}
                  onChange={(val) => setRoleModel(k, val)}
                  rightSection={loadingVendors[vendor] ? <Loader size="xs" /> : undefined}
                  // A vendor that publishes no listing, or has no key yet, leaves the field a
                  // plain typed input — the reason is stated under it rather than marking the
                  // value invalid, since typing a model name by hand stays valid.
                  description={vendorModelErrors[vendor]}
                  style={{ flex: 1 }}
                />
              </Group>
            </Stack>
          );
        })}
      </Stack>

      <Title order={4}>{t("aiModelsTab.nlHeading")}</Title>
      <Text c="dimmed" size="sm">
        {t("aiModelsTab.nlIntro")}
      </Text>
      <NumberInput
        label={t("aiModelsTab.rateLimitLabel")}
        data-testid="ai-models-rate-limit"
        value={s.nl.rate_limit ?? ""}
        onChange={(val) =>
          setS({ ...s, nl: { rate_limit: val === "" ? null : Number(val) } })
        }
      />

      <Title order={4}>{t("aiModelsTab.vectorHeading")}</Title>
      <Text c="dimmed" size="sm">
        {t("aiModelsTab.vectorIntro")}
      </Text>
      <Table data-testid="ai-models-vector-table">
        <Table.Thead>
          <Table.Tr>
            <Table.Th>{t("aiModelsTab.vectorId")}</Table.Th>
            <Table.Th>{t("aiModelsTab.vectorProvider")}</Table.Th>
            <Table.Th>{t("aiModelsTab.vectorDimensions")}</Table.Th>
            <Table.Th>{t("aiModelsTab.vectorApiKeyEnv")}</Table.Th>
            <Table.Th>{t("aiModelsTab.vectorBaseUrl")}</Table.Th>
            <Table.Th>{t("aiModelsTab.vectorEnabled")}</Table.Th>
            <Table.Th />
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {s.vector_models.map((vm, i) => (
            <Table.Tr key={i}>
              <Table.Td>
                <TextInput
                  aria-label={t("aiModelsTab.vectorId")}
                  value={vm.id}
                  onChange={(e) => setVector(i, { id: e.currentTarget.value })}
                />
              </Table.Td>
              <Table.Td>
                <Select
                  aria-label={t("aiModelsTab.vectorProvider")}
                  data={PROVIDER_OPTIONS}
                  value={vm.provider}
                  onChange={(val) => setVector(i, { provider: val ?? "openai" })}
                />
              </Table.Td>
              <Table.Td>
                <NumberInput
                  aria-label={t("aiModelsTab.vectorDimensions")}
                  value={vm.dimensions}
                  onChange={(val) => setVector(i, { dimensions: Number(val) })}
                />
              </Table.Td>
              <Table.Td>
                <TextInput
                  aria-label={t("aiModelsTab.vectorApiKeyEnv")}
                  value={vm.api_key_env ?? ""}
                  onChange={(e) =>
                    setVector(i, { api_key_env: e.currentTarget.value || null })
                  }
                />
              </Table.Td>
              <Table.Td>
                <TextInput
                  aria-label={t("aiModelsTab.vectorBaseUrl")}
                  value={vm.base_url ?? ""}
                  onChange={(e) => setVector(i, { base_url: e.currentTarget.value || null })}
                />
              </Table.Td>
              <Table.Td>
                <Checkbox
                  aria-label={t("aiModelsTab.vectorEnabled")}
                  checked={vm.enabled}
                  onChange={(e) => setVector(i, { enabled: e.currentTarget.checked })}
                />
              </Table.Td>
              <Table.Td>
                <ActionIcon
                  color="red"
                  variant="subtle"
                  aria-label={t("aiModelsTab.removeVectorModel")}
                  onClick={() => removeVector(i)}
                >
                  <Trash2 size={16} />
                </ActionIcon>
              </Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
      <Group>
        <Button
          variant="light"
          leftSection={<Plus size={14} />}
          onClick={addVector}
          data-testid="ai-models-add-vector"
        >
          {t("aiModelsTab.addVectorModel")}
        </Button>
      </Group>

      <Alert color="yellow" icon={<TriangleAlert size={16} />}>
        {s.restart_required_note}
      </Alert>

      <Group gap="sm" align="center">
        <Button
          onClick={save}
          disabled={saving}
          loading={saving}
          title={t("aiModelsTab.saveButtonLabel")}
          aria-label={t("aiModelsTab.saveButtonLabel")}
          data-testid="ai-models-save"
          leftSection={saving ? undefined : <Check size={14} />}
        >
          {t("aiModelsTab.saveButtonLabel")}
        </Button>
        {msg && (
          <Text c="green" size="sm">
            {msg}
          </Text>
        )}
        {error && (
          <Text c="red" size="sm">
            {error}
          </Text>
        )}
      </Group>
    </Stack>
  );
}
