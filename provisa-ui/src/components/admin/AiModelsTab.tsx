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
} from "@mantine/core";
import { Plus, Trash2, TriangleAlert } from "lucide-react";
import {
  fetchAiModels,
  fetchModelLimits,
  fetchVendorModels,
  JEV_KEY,
  LLM_VENDORS,
  setAiModels,
  type AiEndpoint,
  type AiModelAssignments,
  type AiModelsState,
  type ModelLimits,
  type ModelLimitsOverride,
  type ModelLimitsSource,
  type VectorModel,
} from "../../api/aiModels";
import { CollapsibleItem, Collapsibles } from "../collapsible/Collapsible";
import { ListTable, ListRow } from "../list/ListTable";
import { FieldGroup, FormField, FormFooter, FormLayout } from "../form/FormLayout";

// REQ-464, REQ-419, REQ-500, REQ-370, REQ-1790: configure per-operation AI model assignments, the
// embedding-model registry, custom AI endpoints (openai/anthropic style), and the NL rate limit.
const ROLE_KEYS: (keyof AiModelAssignments)[] = [
  "table_description",
  "column_description",
  "relationship_inference",
  "sql_generation",
  "table_selection",
  "mcp_chat",
  "glossary_term_finding",
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

// REQ-464: a model's limits are kept per vendor and model id; this joins the two into one key.
// A newline appears in neither a vendor nor a model id.
function limitsKey(vendor: string, model: string): string {
  return `${vendor}\n${model}`;
}

function flattenLimits(byVendor: AiModelsState["model_limits"] | undefined) {
  const out: Record<string, ModelLimits> = {};
  for (const models of Object.values(byVendor ?? {})) {
    for (const l of Object.values(models)) out[limitsKey(l.vendor, l.model)] = l;
  }
  return out;
}

/** A limit being edited: a whole number, or "" while the field is empty. */
type LimitDraft = { input_tokens: number | ""; output_tokens: number | "" };

/** What a model's limit fields show: the loaded limits with any unsaved edit laid over them. */
function shownLimits(
  loaded: ModelLimits,
  edit: LimitDraft | null | undefined,
): { input: number | ""; output: number | ""; source: ModelLimitsSource } {
  if (edit === null) {
    return {
      input: loaded.base_input_tokens,
      output: loaded.base_output_tokens,
      source: loaded.base_source,
    };
  }
  if (edit !== undefined) {
    return { input: edit.input_tokens, output: edit.output_tokens, source: "override" };
  }
  return { input: loaded.input_tokens, output: loaded.output_tokens, source: loaded.source };
}

/** The digit-group separator of the page's language, so 32000 reads as 32,000 (or 32.000). */
function groupSeparator(language: string): string {
  const part = new Intl.NumberFormat(language).formatToParts(10000).find((p) => p.type === "group");
  return part?.value ?? ",";
}

const EMPTY_VECTOR_MODEL: VectorModel = {
  id: "",
  provider: "openai",
  dimensions: 1536,
  api_key_env: null,
  base_url: null,
  enabled: true,
  version: null,
};

const STYLE_OPTIONS = [
  { value: "openai", label: "openai" },
  { value: "anthropic", label: "anthropic" },
];

const EMPTY_AI_ENDPOINT: AiEndpoint = {
  id: "",
  style: "openai",
  base_url: "",
  api_key_env: null,
  enabled: true,
};

/**
 * A section of this tab as an expandable panel — the same separated-accordion shape the other
 * admin sections use, open by default because the assignments are what the tab is for.
 */
function Panel({
  testId,
  title,
  children,
}: {
  testId: string;
  title: string;
  children: React.ReactNode;
}) {
  return (
    <Collapsibles id={`admin.ai-models.${testId}`} testId={testId}>
      <CollapsibleItem value="section" title={title}>
        <Stack gap="md">{children}</Stack>
      </CollapsibleItem>
    </Collapsibles>
  );
}

export function AiModelsTab() {
  const { t, i18n } = useTranslation();
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

  // REQ-464: each assigned model's limits as loaded, and the unsaved edits to them (null: return
  // the model to its published figure, or to the default pair).
  const [limits, setLimits] = useState<Record<string, ModelLimits>>({});
  const [limitEdits, setLimitEdits] = useState<Record<string, LimitDraft | null>>({});
  const [limitErrors, setLimitErrors] = useState<Record<string, string>>({});
  const requestedLimits = useRef<Set<string>>(new Set());

  useEffect(() => {
    fetchAiModels()
      .then((loaded) => {
        setS(loaded);
        const flat = flattenLimits(loaded.model_limits);
        for (const k of Object.keys(flat)) requestedLimits.current.add(k);
        setLimits(flat);
      })
      .catch((e) => setError(String(e)));
  }, []);

  // The vendor/model pairs assigned right now, sorted and joined so the lookup below re-runs only
  // when the set changes.
  const assignedPairs = s
    ? [
        ...new Set(
          ROLE_KEYS.map((k) => {
            const model = roleModel(s.ai_models[k]).trim();
            return model ? limitsKey(roleVendor(s.ai_models[k]), model) : "";
          }).filter(Boolean),
        ),
      ]
        .sort()
        .join("\t")
    : "";

  // A model given to a task here, not yet saved, has its limits looked up once typing pauses.
  useEffect(() => {
    const missing = assignedPairs.split("\t").filter((k) => k && !requestedLimits.current.has(k));
    if (missing.length === 0) return;
    const timer = setTimeout(() => {
      for (const key of missing) {
        if (requestedLimits.current.has(key)) continue;
        requestedLimits.current.add(key);
        const [vendor, model] = key.split("\n");
        fetchModelLimits(vendor, model)
          .then((l) => setLimits((p) => ({ ...p, [key]: l })))
          .catch((e: unknown) => setLimitErrors((p) => ({ ...p, [key]: String(e) })));
      }
    }, 400);
    return () => clearTimeout(timer);
  }, [assignedPairs]);

  const editLimit = (key: string, field: keyof LimitDraft, value: number | "") => {
    const loaded = limits[key];
    if (!loaded) return;
    setLimitEdits((prev) => {
      const now = shownLimits(loaded, prev[key]);
      const draft: LimitDraft = { input_tokens: now.input, output_tokens: now.output };
      return { ...prev, [key]: { ...draft, [field]: value } };
    });
  };

  const resetLimit = (key: string) => {
    const loaded = limits[key];
    if (!loaded) return;
    setLimitEdits((prev) => {
      const next = { ...prev };
      // Unsaved figures over a saved override return to the published figure on save; unsaved
      // figures over a model's own published figure are simply dropped.
      if (loaded.source === "override") next[key] = null;
      else delete next[key];
      return next;
    });
  };

  const separator = groupSeparator(i18n.language);

  // Vendors named by the five roles right now. Sorted+joined so the effect re-runs only when the
  // SET changes, not on every keystroke in a model field.
  const activeVendors = s
    ? [...new Set(ROLE_KEYS.map((k) => roleVendor(s.ai_models[k])))].sort().join(",")
    : "";

  // REQ-1790: a configured ai_endpoints id is a valid `vendor` for a role assignment too — offer
  // it in the same picker alongside the named vendors and local keyless ones.
  const vendorOptions = s
    ? [...OPERATION_VENDOR_OPTIONS, ...s.ai_endpoints.map((ep) => ep.id).filter(Boolean)]
    : OPERATION_VENDOR_OPTIONS;

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
    // REQ-464: each edited model's limits, or null where an override is cleared.
    const model_limits: Record<string, Record<string, ModelLimitsOverride | null>> = {};
    for (const [key, edit] of Object.entries(limitEdits)) {
      const [vendor, model] = key.split("\n");
      if (edit !== null) {
        const { input_tokens, output_tokens } = edit;
        if (input_tokens === "" || output_tokens === "" || input_tokens < 1 || output_tokens < 1) {
          setError(t("aiModelsTab.limitsIncomplete", { model }));
          setSaving(false);
          return;
        }
        (model_limits[vendor] ??= {})[model] = { input_tokens, output_tokens };
      } else {
        (model_limits[vendor] ??= {})[model] = null;
      }
    }
    try {
      const ai_models: Partial<Record<keyof AiModelAssignments, string | Record<string, unknown>>> =
        {};
      for (const k of ROLE_KEYS) {
        ai_models[k] = serializeRole(s.ai_models[k]);
      }
      const api_keys: Record<string, string> = {};
      for (const vendor of [...LLM_VENDORS, JEV_KEY]) {
        const val = apiKeyInputs[vendor]?.trim();
        if (val || clearApiKeys[vendor]) api_keys[vendor] = val ?? "";
      }
      const res = await setAiModels({
        ai_models,
        vector_models: s.vector_models,
        ai_endpoints: s.ai_endpoints,
        nl: s.nl,
        ...(Object.keys(model_limits).length ? { model_limits } : {}),
        ...(Object.keys(api_keys).length ? { api_keys } : {}),
      });
      setLimits((prev) => {
        const next = { ...prev };
        for (const [key, edit] of Object.entries(limitEdits)) {
          const loaded = next[key];
          if (!loaded) continue;
          next[key] =
            edit === null
              ? {
                  ...loaded,
                  input_tokens: loaded.base_input_tokens,
                  output_tokens: loaded.base_output_tokens,
                  source: loaded.base_source,
                }
              : {
                  ...loaded,
                  input_tokens: Number(edit.input_tokens),
                  output_tokens: Number(edit.output_tokens),
                  source: "override",
                };
        }
        return next;
      });
      setLimitEdits({});
      setMsg(res.restart_required ? t("aiModelsTab.savedRestartRequired") : t("aiModelsTab.saved"));
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

  const setEndpoint = (i: number, patch: Partial<AiEndpoint>) =>
    setS((prev) =>
      prev
        ? {
            ...prev,
            ai_endpoints: prev.ai_endpoints.map((ep, idx) =>
              idx === i ? { ...ep, ...patch } : ep,
            ),
          }
        : prev,
    );

  const addEndpoint = () =>
    setS((prev) =>
      prev ? { ...prev, ai_endpoints: [...prev.ai_endpoints, { ...EMPTY_AI_ENDPOINT }] } : prev,
    );

  const removeEndpoint = (i: number) =>
    setS((prev) =>
      prev ? { ...prev, ai_endpoints: prev.ai_endpoints.filter((_, idx) => idx !== i) } : prev,
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
    <Stack gap="md">
      <Panel testId="ai-models-keys-panel" title={t("aiModelsTab.apiKeysHeading")}>
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
                <FormLayout>
                  <FieldGroup row style={{ alignItems: "center" }}>
                    <FormField size="long">
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
                      />
                    </FormField>
                    {isSet && (
                      <Button
                        variant="subtle"
                        color="red"
                        data-testid={`ai-models-${vendor}-key-clear`}
                        style={{ marginTop: 0 }}
                        onClick={() => {
                          setApiKeyInputs((prev) => ({ ...prev, [vendor]: "" }));
                          setClearApiKeys((prev) => ({ ...prev, [vendor]: true }));
                        }}
                      >
                        {t("aiModelsTab.apiKeyClear")}
                      </Button>
                    )}
                  </FieldGroup>
                </FormLayout>
                {clearing && (
                  <Text c="orange" size="sm">
                    {t("aiModelsTab.apiKeyWillClear")}
                  </Text>
                )}
              </Stack>
            );
          })}
        </Stack>
      </Panel>

      <Panel testId="ai-models-assignments-panel" title={t("aiModelsTab.modelsHeading")}>
        <Text c="dimmed" size="sm">
          {t("aiModelsTab.modelsIntro")}
        </Text>
        <Text c="dimmed" size="sm">
          {t("aiModelsTab.limitsIntro")}
        </Text>
        <Stack gap="sm">
          {ROLE_KEYS.map((k) => {
            const v = s.ai_models[k];
            const vendor = roleVendor(v);
            const model = roleModel(v);
            const key = limitsKey(vendor, model.trim());
            const loaded = model.trim() ? limits[key] : undefined;
            const shown = loaded ? shownLimits(loaded, limitEdits[key]) : undefined;
            return (
              <Stack key={k} gap={4}>
                <Text size="sm" fw={500}>
                  {t(`aiModelsTab.role_${k}`)}
                </Text>
                <FormLayout>
                  <FieldGroup row>
                    <FormField size="medium">
                      <Autocomplete
                        label={t("aiModelsTab.vendorLabel")}
                        data-testid={`ai-model-${k}-vendor`}
                        data={vendorOptions}
                        value={vendor}
                        onChange={(val) => setRoleVendor(k, val)}
                      />
                    </FormField>
                    <FormField hint={vendorModelErrors[vendor]} size="medium">
                      <Autocomplete
                        label={t("aiModelsTab.modelLabel")}
                        data-testid={`ai-model-${k}`}
                        data={vendorModels[vendor] ?? []}
                        value={model}
                        onChange={(val) => setRoleModel(k, val)}
                        rightSection={loadingVendors[vendor] ? <Loader size="xs" /> : undefined}
                      />
                    </FormField>
                  </FieldGroup>
                  {model.trim() && (
                    <FieldGroup row style={{ alignItems: "flex-end" }}>
                      <FormField size="short">
                        <NumberInput
                          label={t("aiModelsTab.inputLimitLabel")}
                          data-testid={`ai-model-${k}-input-limit`}
                          value={shown?.input ?? ""}
                          disabled={!loaded}
                          min={1}
                          allowDecimal={false}
                          allowNegative={false}
                          thousandSeparator={separator}
                          onChange={(val) =>
                            editLimit(key, "input_tokens", val === "" ? "" : Number(val))
                          }
                        />
                      </FormField>
                      <FormField size="short">
                        <NumberInput
                          label={t("aiModelsTab.outputLimitLabel")}
                          data-testid={`ai-model-${k}-output-limit`}
                          value={shown?.output ?? ""}
                          disabled={!loaded}
                          min={1}
                          allowDecimal={false}
                          allowNegative={false}
                          thousandSeparator={separator}
                          onChange={(val) =>
                            editLimit(key, "output_tokens", val === "" ? "" : Number(val))
                          }
                        />
                      </FormField>
                      {loaded && shown?.source === "override" && (
                        <Button
                          variant="subtle"
                          data-testid={`ai-model-${k}-limits-reset`}
                          onClick={() => resetLimit(key)}
                        >
                          {loaded.base_source === "known"
                            ? t("aiModelsTab.limitsResetKnown")
                            : t("aiModelsTab.limitsResetDefault")}
                        </Button>
                      )}
                    </FieldGroup>
                  )}
                  {model.trim() && (limitErrors[key] || shown?.source === "default") && (
                    <Text c="dimmed" size="xs" data-testid={`ai-model-${k}-limits-note`}>
                      {limitErrors[key] ?? t("aiModelsTab.limitsDefaultNote")}
                    </Text>
                  )}
                </FormLayout>
              </Stack>
            );
          })}
        </Stack>
      </Panel>

      <Panel testId="ai-models-nl-panel" title={t("aiModelsTab.nlHeading")}>
        <Text c="dimmed" size="sm">
          {t("aiModelsTab.nlIntro")}
        </Text>
        <FormLayout>
          <FormField size="short">
            <NumberInput
              label={t("aiModelsTab.rateLimitLabel")}
              data-testid="ai-models-rate-limit"
              value={s.nl.rate_limit ?? ""}
              onChange={(val) =>
                setS({ ...s, nl: { rate_limit: val === "" ? null : Number(val) } })
              }
            />
          </FormField>
        </FormLayout>
      </Panel>

      <Panel testId="ai-models-vector-panel" title={t("aiModelsTab.vectorHeading")}>
        <Text c="dimmed" size="sm">
          {t("aiModelsTab.vectorIntro")}
        </Text>
        <ListTable testId="ai-models-vector-table">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("aiModelsTab.vectorId")}</Table.Th>
              <Table.Th>{t("aiModelsTab.vectorProvider")}</Table.Th>
              <Table.Th>{t("aiModelsTab.vectorDimensions")}</Table.Th>
              <Table.Th>{t("aiModelsTab.vectorApiKeyEnv")}</Table.Th>
              <Table.Th>{t("aiModelsTab.vectorBaseUrl")}</Table.Th>
              <Table.Th>{t("aiModelsTab.vectorVersion")}</Table.Th>
              <Table.Th>{t("aiModelsTab.vectorEnabled")}</Table.Th>
              <Table.Th />
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {s.vector_models.map((vm, i) => (
              <ListRow key={i}>
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
                    placeholder="${secret:my_embedding_key}"
                    value={vm.api_key_env ?? ""}
                    onChange={(e) => setVector(i, { api_key_env: e.currentTarget.value || null })}
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
                  <TextInput
                    aria-label={t("aiModelsTab.vectorVersion")}
                    placeholder={t("aiModelsTab.vectorVersionPlaceholder")}
                    value={vm.version ?? ""}
                    onChange={(e) => setVector(i, { version: e.currentTarget.value || null })}
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
              </ListRow>
            ))}
          </Table.Tbody>
        </ListTable>
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
      </Panel>

      <Panel testId="ai-models-endpoints-panel" title={t("aiModelsTab.endpointsHeading")}>
        <Text c="dimmed" size="sm">
          {t("aiModelsTab.endpointsIntro")}
        </Text>
        <ListTable testId="ai-models-endpoints-table">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("aiModelsTab.endpointId")}</Table.Th>
              <Table.Th>{t("aiModelsTab.endpointStyle")}</Table.Th>
              <Table.Th>{t("aiModelsTab.endpointBaseUrl")}</Table.Th>
              <Table.Th>{t("aiModelsTab.endpointApiKeyEnv")}</Table.Th>
              <Table.Th>{t("aiModelsTab.vectorEnabled")}</Table.Th>
              <Table.Th />
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {s.ai_endpoints.map((ep, i) => (
              <ListRow key={i}>
                <Table.Td>
                  <TextInput
                    aria-label={t("aiModelsTab.endpointId")}
                    data-testid={`ai-models-endpoint-${i}-id`}
                    placeholder="openrouter"
                    value={ep.id}
                    onChange={(e) => setEndpoint(i, { id: e.currentTarget.value })}
                  />
                </Table.Td>
                <Table.Td>
                  <Select
                    aria-label={t("aiModelsTab.endpointStyle")}
                    data-testid={`ai-models-endpoint-${i}-style`}
                    data={STYLE_OPTIONS}
                    value={ep.style}
                    onChange={(val) =>
                      setEndpoint(i, { style: (val as "openai" | "anthropic") ?? "openai" })
                    }
                    allowDeselect={false}
                  />
                </Table.Td>
                <Table.Td>
                  <TextInput
                    aria-label={t("aiModelsTab.endpointBaseUrl")}
                    data-testid={`ai-models-endpoint-${i}-base-url`}
                    placeholder="https://openrouter.ai/api/v1"
                    value={ep.base_url}
                    onChange={(e) => setEndpoint(i, { base_url: e.currentTarget.value })}
                  />
                </Table.Td>
                <Table.Td>
                  <TextInput
                    aria-label={t("aiModelsTab.endpointApiKeyEnv")}
                    data-testid={`ai-models-endpoint-${i}-key-env`}
                    placeholder="${secret:openrouter_key}"
                    value={ep.api_key_env ?? ""}
                    onChange={(e) => setEndpoint(i, { api_key_env: e.currentTarget.value || null })}
                  />
                </Table.Td>
                <Table.Td>
                  <Checkbox
                    aria-label={t("aiModelsTab.vectorEnabled")}
                    checked={ep.enabled}
                    onChange={(e) => setEndpoint(i, { enabled: e.currentTarget.checked })}
                  />
                </Table.Td>
                <Table.Td>
                  <ActionIcon
                    color="red"
                    variant="subtle"
                    aria-label={t("aiModelsTab.removeEndpoint")}
                    onClick={() => removeEndpoint(i)}
                  >
                    <Trash2 size={16} />
                  </ActionIcon>
                </Table.Td>
              </ListRow>
            ))}
          </Table.Tbody>
        </ListTable>
        <Group>
          <Button
            variant="light"
            leftSection={<Plus size={14} />}
            onClick={addEndpoint}
            data-testid="ai-models-add-endpoint"
          >
            {t("aiModelsTab.addEndpoint")}
          </Button>
        </Group>
      </Panel>

      <Panel testId="ai-models-jev-panel" title={t("aiModelsTab.jevHeading")}>
        <Text c="dimmed" size="sm">
          {t("aiModelsTab.jevIntro")}
        </Text>
        <Stack gap={4}>
          <Group gap="xs" align="center">
            <Text size="sm" fw={500}>
              Jev
            </Text>
            <Badge
              color={s.api_keys_set[JEV_KEY] ? "green" : "gray"}
              data-testid="ai-models-jev-key-status"
            >
              {s.api_keys_set[JEV_KEY] ? t("aiModelsTab.apiKeySet") : t("aiModelsTab.apiKeyNotSet")}
            </Badge>
          </Group>
          <FormLayout>
            <FieldGroup row>
              <FormField size="long">
                <PasswordInput
                  aria-label={t("aiModelsTab.apiKeyLabel", { vendor: "Jev" })}
                  placeholder={t("aiModelsTab.apiKeyPlaceholder")}
                  data-testid="ai-models-jev-key-input"
                  value={apiKeyInputs[JEV_KEY] ?? ""}
                  onChange={(e) => {
                    const value = e.currentTarget.value;
                    setApiKeyInputs((prev) => ({ ...prev, [JEV_KEY]: value }));
                    setClearApiKeys((prev) => ({ ...prev, [JEV_KEY]: false }));
                  }}
                />
              </FormField>
              {s.api_keys_set[JEV_KEY] && (
                <Button
                  variant="subtle"
                  color="red"
                  data-testid="ai-models-jev-key-clear"
                  onClick={() => {
                    setApiKeyInputs((prev) => ({ ...prev, [JEV_KEY]: "" }));
                    setClearApiKeys((prev) => ({ ...prev, [JEV_KEY]: true }));
                  }}
                >
                  {t("aiModelsTab.apiKeyClear")}
                </Button>
              )}
            </FieldGroup>
          </FormLayout>
          {clearApiKeys[JEV_KEY] && (
            <Text c="orange" size="sm">
              {t("aiModelsTab.apiKeyWillClear")}
            </Text>
          )}
        </Stack>
      </Panel>

      <Alert color="yellow" icon={<TriangleAlert size={16} />}>
        {s.restart_required_note}
      </Alert>

      <FormFooter onSave={save} saving={saving} saveProps={{ "data-testid": "ai-models-save" }}>
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
      </FormFooter>
    </Stack>
  );
}
