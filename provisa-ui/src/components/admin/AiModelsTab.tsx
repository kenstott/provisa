// Copyright (c) 2026 Kenneth Stott
// Canary: 1b363436-3b0b-482b-90bb-59eecca7d20d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Alert,
  Button,
  Checkbox,
  Group,
  Loader,
  NumberInput,
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

  useEffect(() => {
    fetchAiModels()
      .then(setS)
      .catch((e) => setError(String(e)));
  }, []);

  const save = async () => {
    if (!s) return;
    setSaving(true);
    setMsg("");
    setError("");
    try {
      // Only string-form assignments are editable here; dict-form roles are left untouched.
      const ai_models: Partial<Record<keyof AiModelAssignments, string>> = {};
      for (const k of ROLE_KEYS) {
        const v = s.ai_models[k];
        if (typeof v === "string") ai_models[k] = v;
      }
      const res = await setAiModels({
        ai_models,
        vector_models: s.vector_models,
        nl: s.nl,
      });
      setMsg(
        res.restart_required ? t("aiModelsTab.savedRestartRequired") : t("aiModelsTab.saved"),
      );
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  const setRole = (k: keyof AiModelAssignments, value: string) =>
    setS((prev) => (prev ? { ...prev, ai_models: { ...prev.ai_models, [k]: value } } : prev));

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
      <Title order={4}>{t("aiModelsTab.modelsHeading")}</Title>
      <Text c="dimmed" size="sm">
        {t("aiModelsTab.modelsIntro")}
      </Text>

      <Stack gap="sm">
        {ROLE_KEYS.map((k) => {
          const v = s.ai_models[k];
          if (typeof v !== "string") {
            return (
              <TextInput
                key={k}
                label={t(`aiModelsTab.role_${k}`)}
                data-testid={`ai-model-${k}`}
                value={JSON.stringify(v)}
                disabled
                description={t("aiModelsTab.dictNotEditable")}
              />
            );
          }
          return (
            <TextInput
              key={k}
              label={t(`aiModelsTab.role_${k}`)}
              data-testid={`ai-model-${k}`}
              value={v}
              onChange={(e) => setRole(k, e.currentTarget.value)}
            />
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
