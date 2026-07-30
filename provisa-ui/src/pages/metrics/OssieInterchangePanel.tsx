// Copyright (c) 2026 Kenneth Stott
// Canary: 5f1c8a7d-2b3e-4c9a-8d6f-7e0a1b4c5d2e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1316: semantic interchange (Ossie) surface in the Model area — copyable
// endpoint URL + download of the live document, and an upload flow that lands
// as a review screen of proposed tables/relationships/metrics which the modeler
// accepts or trims BEFORE anything registers. Imported definitions never bypass
// registration review: Apply routes checked items through the existing
// registration mutations only.

import { useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Badge,
  Button,
  Card,
  Checkbox,
  Code,
  Divider,
  Group,
  Modal,
  Stack,
  Text,
  Title,
} from "@mantine/core";
import { Download, Upload } from "lucide-react";
import { CopyButton } from "../../components/CopyButton";
import {
  OSSIE_ENDPOINT_PATH,
  fetchOssieYaml,
  importOssie,
  type OssieImportProposals,
} from "../../api/admin";
import {
  useRegisterTable,
  useUpsertMetric,
  useUpsertRelationship,
} from "../../hooks/useAdminQueries";
import type { MutationResult } from "../../types/admin";

type ItemKey = `table:${string}` | `rel:${string}` | `metric:${string}`;

export function OssieInterchangePanel() {
  const { t } = useTranslation();
  const { registerTable } = useRegisterTable();
  const { upsertRelationship } = useUpsertRelationship();
  const { upsertMetric } = useUpsertMetric();
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [error, setError] = useState("");
  const [importing, setImporting] = useState(false);
  const [proposals, setProposals] = useState<OssieImportProposals | null>(null);
  const [checked, setChecked] = useState<Set<ItemKey>>(new Set());
  const [applying, setApplying] = useState(false);
  const [results, setResults] = useState<Record<string, MutationResult>>({});

  const endpointUrl = `${window.location.origin}${OSSIE_ENDPOINT_PATH}`;

  const handleDownload = async () => {
    setError("");
    try {
      const yaml = await fetchOssieYaml();
      const blob = new Blob([yaml], { type: "text/yaml" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "provisa.ossie.yaml";
      a.click();
      URL.revokeObjectURL(url);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleFileChosen = async (file: File) => {
    setError("");
    setImporting(true);
    try {
      const body = await file.text();
      const parsed = await importOssie(body);
      // Review screen opens with everything checked; trimming = unchecking.
      const all = new Set<ItemKey>();
      for (const tbl of parsed.tables) all.add(`table:${tbl.name}`);
      for (const rel of parsed.relationships) all.add(`rel:${rel.name}`);
      for (const m of parsed.metrics) all.add(`metric:${m.name}`);
      setChecked(all);
      setResults({});
      setProposals(parsed);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setImporting(false);
    }
  };

  const toggle = (key: ItemKey) =>
    setChecked((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });

  const handleApply = async () => {
    if (!proposals) return;
    setApplying(true);
    const outcome: Record<string, MutationResult> = {};
    // Tables first, then relationships (which reference tables), then metrics —
    // each through the EXISTING registration mutations (REQ-1316).
    for (const tbl of proposals.tables) {
      const key: ItemKey = `table:${tbl.name}`;
      if (!checked.has(key)) continue;
      outcome[key] = await registerTable({
        sourceId: tbl.source_id,
        domainId: proposals.model_name,
        schemaName: tbl.schema_name,
        tableName: tbl.table_name,
        alias: tbl.name !== tbl.table_name ? tbl.name : undefined,
        description: tbl.description ?? undefined,
        columns: tbl.columns.map((c) => ({
          name: c.name,
          visibleTo: ["*"],
          dataType: c.datatype ?? undefined,
          description: c.description ?? undefined,
          isPrimaryKey: c.is_primary_key,
        })),
        uniqueConstraints: tbl.unique_keys.map((cols, i) => ({
          name: `${tbl.table_name}_uq_${i + 1}`,
          columns: cols,
        })),
        modelingRole: tbl.modeling_role ?? undefined,
        modelingHistory: tbl.modeling_history ?? undefined,
      });
    }
    for (const rel of proposals.relationships) {
      const key: ItemKey = `rel:${rel.name}`;
      if (!checked.has(key)) continue;
      outcome[key] = await upsertRelationship({
        id: rel.name,
        sourceTableId: rel.from,
        targetTableId: rel.to,
        sourceColumn: rel.from_columns[0] ?? "",
        targetColumn: rel.to_columns[0] ?? "",
        cardinality: "many-to-one",
      });
    }
    for (const m of proposals.metrics) {
      const key: ItemKey = `metric:${m.name}`;
      if (!checked.has(key)) continue;
      outcome[key] = await upsertMetric({
        name: m.name,
        expression: m.expression,
        datatype: m.datatype,
        description: m.description,
        aiContext: m.ai_context,
      });
    }
    setResults(outcome);
    setApplying(false);
  };

  const resultBadge = (key: string) => {
    const r = results[key];
    if (!r) return null;
    return r.success ? (
      <Badge size="xs" color="green" data-testid={`ossie-result-${key}`}>
        {t("ossiePanel.applied")}
      </Badge>
    ) : (
      <Badge size="xs" color="red" title={r.message} data-testid={`ossie-result-${key}`}>
        {r.message || t("ossiePanel.failed")}
      </Badge>
    );
  };

  return (
    <Card withBorder padding="md" mb="md" data-testid="ossie-panel">
      <Group justify="space-between" wrap="wrap" gap="sm">
        <div>
          <Title order={5}>{t("ossiePanel.title")}</Title>
          <Group gap="0.35rem" mt={4}>
            <Text size="xs" c="dimmed">
              {t("ossiePanel.endpointLabel")}
            </Text>
            <Code data-testid="ossie-endpoint-url">{endpointUrl}</Code>
            <CopyButton text={endpointUrl} title={t("ossiePanel.copyEndpoint")} />
          </Group>
        </div>
        <Group gap="sm">
          <Button
            size="xs"
            variant="default"
            leftSection={<Download size={13} />}
            onClick={handleDownload}
            data-testid="ossie-download"
          >
            {t("ossiePanel.download")}
          </Button>
          <Button
            size="xs"
            variant="default"
            leftSection={<Upload size={13} />}
            loading={importing}
            onClick={() => fileInputRef.current?.click()}
            data-testid="ossie-import"
          >
            {t("ossiePanel.import")}
          </Button>
          <input
            ref={fileInputRef}
            type="file"
            accept=".yaml,.yml,.json,text/yaml,application/json"
            style={{ display: "none" }}
            data-testid="ossie-file-input"
            onChange={(e) => {
              const file = e.target.files?.[0];
              e.target.value = "";
              if (file) void handleFileChosen(file);
            }}
          />
        </Group>
      </Group>
      {error && (
        <Alert color="red" mt="sm" withCloseButton onClose={() => setError("")} data-testid="ossie-error">
          {error}
        </Alert>
      )}

      {/* REQ-1316: registration review — the modeler accepts or trims the proposals. */}
      <Modal
        opened={proposals !== null}
        onClose={() => setProposals(null)}
        title={
          <Title order={4}>
            {t("ossiePanel.reviewTitle", { model: proposals?.model_name ?? "" })}
          </Title>
        }
        size="xl"
        data-testid="ossie-review-modal"
      >
        {proposals && (
          <Stack gap="sm">
            <Text size="sm" c="dimmed">
              {t("ossiePanel.reviewHint")}
            </Text>
            <Divider label={t("ossiePanel.tablesSection", { count: proposals.tables.length })} />
            {proposals.tables.map((tbl) => {
              const key: ItemKey = `table:${tbl.name}`;
              return (
                <Group key={key} gap="sm" wrap="nowrap">
                  <Checkbox
                    checked={checked.has(key)}
                    onChange={() => toggle(key)}
                    data-testid={`ossie-check-${key}`}
                    label={
                      <Text size="sm" ff="monospace">
                        {tbl.name}{" "}
                        <Text span size="xs" c="dimmed">
                          {tbl.source_id}.{tbl.schema_name}.{tbl.table_name} ·{" "}
                          {t("ossiePanel.columnCount", { count: tbl.columns.length })}
                          {tbl.modeling_role ? ` · ${tbl.modeling_role}` : ""}
                        </Text>
                      </Text>
                    }
                  />
                  {resultBadge(key)}
                </Group>
              );
            })}
            <Divider
              label={t("ossiePanel.relationshipsSection", {
                count: proposals.relationships.length,
              })}
            />
            {proposals.relationships.map((rel) => {
              const key: ItemKey = `rel:${rel.name}`;
              return (
                <Group key={key} gap="sm" wrap="nowrap">
                  <Checkbox
                    checked={checked.has(key)}
                    onChange={() => toggle(key)}
                    data-testid={`ossie-check-${key}`}
                    label={
                      <Text size="sm" ff="monospace">
                        {rel.name}{" "}
                        <Text span size="xs" c="dimmed">
                          {rel.from}.{rel.from_columns.join(",")} → {rel.to}.
                          {rel.to_columns.join(",")}
                        </Text>
                      </Text>
                    }
                  />
                  {resultBadge(key)}
                </Group>
              );
            })}
            <Divider label={t("ossiePanel.metricsSection", { count: proposals.metrics.length })} />
            {proposals.metrics.map((m) => {
              const key: ItemKey = `metric:${m.name}`;
              return (
                <Group key={key} gap="sm" wrap="nowrap">
                  <Checkbox
                    checked={checked.has(key)}
                    onChange={() => toggle(key)}
                    data-testid={`ossie-check-${key}`}
                    label={
                      <Text size="sm" ff="monospace">
                        {m.name}{" "}
                        <Text span size="xs" c="dimmed">
                          {m.expression}
                        </Text>
                      </Text>
                    }
                  />
                  {resultBadge(key)}
                </Group>
              );
            })}
            <Group justify="flex-end" gap="sm" mt="sm">
              <Button variant="default" onClick={() => setProposals(null)} data-testid="ossie-review-close">
                {Object.keys(results).length > 0 ? t("ossiePanel.close") : t("ossiePanel.cancel")}
              </Button>
              <Button
                onClick={handleApply}
                loading={applying}
                disabled={checked.size === 0}
                data-testid="ossie-apply"
              >
                {t("ossiePanel.apply", { count: checked.size })}
              </Button>
            </Group>
          </Stack>
        )}
      </Modal>
    </Card>
  );
}
