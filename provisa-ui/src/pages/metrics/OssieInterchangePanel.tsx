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
  Text,
  Title,
} from "@mantine/core";
import { Download, Upload } from "lucide-react";
import { CopyButton } from "../../components/CopyButton";
import { FormChecks, FormField, FormFooter, FormLayout } from "../../components/form/FormLayout";
import {
  OSSIE_ENDPOINT_PATH,
  fetchDbtSourcesYaml,
  fetchOssieYaml,
  importOssie,
  type OssieImportProposals,
} from "../../api/admin";
import {
  useRegisterTable,
  useUpsertMetric,
  useUpsertRelationship,
} from "../../hooks/useAdminQueries";
import { useAuth } from "../../context/AuthContext";
import type { MutationResult } from "../../types/admin";
import { Tip } from "../../components/Tip";

function saveAs(text: string, filename: string) {
  const url = URL.createObjectURL(new Blob([text], { type: "text/yaml" }));
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

type ItemKey = `table:${string}` | `rel:${string}` | `metric:${string}`;

export function OssieInterchangePanel() {
  const { t } = useTranslation();
  const { actingRoles } = useAuth();
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
      saveAs(await fetchOssieYaml(), "provisa.ossie.yaml");
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  // REQ-1967: the catalog as the roles in use are served it, for a dbt project's sources;
  // "Role: All" sends the selected set (REQ-1620).
  const handleDbtSources = async () => {
    if (!actingRoles) return;
    setError("");
    try {
      saveAs(await fetchDbtSourcesYaml(actingRoles), "provisa.sources.yml");
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
    // A mutation that raises is that item's failure, named; the items after it are still applied.
    const attempt = async (run: () => Promise<MutationResult>): Promise<MutationResult> => {
      try {
        return await run();
      } catch (e) {
        return { success: false, message: e instanceof Error ? e.message : String(e) };
      }
    };
    try {
      // Tables first, then relationships (which reference tables), then metrics —
      // each through the EXISTING registration mutations (REQ-1316).
      for (const tbl of proposals.tables) {
        const key: ItemKey = `table:${tbl.name}`;
        if (!checked.has(key)) continue;
        outcome[key] = await attempt(() =>
          registerTable({
            sourceId: tbl.source_id,
            domainId: proposals.model_name,
            schemaName: tbl.schema_name,
            tableName: tbl.table_name,
            alias: tbl.name !== tbl.table_name ? tbl.name : undefined,
            description: tbl.description ?? undefined,
            columns: tbl.columns.map((c) => ({
              name: c.name,
              alias: c.alias ?? undefined,
              // Registration grants read to the registrant and Viewer (REQ-2154).
              visibleTo: [],
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
          }),
        );
      }
      for (const rel of proposals.relationships) {
        const key: ItemKey = `rel:${rel.name}`;
        if (!checked.has(key)) continue;
        outcome[key] = await attempt(() =>
          upsertRelationship(
            {
              id: rel.name,
              sourceTableId: rel.from,
              targetTableId: rel.to,
              // A key of several columns is one ordered, comma-separated list on each end.
              sourceColumn: rel.from_columns.join(","),
              targetColumn: rel.to_columns.join(","),
              cardinality: "many-to-one",
            },
            true, // an imported relationship is a new one
          ),
        );
      }
      for (const m of proposals.metrics) {
        const key: ItemKey = `metric:${m.name}`;
        if (!checked.has(key)) continue;
        outcome[key] = await attempt(() =>
          upsertMetric(
            {
              name: m.name,
              expression: m.expression,
              datatype: m.datatype,
              description: m.description,
              aiContext: m.ai_context,
            },
            true, // an imported metric is a new one: a name the model holds is refused
          ),
        );
      }
    } finally {
      setApplying(false);
    }
    setResults(outcome);
  };

  const resultBadge = (key: string) => {
    const r = results[key];
    if (!r) return null;
    return r.success ? (
      <Badge size="xs" color="green" data-testid={`ossie-result-${key}`}>
        {t("ossiePanel.applied")}
      </Badge>
    ) : (
      <Tip text={r.message}>
        <Badge size="xs" color="red" data-testid={`ossie-result-${key}`}>
          {r.message || t("ossiePanel.failed")}
        </Badge>
      </Tip>
    );
  };

  const applyButton = (
    <Button
      onClick={handleApply}
      loading={applying}
      disabled={checked.size === 0}
      data-testid="ossie-apply"
    >
      {t("ossiePanel.apply", { count: checked.size })}
    </Button>
  );
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
            <Tip text={t("ossiePanel.copyEndpoint")} named>
              <CopyButton text={endpointUrl} aria-label={t("ossiePanel.copyEndpoint")} />
            </Tip>
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
          <Tip text={t("ossiePanel.dbtSourcesHelp", { role: actingRoles ?? "" })}>
            <Button
              size="xs"
              variant="default"
              leftSection={<Download size={13} />}
              onClick={handleDbtSources}
              disabled={!actingRoles}
              data-testid="dbt-sources-download"
            >
              {t("ossiePanel.dbtSources")}
            </Button>
          </Tip>
          {/* The opener of the review is hidden while the review is open. */}
          {!proposals && (
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
          )}
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
      <Text size="xs" c="dimmed" mt="xs" data-testid="export-governance-line">
        {t("ossiePanel.governance")}
      </Text>
      {error && (
        <Alert
          color="red"
          mt="sm"
          withCloseButton
          onClose={() => setError("")}
          data-testid="ossie-error"
        >
          {error}
        </Alert>
      )}

      {/* REQ-1316: registration review — the modeler accepts or trims the proposals. */}
      {/* A working surface, so it is laid out in the panel, not in a dialog (REQ-1968). */}
      {proposals && (
        <FormLayout data-testid="ossie-review-modal" style={{ marginTop: "0.75rem" }}>
          <FormField size="full">
            <Title order={4}>{t("ossiePanel.reviewTitle", { model: proposals.model_name })}</Title>
            <Text size="sm" c="dimmed">
              {t("ossiePanel.reviewHint")}
            </Text>
          </FormField>
          <Divider label={t("ossiePanel.tablesSection", { count: proposals.tables.length })} />
          <FormChecks column>
            {proposals.tables.map((tbl) => {
              const key: ItemKey = `table:${tbl.name}`;
              return (
                <FormField key={key} size="full">
                  <Group gap="sm" wrap="nowrap">
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
                          {tbl.not_importable.length > 0 && (
                            <Text
                              size="xs"
                              c="orange"
                              data-testid={`ossie-not-importable-${tbl.name}`}
                            >
                              {t("ossiePanel.notImportable", {
                                count: tbl.not_importable.length,
                                names: tbl.not_importable.map((f) => f.name).join(", "),
                              })}
                            </Text>
                          )}
                        </Text>
                      }
                    />
                    {resultBadge(key)}
                  </Group>
                </FormField>
              );
            })}
          </FormChecks>
          <Divider
            label={t("ossiePanel.relationshipsSection", { count: proposals.relationships.length })}
          />
          <FormChecks column>
            {proposals.relationships.map((rel) => {
              const key: ItemKey = `rel:${rel.name}`;
              return (
                <FormField key={key} size="full">
                  <Group gap="sm" wrap="nowrap">
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
                </FormField>
              );
            })}
          </FormChecks>
          <Divider label={t("ossiePanel.metricsSection", { count: proposals.metrics.length })} />
          <FormChecks column>
            {proposals.metrics.map((m) => {
              const key: ItemKey = `metric:${m.name}`;
              return (
                <FormField key={key} size="full">
                  <Group gap="sm" wrap="nowrap">
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
                </FormField>
              );
            })}
          </FormChecks>
          {/* Apply takes Save's place. Until something has been applied the other action is
              the shared Cancel; after, it is Close. */}
          {Object.keys(results).length > 0 ? (
            <FormFooter action={applyButton}>
              <Button
                variant="default"
                onClick={() => setProposals(null)}
                data-testid="ossie-review-close"
              >
                {t("ossiePanel.close")}
              </Button>
            </FormFooter>
          ) : (
            <FormFooter
              action={applyButton}
              onCancel={() => setProposals(null)}
              cancelProps={{ "data-testid": "ossie-review-close" }}
            />
          )}
        </FormLayout>
      )}
    </Card>
  );
}
