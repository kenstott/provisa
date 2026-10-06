// Copyright (c) 2026 Kenneth Stott
// Canary: 8f2d6b19-0e47-4a3c-9b85-d1c7e4a0f263
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Badge,
  Button,
  Checkbox,
  Group,
  NumberInput,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import {
  defineDataset,
  dropDataset,
  fetchDatasets,
  fetchProfileRuns,
  fetchRelationships,
  fetchReport,
  generateDataset,
  type DatasetRelationship,
  type FanoutCondition,
  type ProfiledTableRuns,
  type ReportRow,
  type SyntheticDataset,
} from "../../api/synthetic";
import { DatasetAssertions } from "./synthetic/DatasetAssertions";
import { DatasetConditions } from "./synthetic/DatasetConditions";

const PROD = "prod";

function num(v: number | null): string {
  if (v == null) return "";
  return Number.isInteger(v) ? String(v) : v.toPrecision(4);
}

// The report's own measures beside the profile comparison's (REQ-1939): an assertion's result
// and each conditional fan-out's parents and children.
const OWN_MEASURES = new Set([
  "assertion",
  "conditional_parents",
  "conditional_children",
  "privacy_guarantee",
  "privacy_epsilon",
  "privacy_epsilon_family",
  "privacy_dropped",
  "privacy_mostly_noise",
  "privacy_epsilon_charged",
]);

// REQ-1939: an environment's synthetic datasets -- defined, generated, regenerated and dropped
// beside the environment's other settings. Production holds none, so it is not offered.
export function SyntheticDatasetsPanel({ envs }: { envs: string[] }) {
  const { t } = useTranslation();
  const choices = envs.filter((e) => e !== PROD);
  const [env, setEnv] = useState<string | null>(choices[0] ?? null);
  const [datasets, setDatasets] = useState<SyntheticDataset[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [report, setReport] = useState<{ id: string; rows: ReportRow[] } | null>(null);
  // The define form.
  const [name, setName] = useState("");
  const [seed, setSeed] = useState<number>(1);
  const [scale, setScale] = useState<number>(1);
  const [profileEnv, setProfileEnv] = useState<string>(PROD);
  const [runs, setRuns] = useState<ProfiledTableRuns[]>([]);
  const [picked, setPicked] = useState<Record<number, { runId: string; scale: number | null }>>({});
  const [relationships, setRelationships] = useState<DatasetRelationship[]>([]);
  const [conditions, setConditions] = useState<FanoutCondition[]>([]);
  const [assertions, setAssertions] = useState<string[]>([]);
  const [epsilon, setEpsilon] = useState<number | null>(null);

  const reload = useCallback(() => {
    if (!env) return;
    fetchDatasets(env)
      .then(setDatasets)
      .catch((e: Error) => setError(e.message));
  }, [env]);
  useEffect(reload, [reload]);

  useEffect(() => {
    if (!env) return;
    fetchRelationships(env)
      .then(setRelationships)
      .catch((e: Error) => setError(e.message));
  }, [env]);

  useEffect(() => {
    if (!env) return;
    fetchProfileRuns(env, profileEnv)
      .then(setRuns)
      .catch((e: Error) => setError(e.message));
  }, [env, profileEnv]);

  const act = async (label: string, work: () => Promise<unknown>) => {
    setBusy(label);
    setError(null);
    try {
      await work();
      reload();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(null);
    }
  };

  if (!env) {
    return (
      <Text size="sm" c="dimmed" data-testid="synthetic-no-env">
        {t("syntheticDatasets.noEnvironment")}
      </Text>
    );
  }

  const define = () =>
    act("define", () =>
      defineDataset(env, name.trim(), {
        seed,
        scale,
        tables: Object.entries(picked).map(([id, p]) => ({
          tableId: Number(id),
          profileEnv,
          runId: p.runId,
          scale: p.scale,
        })),
        fanoutConditions: conditions,
        assertions,
        privateEpsilon: epsilon,
      }),
    );
  const generated = relationships.filter(
    (r) => picked[r.parentTableId] !== undefined && picked[r.childTableId] !== undefined,
  );
  const measureLabel = (m: string) =>
    OWN_MEASURES.has(m) ? t(`syntheticDatasets.measure_${m}`) : m;
  const syntheticCell = (r: ReportRow) => {
    if (r.measure !== "assertion") return num(r.synthetic);
    if (r.synthetic == null) return t("syntheticDatasets.assertionError");
    return r.synthetic === 1
      ? t("syntheticDatasets.assertionTrue")
      : t("syntheticDatasets.assertionFalse");
  };

  return (
    <Stack gap="md" data-testid="synthetic-panel">
      <Title order={4}>{t("syntheticDatasets.title")}</Title>
      <Text size="sm" c="dimmed">
        {t("syntheticDatasets.intro")}
      </Text>
      <Select
        label={t("syntheticDatasets.environment")}
        data={choices}
        value={env}
        onChange={setEnv}
        allowDeselect={false}
        data-testid="synthetic-env"
      />
      {error && (
        <Alert color="red" data-testid="synthetic-error">
          {error}
        </Alert>
      )}
      <Table striped data-testid="synthetic-datasets">
        <Table.Thead>
          <Table.Tr>
            <Table.Th>{t("syntheticDatasets.colName")}</Table.Th>
            <Table.Th>{t("syntheticDatasets.colStatus")}</Table.Th>
            <Table.Th>{t("syntheticDatasets.colScale")}</Table.Th>
            <Table.Th>{t("syntheticDatasets.colTables")}</Table.Th>
            <Table.Th />
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {datasets.map((d) => (
            <Table.Tr key={d.id} data-testid={`synthetic-row-${d.id}`}>
              <Table.Td>{d.id}</Table.Td>
              <Table.Td>
                <Badge
                  color={
                    d.status === "generated" ? "green" : d.status === "failed" ? "red" : "gray"
                  }
                >
                  {t(`syntheticDatasets.status_${d.status}`)}
                </Badge>
                {d.error && (
                  <Text size="xs" c="red">
                    {d.error}
                  </Text>
                )}
              </Table.Td>
              <Table.Td>×{d.scale}</Table.Td>
              <Table.Td>{d.tables.length}</Table.Td>
              <Table.Td>
                <Group gap={4}>
                  <Button
                    size="xs"
                    loading={busy === `generate:${d.id}`}
                    onClick={() => act(`generate:${d.id}`, () => generateDataset(env, d.id))}
                    data-testid={`synthetic-generate-${d.id}`}
                  >
                    {d.status === "generated"
                      ? t("syntheticDatasets.regenerate")
                      : t("syntheticDatasets.generate")}
                  </Button>
                  <Button
                    size="xs"
                    variant="light"
                    disabled={d.status !== "generated"}
                    onClick={() =>
                      fetchReport(env, d.id)
                        .then((rows) => setReport({ id: d.id, rows }))
                        .catch((e: Error) => setError(e.message))
                    }
                    data-testid={`synthetic-report-${d.id}`}
                  >
                    {t("syntheticDatasets.report")}
                  </Button>
                  <Button
                    size="xs"
                    variant="default"
                    color="red"
                    loading={busy === `drop:${d.id}`}
                    onClick={() => act(`drop:${d.id}`, () => dropDataset(env, d.id))}
                    data-testid={`synthetic-drop-${d.id}`}
                  >
                    {t("syntheticDatasets.drop")}
                  </Button>
                </Group>
              </Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>

      {report && (
        <Stack gap={4} data-testid="synthetic-report">
          <Title order={5}>{t("syntheticDatasets.reportTitle", { dataset: report.id })}</Title>
          <Table fz="xs" withTableBorder>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>{t("syntheticDatasets.colTable")}</Table.Th>
                <Table.Th>{t("syntheticDatasets.colColumn")}</Table.Th>
                <Table.Th>{t("syntheticDatasets.colMeasure")}</Table.Th>
                <Table.Th>{t("syntheticDatasets.colSource")}</Table.Th>
                <Table.Th>{t("syntheticDatasets.colSynthetic")}</Table.Th>
                <Table.Th>{t("syntheticDatasets.colDelta")}</Table.Th>
                <Table.Th>{t("syntheticDatasets.colNote")}</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {report.rows.map((r, i) => (
                <Table.Tr key={i}>
                  <Table.Td>{r.table}</Table.Td>
                  <Table.Td>{r.column ?? ""}</Table.Td>
                  <Table.Td>{measureLabel(r.measure)}</Table.Td>
                  <Table.Td>{num(r.source)}</Table.Td>
                  <Table.Td>{syntheticCell(r)}</Table.Td>
                  <Table.Td>{num(r.delta)}</Table.Td>
                  <Table.Td>{r.note ?? ""}</Table.Td>
                </Table.Tr>
              ))}
            </Table.Tbody>
          </Table>
        </Stack>
      )}

      <Title order={5}>{t("syntheticDatasets.defineTitle")}</Title>
      <Group align="end">
        <TextInput
          label={t("syntheticDatasets.name")}
          value={name}
          onChange={(e) => setName(e.currentTarget.value)}
          data-testid="synthetic-name"
        />
        <NumberInput
          label={t("syntheticDatasets.scale")}
          description={t("syntheticDatasets.scaleHelp")}
          value={scale}
          min={0.001}
          decimalScale={3}
          onChange={(v) => setScale(Number(v))}
          data-testid="synthetic-scale"
        />
        <NumberInput
          label={t("syntheticDatasets.seed")}
          value={seed}
          allowDecimal={false}
          onChange={(v) => setSeed(Number(v))}
          data-testid="synthetic-seed"
        />
        <Select
          label={t("syntheticDatasets.profileEnv")}
          data={envs}
          value={profileEnv}
          onChange={(v) => v && setProfileEnv(v)}
          allowDeselect={false}
          data-testid="synthetic-profile-env"
        />
      </Group>
      {runs.length === 0 ? (
        <Text size="sm" c="dimmed" data-testid="synthetic-no-runs">
          {t("syntheticDatasets.noRuns", { env: profileEnv })}
        </Text>
      ) : (
        <Table data-testid="synthetic-tables">
          <Table.Tbody>
            {runs.map((r) => {
              const p = picked[r.tableId];
              return (
                <Table.Tr key={r.tableId}>
                  <Table.Td>
                    <Checkbox
                      label={r.tableName}
                      checked={p !== undefined}
                      onChange={(e) => {
                        const next = { ...picked };
                        if (e.currentTarget.checked)
                          next[r.tableId] = { runId: r.runs[0].runId, scale: null };
                        else delete next[r.tableId];
                        setPicked(next);
                      }}
                      data-testid={`synthetic-table-${r.tableName}`}
                    />
                  </Table.Td>
                  <Table.Td>
                    <Select
                      size="xs"
                      aria-label={t("syntheticDatasets.run")}
                      disabled={p === undefined}
                      data={r.runs.map((run) => ({
                        value: run.runId,
                        label: t("syntheticDatasets.runOption", {
                          time: new Date(run.runTime).toLocaleString(),
                          rows: run.rowCount ?? 0,
                        }),
                      }))}
                      value={p?.runId ?? r.runs[0].runId}
                      onChange={(v) =>
                        v &&
                        setPicked({ ...picked, [r.tableId]: { ...picked[r.tableId], runId: v } })
                      }
                    />
                  </Table.Td>
                  <Table.Td>
                    <NumberInput
                      size="xs"
                      aria-label={t("syntheticDatasets.tableScale")}
                      placeholder={t("syntheticDatasets.tableScalePlaceholder")}
                      disabled={p === undefined}
                      min={0.001}
                      decimalScale={3}
                      value={p?.scale ?? ""}
                      onChange={(v) =>
                        setPicked({
                          ...picked,
                          [r.tableId]: { ...picked[r.tableId], scale: v === "" ? null : Number(v) },
                        })
                      }
                    />
                  </Table.Td>
                </Table.Tr>
              );
            })}
          </Table.Tbody>
        </Table>
      )}
      <Group align="end">
        <Checkbox
          label={t("syntheticDatasets.private")}
          description={t("syntheticDatasets.privateHelp")}
          checked={epsilon !== null}
          onChange={(e) => setEpsilon(e.currentTarget.checked ? 1 : null)}
          data-testid="synthetic-private"
        />
        {epsilon !== null && (
          <NumberInput
            label={t("syntheticDatasets.epsilon")}
            min={0.001}
            decimalScale={3}
            value={epsilon}
            onChange={(v) => setEpsilon(Number(v))}
            data-testid="synthetic-epsilon"
          />
        )}
      </Group>
      <DatasetConditions relationships={generated} value={conditions} onChange={setConditions} />
      <DatasetAssertions value={assertions} onChange={setAssertions} />
      <Group>
        <Button
          onClick={define}
          loading={busy === "define"}
          disabled={name.trim() === "" || Object.keys(picked).length === 0}
          data-testid="synthetic-define"
        >
          {t("syntheticDatasets.define")}
        </Button>
      </Group>
    </Stack>
  );
}
