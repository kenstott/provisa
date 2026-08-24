// Copyright (c) 2026 Kenneth Stott
// Canary: 8a3f47d1-6c92-4b05-9e73-1d5a2f8c40be
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1443 clause 7: the data-quality contract builder on the table edit surface.
//
// The rows ARE the editor: one row per check, its args editable in place, plus the dataset the
// contract observes. There is no raw-text pane — an editor that offered both left an operator
// choosing between two representations of one contract.
//
// The contract text is still what registers and what the scan runs; it is serialized SERVER-side
// from these rows (provisa.dq.contract.build_contract), so the checker's dialect has exactly one
// implementation and the browser can never emit a shape the checker refuses. A row's args are the
// check's own authored arguments rather than a normalized summary, so a contract pasted in from a
// repo parses to rows and serializes back keeping every check body it arrived with.
//
// The args cell holds the ARGUMENTS alone. The dialect's envelope — soda's single key naming the
// check type, GX's `type`/`kwargs` pair — is restated by the server serializer, because it repeats
// what the row's own Check column already says and an operator cannot change it.
//
// The panel knows no check names. The picker offers what the server says this checker can express
// against each column's actual type (provisa.dq.catalog), scoped by the dataset the CONTRACT names
// — resolved server-side the same way the registration and the scan resolve it, so the offered
// checks match the columns the checker will really observe.

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Badge,
  Button,
  Group,
  NumberInput,
  Select,
  Stack,
  Table,
  Text,
  Textarea,
  TextInput,
} from "@mantine/core";
import { Play, Plus, Trash2 } from "lucide-react";
import { useDqContract } from "../../hooks/useAdminQueries";
import type { DqCheck, DqCheckCatalog, DqCheckKind, DqDryRun } from "../../types/admin";
import { CollapsibleSection } from "./CollapsibleSection";
import { FieldLabel } from "./FieldLabel";

/** A column-less key for the dataset-level checks, matching the server's empty `column_name`. */
const DATASET_SCOPE = "";

interface DataQualityPanelProps {
  /** The checker source type this results table's scans come from — soda | great_expectations. */
  checker: string;
  /** The source the dry run scans through; the checker's connection lives on it. */
  sourceId: string;
  contractText: string;
  onChange: (contractText: string) => void;
}

export function DataQualityPanel({
  checker,
  sourceId,
  contractText,
  onChange,
}: DataQualityPanelProps) {
  const { t } = useTranslation();
  const { parseContract, buildContract, checkCatalog, buildCheck, dryRunContract } =
    useDqContract();

  const [dataset, setDataset] = useState<string | null>(null);
  const [checks, setChecks] = useState<DqCheck[]>([]);
  const [parseError, setParseError] = useState<string | null>(null);
  // Kept with the dataset it was fetched for: renaming the dataset must not leave the picker
  // offering the previous table's columns while the new fetch is in flight.
  const [fetched, setFetched] = useState<{ dataset: string; catalog: DqCheckCatalog } | null>(null);
  const [buildError, setBuildError] = useState<string | null>(null);
  const [dryRun, setDryRun] = useState<DqDryRun | null>(null);
  const [running, setRunning] = useState(false);

  // The editors for the check being composed.
  const [column, setColumn] = useState<string>(DATASET_SCOPE);
  const [checkType, setCheckType] = useState<string>("");
  const [comparator, setComparator] = useState<string>("");
  const [threshold, setThreshold] = useState<number | null>(null);
  const [unit, setUnit] = useState<string>("");
  const [metric, setMetric] = useState<string>("");
  const [level, setLevel] = useState<string>("fail");
  const [params, setParams] = useState<Record<string, string>>({});

  // In-flight edits, keyed by row index and cleared whenever the registered contract comes back —
  // so a row shows what was typed until the server has serialized it, then shows what registered.
  const [argDrafts, setArgDrafts] = useState<Record<number, string>>({});
  const [datasetDraft, setDatasetDraft] = useState("");

  // The registered contract text is what the rows are read from — including the rows that appear
  // when a contract arrives already written, pasted from a repo.
  useEffect(() => {
    let live = true;
    void parseContract({ checker, contractText }).then((parsed) => {
      if (!live || parsed === null) return;
      setDataset(parsed.dataset);
      setDatasetDraft(parsed.dataset ?? "");
      setChecks(parsed.checks);
      setArgDrafts({});
      setParseError(parsed.error);
    });
    return () => {
      live = false;
    };
  }, [checker, contractText, parseContract]);

  // The picker's vocabulary is scoped to the dataset the contract names; a contract that names none
  // yet has no columns to offer checks against.
  useEffect(() => {
    if (dataset === null) return;
    let live = true;
    void checkCatalog({ checker, dataset }).then((result) => {
      if (live && result !== null) setFetched({ dataset, catalog: result });
    });
    return () => {
      live = false;
    };
  }, [checker, dataset, checkCatalog]);

  const catalog = fetched !== null && fetched.dataset === dataset ? fetched.catalog : null;

  const kinds: DqCheckKind[] =
    column === DATASET_SCOPE
      ? (catalog?.datasetChecks ?? [])
      : (catalog?.columns.find((c) => c.name === column)?.checks ?? []);
  const kind = kinds.find((k) => k.checkType === checkType) ?? null;

  const rewrite = useCallback(
    async (next: DqCheck[], nextDataset?: string) => {
      const target = nextDataset ?? dataset;
      if (target === null) return;
      const built = await buildContract({ checker, dataset: target, checks: next });
      if (built === null) return;
      if (built.error !== null) {
        setBuildError(built.error);
        return;
      }
      setBuildError(null);
      onChange(built.text);
    },
    [buildContract, checker, dataset, onChange],
  );

  const addCheck = useCallback(async () => {
    const built = await buildCheck({
      checker,
      check: {
        checkType,
        columnName: column,
        params: Object.keys(params).length > 0 ? JSON.stringify(params) : "",
        comparator,
        thresholdValue: threshold,
        metric,
        unit,
        level,
      },
    });
    if (built === null) return;
    if (built.error !== null) {
      setBuildError(built.error);
      return;
    }
    setBuildError(null);
    await rewrite([
      ...checks,
      { columnName: column, checkType, definition: built.definition, extra: "" },
    ]);
  }, [
    buildCheck,
    checker,
    checkType,
    checks,
    column,
    comparator,
    level,
    metric,
    params,
    rewrite,
    threshold,
    unit,
  ]);

  const runNow = useCallback(async () => {
    setRunning(true);
    try {
      const result = await dryRunContract({ sourceId, contractText });
      // A rejected mutation and a mutation that returned no payload both arrive here as null, and a
      // null dry run renders NOTHING — the operator watches the button stop spinning and is told
      // neither an outcome nor a reason. The transport failure IS the dry run's result, so it is
      // reported as one.
      setDryRun(
        result ?? {
          success: false,
          message: t("dataQualityPanel.dryRunNoResponse"),
          checkerVersion: null,
          checks: [],
        },
      );
    } catch (error) {
      setDryRun({
        success: false,
        message: error instanceof Error ? error.message : String(error),
        checkerVersion: null,
        checks: [],
      });
    } finally {
      setRunning(false);
    }
  }, [contractText, dryRunContract, sourceId, t]);

  return (
    // REQ-1358: `tourId` marks the whole section, not just its header, so the tour step highlights
    // the contract editor and the check builder together.
    <CollapsibleSection
      title={t("dataQualityPanel.title")}
      testId="dq-contract-panel"
      tourId="dq-panel"
      defaultOpen
    >
      <Stack gap="sm">
        {/* The dataset the contract observes. Committing on blur rather than per keystroke keeps a
            half-typed path from being serialized and rejected on every character. */}
        <div>
          <FieldLabel
            text={t("dataQualityPanel.datasetLabel")}
            help={t("dataQualityPanel.datasetHelp")}
          />
          <TextInput
            data-testid="dq-dataset-input"
            aria-label={t("dataQualityPanel.datasetLabel")}
            value={datasetDraft}
            onChange={(e) => setDatasetDraft(e.currentTarget.value)}
            onBlur={() => {
              if (datasetDraft !== (dataset ?? "")) void rewrite(checks, datasetDraft);
            }}
            styles={{ input: { fontFamily: "var(--mantine-font-family-monospace)" } }}
          />
        </div>
        {parseError !== null && (
          <Alert color="red" data-testid="dq-parse-error">
            {parseError}
          </Alert>
        )}
        {catalog?.error != null && (
          <Alert color="orange" data-testid="dq-catalog-error">
            {catalog.error}
          </Alert>
        )}
        {dataset !== null && (
          <Text size="xs" c="dimmed" data-testid="dq-dataset">
            {t("dataQualityPanel.dataset", { dataset })}
          </Text>
        )}

        {checks.length > 0 && (
          <Table className="data-table" data-testid="dq-check-rows">
            <Table.Thead>
              <Table.Tr>
                <Table.Th>{t("dataQualityPanel.columnHeader")}</Table.Th>
                <Table.Th>{t("dataQualityPanel.checkHeader")}</Table.Th>
                <Table.Th>{t("dataQualityPanel.definitionHeader")}</Table.Th>
                <Table.Th />
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {checks.map((c, i) => (
                <Table.Tr key={`${c.columnName}:${c.checkType}:${i}`}>
                  <Table.Td>{c.columnName || t("dataQualityPanel.datasetScope")}</Table.Td>
                  <Table.Td>{c.checkType}</Table.Td>
                  <Table.Td>
                    {/* The row IS the editor: the check's own args, committed on blur. A body that
                        does not parse is refused by the server serializer, which reports it in
                        dq-build-error and leaves the registered contract as it was. */}
                    <Textarea
                      data-testid={`dq-args-${i}`}
                      aria-label={t("dataQualityPanel.definitionHeader")}
                      value={argDrafts[i] ?? c.definition}
                      onChange={(e) => setArgDrafts({ ...argDrafts, [i]: e.currentTarget.value })}
                      onBlur={() => {
                        const edited = argDrafts[i];
                        if (edited === undefined || edited === c.definition) return;
                        void rewrite(
                          checks.map((row, j) => (j === i ? { ...row, definition: edited } : row)),
                        );
                      }}
                      autosize
                      minRows={1}
                      styles={{ input: { fontFamily: "var(--mantine-font-family-monospace)" } }}
                    />
                  </Table.Td>
                  <Table.Td>
                    <Button
                      variant="subtle"
                      size="compact-xs"
                      aria-label={t("dataQualityPanel.removeCheck")}
                      data-testid={`dq-remove-${i}`}
                      onClick={() => void rewrite(checks.filter((_, j) => j !== i))}
                    >
                      <Trash2 size={14} />
                    </Button>
                  </Table.Td>
                </Table.Tr>
              ))}
            </Table.Tbody>
          </Table>
        )}

        {/* The picker. Offering only what this checker can express against this column's type is the
            reason the catalog is a server call rather than a constant in the browser. */}
        <Group align="flex-end" gap="xs" wrap="wrap">
          <Select
            label={t("dataQualityPanel.columnLabel")}
            data-testid="dq-column"
            data={[
              { value: DATASET_SCOPE, label: t("dataQualityPanel.datasetScope") },
              ...(catalog?.columns ?? []).map((c) => ({
                value: c.name,
                label: c.dataType === null ? c.name : `${c.name} — ${c.dataType}`,
              })),
            ]}
            value={column}
            onChange={(v) => {
              setColumn(v ?? DATASET_SCOPE);
              setCheckType("");
              setParams({});
            }}
            allowDeselect={false}
            comboboxProps={{ withinPortal: true }}
          />
          <Select
            label={t("dataQualityPanel.checkLabel")}
            data-testid="dq-check-type"
            data={kinds.map((k) => ({ value: k.checkType, label: k.checkType }))}
            value={checkType || null}
            onChange={(v) => {
              setCheckType(v ?? "");
              setParams({});
              const picked = kinds.find((k) => k.checkType === v) ?? null;
              setComparator(picked?.comparators[0] ?? "");
              setMetric(picked?.metrics[0] ?? "");
              setUnit(picked?.thresholdUnits[0] ?? "");
              setLevel(picked?.levels.includes("fail") ? "fail" : (picked?.levels[0] ?? "fail"));
            }}
            placeholder={t("dataQualityPanel.checkPlaceholder")}
            comboboxProps={{ withinPortal: true }}
          />
          {kind?.metrics.length ? (
            <Select
              label={t("dataQualityPanel.metricLabel")}
              data-testid="dq-metric"
              data={kind.metrics}
              value={metric || null}
              onChange={(v) => setMetric(v ?? "")}
              comboboxProps={{ withinPortal: true }}
            />
          ) : null}
          {kind?.comparators.length ? (
            <>
              <Select
                label={t("dataQualityPanel.comparatorLabel")}
                data-testid="dq-comparator"
                data={kind.comparators}
                value={comparator || null}
                onChange={(v) => setComparator(v ?? "")}
                comboboxProps={{ withinPortal: true }}
                w={90}
              />
              <NumberInput
                label={t("dataQualityPanel.thresholdLabel")}
                data-testid="dq-threshold"
                value={threshold ?? ""}
                onChange={(v) => setThreshold(v === "" ? null : Number(v))}
                w={120}
              />
            </>
          ) : null}
          {kind?.thresholdUnits.length ? (
            <Select
              label={t("dataQualityPanel.unitLabel")}
              data-testid="dq-unit"
              data={kind.thresholdUnits}
              value={unit || null}
              onChange={(v) => setUnit(v ?? "")}
              comboboxProps={{ withinPortal: true }}
              w={110}
            />
          ) : null}
          {/* Severity per check: warn means the threshold WAS breached and only the reaction is
              milder — the outcome the export publishes is still a failure. */}
          {kind?.levels.length ? (
            <Select
              label={t("dataQualityPanel.levelLabel")}
              data-testid="dq-level"
              data={kind.levels}
              value={level}
              onChange={(v) => setLevel(v ?? "fail")}
              allowDeselect={false}
              comboboxProps={{ withinPortal: true }}
              w={110}
            />
          ) : null}
          {(kind?.params ?? []).map((p) =>
            p.choices.length > 0 ? (
              <Select
                key={p.name}
                label={p.name}
                data-testid={`dq-param-${p.name}`}
                data={p.choices}
                value={params[p.name] ?? null}
                onChange={(v) => setParams({ ...params, [p.name]: v ?? "" })}
                comboboxProps={{ withinPortal: true }}
              />
            ) : (
              <TextInput
                key={p.name}
                label={p.name}
                data-testid={`dq-param-${p.name}`}
                value={params[p.name] ?? ""}
                onChange={(e) => setParams({ ...params, [p.name]: e.currentTarget.value })}
              />
            ),
          )}
          <Button
            leftSection={<Plus size={14} />}
            data-testid="dq-add-check"
            disabled={checkType === "" || dataset === null}
            onClick={() => void addCheck()}
          >
            {t("dataQualityPanel.addCheck")}
          </Button>
        </Group>
        {buildError !== null && (
          <Alert color="red" data-testid="dq-build-error">
            {buildError}
          </Alert>
        )}

        {/* A contract whose dataset resolved elsewhere still scans cleanly, so the only way to see it
            aimed at the wrong table is to run it and read back which table it says it observed. */}
        <Group>
          <Button
            variant="default"
            leftSection={<Play size={14} />}
            data-testid="dq-dry-run"
            loading={running}
            disabled={contractText.trim() === ""}
            onClick={() => void runNow()}
          >
            {t("dataQualityPanel.dryRun")}
          </Button>
          <Text size="xs" c="dimmed">
            {t("dataQualityPanel.dryRunHelp")}
          </Text>
        </Group>
        {dryRun !== null && (
          <Alert
            color={dryRun.success ? "green" : "red"}
            data-testid="dq-dry-run-result"
            title={dryRun.message}
          >
            {dryRun.checks.length > 0 && (
              <Table className="data-table">
                <Table.Thead>
                  <Table.Tr>
                    <Table.Th>{t("dataQualityPanel.columnHeader")}</Table.Th>
                    <Table.Th>{t("dataQualityPanel.checkHeader")}</Table.Th>
                    <Table.Th>{t("dataQualityPanel.outcomeHeader")}</Table.Th>
                    <Table.Th>{t("dataQualityPanel.valueHeader")}</Table.Th>
                    <Table.Th>{t("dataQualityPanel.failedRowsHeader")}</Table.Th>
                  </Table.Tr>
                </Table.Thead>
                <Table.Tbody>
                  {dryRun.checks.map((c, i) => (
                    <Table.Tr key={`${c.columnName ?? ""}:${c.checkType}:${i}`}>
                      <Table.Td>{c.columnName ?? t("dataQualityPanel.datasetScope")}</Table.Td>
                      <Table.Td>{c.checkType}</Table.Td>
                      <Table.Td>
                        <Badge color={c.outcome === "pass" ? "green" : "red"}>{c.outcome}</Badge>
                      </Table.Td>
                      <Table.Td>{c.value ?? ""}</Table.Td>
                      <Table.Td>{c.failedRows ?? ""}</Table.Td>
                    </Table.Tr>
                  ))}
                </Table.Tbody>
              </Table>
            )}
          </Alert>
        )}
      </Stack>
    </CollapsibleSection>
  );
}
