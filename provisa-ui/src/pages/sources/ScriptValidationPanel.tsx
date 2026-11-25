// Copyright (c) 2026 Kenneth Stott
// Canary: 9d3f7a20-4e81-4c56-b2a9-1f6e8c0d5b73
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2225: a saved Spark source's script, as its form holds it, validated by the source's host in
// three steps -- the static check of its tags and types, a dry run of each table and query on a
// sample of the governed tables picked for its table parameters (a @command is checked statically
// and not run, by design), and each resolver's two keys -- with each step's result and each
// function's.

// Requirements: REQ-2225

import { useState } from "react";
import { Alert, Badge, Button, Code, Group, Select, Stack, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import {
  validateScript,
  type DryRunResult,
  type FunctionStatus,
  type KeyResult,
  type ScriptFunction,
  type StepStatus,
  type ValidationReport,
} from "../../api/codeHosts";
import { FieldGroup, FormField, FormFold, FormFolds } from "../../components/form/FormLayout";
import { useTables } from "../../hooks/useTableQueries";
import { domainToSqlName } from "../../naming";

interface Props {
  orgId: string;
  sourceId: string;
  /** The script as the form holds it. */
  script: string;
}

const STEP_COLOR: Record<StepStatus, string> = {
  ok: "green",
  failed: "red",
  incomplete: "yellow",
  not_run: "gray",
};

const FUNCTION_COLOR: Record<FunctionStatus, string> = {
  ok: "green",
  empty: "gray",
  mismatch: "red",
  error: "red",
  needs_input: "yellow",
  not_run: "gray",
  not_run_by_design: "gray",
};

type Inputs = Record<string, Record<string, string>>;
type Keys = Record<string, [string, string]>;

function StepHead({
  step,
  status,
  reason,
}: {
  step: "static" | "dry_run" | "keys";
  status: StepStatus;
  reason?: string;
}) {
  const { t } = useTranslation();
  return (
    <Stack gap={2}>
      <Group gap="xs">
        <Text size="sm" fw={600}>
          {t(`scriptValidation.step_${step}`)}
        </Text>
        <Badge
          color={STEP_COLOR[status]}
          variant="light"
          data-testid={`script-validation-status-${step}`}
        >
          {t(`scriptValidation.status_${status}`)}
        </Badge>
      </Group>
      {reason && (
        <Text size="sm" c="dimmed" data-testid={`script-validation-reason-${step}`}>
          {reason}
        </Text>
      )}
    </Stack>
  );
}

function FunctionHead({ name, status }: { name: string; status: FunctionStatus }) {
  const { t } = useTranslation();
  return (
    <Group gap="xs">
      <Code>{name}</Code>
      <Badge color={FUNCTION_COLOR[status]} variant="dot">
        {t(`scriptValidation.fn_${status}`)}
      </Badge>
    </Group>
  );
}

function Lines({ lines, testId }: { lines: string[]; testId: string }) {
  return (
    <>
      {lines.map((line) => (
        <Text key={line} size="sm" c="red" data-testid={testId}>
          {line}
        </Text>
      ))}
    </>
  );
}

function DryRunRow({
  result,
  fn,
  tables,
  inputs,
  setInput,
}: {
  result: DryRunResult | undefined;
  fn: ScriptFunction;
  tables: string[];
  inputs: Record<string, string>;
  setInput: (parameter: string, table: string) => void;
}) {
  const { t } = useTranslation();
  // REQ-2225: a @command may write or call out, so its dry run is never asked for.
  const command = fn.kind === "command";
  return (
    <Stack gap={4} data-testid={`script-validation-fn-dry_run-${fn.name}`}>
      <Group gap="sm">
        <FunctionHead name={fn.name} status={result?.status ?? "not_run"} />
        {result && result.rows !== undefined && result.status !== "needs_input" && (
          <Text size="sm" c="dimmed">
            {t("scriptValidation.rows", { count: result.rows })}
          </Text>
        )}
      </Group>
      {result?.status === "not_run_by_design" && (
        <Text size="sm" c="dimmed" data-testid="script-validation-by-design">
          {t("scriptValidation.commandNotRun")}
        </Text>
      )}
      {result?.error && <Lines lines={[result.error]} testId="script-validation-error" />}
      {result?.mismatches && (
        <Lines lines={result.mismatches} testId="script-validation-mismatch" />
      )}
      {result?.missing && (
        <Text size="sm" data-testid="script-validation-missing">
          {t("scriptValidation.needsTables", { tables: result.missing.join(", ") })}
        </Text>
      )}
      {fn.tables.length > 0 && !command && (
        <FieldGroup>
          {fn.tables.map((parameter) => (
            <FormField key={parameter} size="medium">
              <Select
                searchable
                label={t("scriptValidation.tableFor", { parameter })}
                data={tables}
                value={inputs[parameter] ?? null}
                onChange={(v) => setInput(parameter, v ?? "")}
                data-testid={`script-validation-input-${fn.name}-${parameter}`}
              />
            </FormField>
          ))}
        </FieldGroup>
      )}
    </Stack>
  );
}

function KeyRow({
  result,
  fn,
  keys,
  setKeys,
}: {
  result: KeyResult | undefined;
  fn: ScriptFunction;
  keys: [string, string] | undefined;
  setKeys: (keys: [string, string]) => void;
}) {
  const { t } = useTranslation();
  const [source, target] = keys ?? ["", ""];
  return (
    <Stack gap={4} data-testid={`script-validation-fn-keys-${fn.name}`}>
      <Group gap="sm">
        <FunctionHead name={fn.name} status={result?.status ?? "not_run"} />
        {result?.rows !== undefined && (
          <Text size="sm" c="dimmed">
            {t("scriptValidation.rows", { count: result.rows })}
          </Text>
        )}
      </Group>
      {result?.error && <Lines lines={[result.error]} testId="script-validation-error" />}
      {result?.problems && <Lines lines={result.problems} testId="script-validation-mismatch" />}
      <FieldGroup>
        <FormField size="medium">
          <Select
            label={t("scriptValidation.sourceKey")}
            data={fn.columns}
            value={source || null}
            onChange={(v) => setKeys([v ?? "", target])}
            data-testid={`script-validation-key-${fn.name}-source`}
          />
        </FormField>
        <FormField size="medium">
          <Select
            label={t("scriptValidation.targetKey")}
            data={fn.columns}
            value={target || null}
            onChange={(v) => setKeys([source, v ?? ""])}
            data-testid={`script-validation-key-${fn.name}-target`}
          />
        </FormField>
      </FieldGroup>
    </Stack>
  );
}

/** The inputs and keys named so far, as the server takes them: blanks left out. */
function named(inputs: Inputs, keys: Keys): { inputs: Inputs; keys: Keys } {
  const given: Inputs = {};
  for (const [fn, parameters] of Object.entries(inputs)) {
    const set = Object.fromEntries(Object.entries(parameters).filter(([, table]) => table));
    if (Object.keys(set).length) given[fn] = set;
  }
  const both = Object.entries(keys).filter(([, [source, target]]) => source && target);
  return { inputs: given, keys: Object.fromEntries(both) };
}

export function ScriptValidationPanel({ orgId, sourceId, script }: Props) {
  const { t } = useTranslation();
  const { tables } = useTables({ columns: false });
  const [report, setReport] = useState<ValidationReport | null>(null);
  const [inputs, setInputs] = useState<Inputs>({});
  const [keys, setKeys] = useState<Keys>({});
  const [busy, setBusy] = useState(false);
  const [failed, setFailed] = useState("");

  const tableNames = tables
    .filter((row) => !row.withdrawn)
    .map((row) => `${domainToSqlName(row.domainId)}.${row.tableName}`);

  const run = async () => {
    setBusy(true);
    setFailed("");
    try {
      setReport(await validateScript(orgId, sourceId, { script, ...named(inputs, keys) }));
    } catch (e) {
      setFailed(String(e instanceof Error ? e.message : e));
    } finally {
      setBusy(false);
    }
  };

  const functions = report?.static.functions ?? [];
  const resolvers = functions.filter((fn) => fn.resolver);
  const ranOf = (name: string) => report?.dry_run.functions.find((r) => r.function === name);
  const keyedOf = (name: string) => report?.keys.functions.find((r) => r.function === name);

  return (
    <FormFolds id="script-validation">
      <FormFold
        value="validate"
        title={t("scriptValidation.title")}
        note={t("scriptValidation.note")}
        defaultOpen
      >
        {failed && (
          <Alert color="red" variant="light" data-testid="script-validation-failed">
            {failed}
          </Alert>
        )}
        <Group>
          <Button
            onClick={run}
            loading={busy}
            disabled={!script.trim()}
            data-testid="script-validation-run"
          >
            {report ? t("scriptValidation.runAgain") : t("scriptValidation.run")}
          </Button>
        </Group>
        {report && (
          <Stack gap="md" data-testid="script-validation-report">
            <Text size="sm" data-testid="script-validation-cluster">
              {report.cluster
                ? t("scriptValidation.cluster", {
                    address: report.cluster.address,
                    origin: t(`scriptValidation.origin_${report.cluster.origin}`),
                  })
                : t("scriptValidation.noCluster")}
            </Text>
            <Stack gap="xs" data-testid="script-validation-step-static">
              <StepHead step="static" status={report.static.status} />
              <Lines lines={report.static.problems} testId="script-validation-problem" />
              {report.static.status === "ok" && (
                <Text size="sm" c="dimmed">
                  {t("scriptValidation.functions", { count: functions.length })}
                </Text>
              )}
            </Stack>
            <Stack gap="xs" data-testid="script-validation-step-dry_run">
              <StepHead
                step="dry_run"
                status={report.dry_run.status}
                reason={report.dry_run.reason}
              />
              {functions.map((fn) => (
                <DryRunRow
                  key={fn.name}
                  fn={fn}
                  result={ranOf(fn.name)}
                  tables={tableNames}
                  inputs={inputs[fn.name] ?? {}}
                  setInput={(parameter, table) =>
                    setInputs({ ...inputs, [fn.name]: { ...inputs[fn.name], [parameter]: table } })
                  }
                />
              ))}
            </Stack>
            <Stack gap="xs" data-testid="script-validation-step-keys">
              <StepHead step="keys" status={report.keys.status} reason={report.keys.reason} />
              {resolvers.length === 0 && report.static.status === "ok" && (
                <Text size="sm" c="dimmed">
                  {t("scriptValidation.noResolvers")}
                </Text>
              )}
              {resolvers.map((fn) => (
                <KeyRow
                  key={fn.name}
                  fn={fn}
                  result={keyedOf(fn.name)}
                  keys={keys[fn.name]}
                  setKeys={(pair) => setKeys({ ...keys, [fn.name]: pair })}
                />
              ))}
            </Stack>
          </Stack>
        )}
      </FormFold>
    </FormFolds>
  );
}
