// Copyright (c) 2026 Kenneth Stott
// Canary: 177e863c-e6cc-4fe0-a80b-6dc02116e06e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2199: a wizard's review and its result. The review shows what each answer becomes in
// Provisa's primitives -- an entity or a fact, its key or grain, its history setting, its
// relationships and its first-draft description -- under the method's conventional name, which
// the user keeps or changes per table. Create makes them all through the entity/fact API; the
// result shows each table made, with its relationships. The wizard does not come back to them.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Badge, Button, Group, Stack, Text } from "@mantine/core";
import {
  useRegisterEntity,
  useRegisterFact,
  useTables,
  useUpsertRelationship,
} from "../../../hooks/useAdminQueries";
import { serverMessage } from "../../../i18n/serverMessage";
import type { ListedTable } from "../../../types/admin";
import { FieldGroup, FormEntry, FormField, FormLayout } from "../../../components/form/FormLayout";
import {
  createPlan,
  targetName,
  type CreatedTable,
  type ModelPlan,
  type Names,
  type PlannedTable,
} from "./plan";
import { TextField } from "./pickers";
import { StepHead, WizardFooter } from "./WizardFrame";

/** The plan's tables under their final names, ready to create. */
function initialNames(plan: ModelPlan): Names {
  return Object.fromEntries(plan.tables.map((tbl) => [tbl.id, tbl.name]));
}

function Line({ label, value, testId }: { label: string; value: string; testId?: string }) {
  return (
    <Text size="sm" data-testid={testId}>
      <Text span fw={600}>
        {label}:
      </Text>{" "}
      {value}
    </Text>
  );
}

function PrimitiveSummary({ table, names }: { table: PlannedTable; names: Names }) {
  const { t } = useTranslation();
  const none = t("modelingWizard.desc.none");
  if (table.primitive === "entity") {
    return (
      <>
        <Line label={t("modelingWizard.review.source")} value={table.source} />
        <Line label={t("modelingWizard.review.key")} value={table.key.join(", ")} />
        <Line
          label={t("modelingWizard.review.attributes")}
          value={table.attributes.join(", ") || none}
        />
        <Line
          label={t("modelingWizard.review.history")}
          value={
            table.history === "none"
              ? t("modelingWizard.review.historyNone")
              : t("modelingWizard.review.historyKept", {
                  validTime: table.validTime ?? "",
                  recordedTime: table.recordedTime ?? "",
                })
          }
          testId={`wizard-review-history-${table.id}`}
        />
        {table.relationships.length > 0 && (
          <Line
            label={t("modelingWizard.review.relationships")}
            value={table.relationships
              .map((r) =>
                t("modelingWizard.review.relationship", {
                  via: r.sourceColumns.join(", "),
                  target: targetName(r.target, names),
                }),
              )
              .join("; ")}
            testId={`wizard-review-relationships-${table.id}`}
          />
        )}
      </>
    );
  }
  return (
    <>
      <Line label={t("modelingWizard.review.source")} value={table.source} />
      <Line label={t("modelingWizard.review.grain")} value={table.grain.join(", ")} />
      <Line
        label={t("modelingWizard.review.measures")}
        value={table.measures.map((m) => `${m.agg}(${m.column})`).join(", ") || none}
      />
      <Line
        label={t("modelingWizard.review.relationships")}
        value={
          table.dimensions
            .map((d) =>
              t("modelingWizard.review.relationship", {
                via: d.via,
                target: targetName(d.target, names),
              }),
            )
            .join("; ") || none
        }
        testId={`wizard-review-relationships-${table.id}`}
      />
    </>
  );
}

function CreatedView({ rows, onDone }: { rows: CreatedTable[]; onDone: () => void }) {
  const { t } = useTranslation();
  const failed = rows.find((r) => r.status === "failed");
  // The title counts tables; the relationships made between them are listed below it.
  const made = rows.filter((r) => r.status === "created" && r.primitive !== "relationship");
  return (
    <Stack gap="md" data-testid="wizard-created">
      <Alert
        color={failed ? "red" : "green"}
        title={
          failed
            ? t("modelingWizard.result.stopped", { name: failed.name })
            : t("modelingWizard.result.created", { count: made.length })
        }
      >
        <Stack gap={4}>
          {failed?.message && <Text size="sm">{failed.message}</Text>}
          <Text size="sm">{t("modelingWizard.result.draftNote")}</Text>
          <Text size="sm">{t("modelingWizard.result.oneWay")}</Text>
        </Stack>
      </Alert>
      <Stack gap="xs">
        {rows.map((r) => (
          <Group key={r.id} gap="xs" wrap="nowrap" data-testid={`wizard-created-${r.id}`}>
            <Badge
              variant="light"
              color={r.status === "created" ? "green" : r.status === "failed" ? "red" : "gray"}
            >
              {t(`modelingWizard.result.status.${r.status}`)}
            </Badge>
            <Text size="sm" fw={600}>
              {r.name}
            </Text>
            <Text size="sm" c="dimmed">
              {t(`modelingWizard.primitive.${r.primitive}`)} · {t(`modelingWizard.role.${r.role}`)}
              {r.primitive !== "relationship" &&
                r.relatesTo.length > 0 &&
                ` · ${t("modelingWizard.result.relatesTo", { tables: r.relatesTo.join(", ") })}`}
            </Text>
          </Group>
        ))}
      </Stack>
      <Group justify="flex-start">
        <Button onClick={onDone} data-testid="wizard-done">
          {t("modelingForm.done")}
        </Button>
      </Group>
    </Stack>
  );
}

export function ModelReview({
  plan,
  index,
  count,
  onBack,
  onCancel,
  onDone,
  onCreated,
}: {
  plan: ModelPlan;
  index: number;
  count: number;
  onBack: () => void;
  onCancel: () => void;
  onDone: () => void;
  /** Called once the plan has been created, whatever came of it: there is no going back. */
  onCreated: () => void;
}) {
  const { t } = useTranslation();
  const { registerEntity } = useRegisterEntity();
  const { registerFact } = useRegisterFact();
  const { upsertRelationship } = useUpsertRelationship();
  const { refetch: refetchTables } = useTables({ inPage: true });
  const [names, setNames] = useState<Names>(() => initialNames(plan));
  const [busy, setBusy] = useState(false);
  const [problem, setProblem] = useState<string | null>(null);
  const [rows, setRows] = useState<CreatedTable[] | null>(null);

  if (rows) return <CreatedView rows={rows} onDone={onDone} />;

  const create = async () => {
    const blank = plan.tables.find((tbl) => !names[tbl.id]?.trim());
    if (blank) {
      setProblem(t("modelingWizard.review.nameRequired"));
      return;
    }
    const final = Object.fromEntries(Object.entries(names).map(([id, n]) => [id, n.trim()]));
    if (new Set(Object.values(final)).size !== Object.keys(final).length) {
      setProblem(t("modelingWizard.review.nameTaken"));
      return;
    }
    setProblem(null);
    setBusy(true);
    const localized = (call: typeof registerEntity) => async (input: Record<string, unknown>) => {
      const res = await call(input);
      return { success: res.success, message: serverMessage(res, res.message) };
    };
    try {
      setRows(
        await createPlan(plan, final, {
          registerEntity: localized(registerEntity),
          registerFact: localized(registerFact),
          createRelationship: localized((input) => upsertRelationship(input, true)),
          // Registration may give a table an alias; a relationship names the table by it.
          virtualNames: async () => {
            const read = (await refetchTables()) as { data?: { tables?: ListedTable[] } };
            return Object.fromEntries(
              (read.data?.tables ?? [])
                .filter((tbl) => tbl.domainId === plan.domainId)
                .map((tbl) => [tbl.tableName, tbl.alias ?? tbl.tableName]),
            );
          },
        }),
      );
      onCreated();
    } finally {
      setBusy(false);
    }
  };

  return (
    <FormLayout>
      <StepHead
        index={index}
        count={count}
        title={t("modelingWizard.review.title")}
        intro={t("modelingWizard.review.intro")}
        testId="wizard-step-review"
      />
      {plan.tables.map((table) => (
        <FormEntry key={table.id} data-testid={`wizard-review-${table.id}`}>
          <FieldGroup>
            <TextField
              label={t("modelingWizard.review.nameOf", {
                role: t(`modelingWizard.role.${table.role}`),
              })}
              help={t("modelingWizard.review.nameHelp")}
              required
              testId={`wizard-review-name-${table.id}`}
              value={names[table.id] ?? ""}
              onChange={(v) => setNames((prev) => ({ ...prev, [table.id]: v }))}
            />
          </FieldGroup>
          <FieldGroup>
            <FormField size="full">
              <Stack gap={2}>
                <Group gap="xs">
                  <Badge variant="light" color={table.primitive === "fact" ? "grape" : "teal"}>
                    {t(`modelingWizard.primitive.${table.primitive}`)}
                  </Badge>
                  <Text size="sm" c="dimmed">
                    {t(`modelingWizard.roleIn.${table.role}`)}
                  </Text>
                </Group>
                <PrimitiveSummary table={table} names={names} />
                <Line
                  label={t("modelingWizard.review.description")}
                  value={table.describe((id) => names[id] ?? id)}
                  testId={`wizard-review-description-${table.id}`}
                />
              </Stack>
            </FormField>
          </FieldGroup>
        </FormEntry>
      ))}
      {plan.notCreated.length > 0 && (
        <FormField size="full">
          <Alert color="gray" title={t("modelingWizard.review.notCreatedTitle")}>
            <Stack gap={2}>
              {plan.notCreated.map((line) => (
                <Text size="sm" key={line}>
                  {line}
                </Text>
              ))}
            </Stack>
          </Alert>
        </FormField>
      )}
      <FormField size="full">
        <Text size="sm" c="dimmed">
          {t("modelingWizard.review.draftNote")}
        </Text>
      </FormField>
      <WizardFooter
        first={false}
        nextLabel={t("modelingWizard.review.create")}
        nextTestId="wizard-create"
        creates
        onNext={() => void create()}
        onBack={onBack}
        onCancel={onCancel}
        problem={problem}
        busy={busy}
      />
    </FormLayout>
  );
}
