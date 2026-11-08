// Copyright (c) 2026 Kenneth Stott
// Canary: cbbf9e2e-bd59-4fdf-bbba-19aeb4ef37f3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2199: the star schema (Kimball) wizard. It asks in Kimball's words -- the business process
// and its fact type, the grain, the dimensions (each new or conformed, and how it handles change),
// the measures and their additivity -- then reviews what they become: a dim_ entity per new
// dimension (SCD type 2 and snapshot keep history) and one fact_ fact related to every dimension.

import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon } from "@mantine/core";
import { Trash2 } from "lucide-react";
import { usePanelDirty } from "../../../components/list/panelLeave";
import { FieldGroup, FormEntry, FormLayout } from "../../../components/form/FormLayout";
import type { Domain } from "../../../types/admin";
import { AGGS } from "./columns";
import { ModelReview } from "./ModelReview";
import { ChoiceField, ChoicesField, DomainField, TextField } from "./pickers";
import { useColumnOptions, useModelTables, type ModelTables } from "./useModelTables";
import {
  planStar,
  type Additivity,
  type FactType,
  type Scd,
  type StarAnswers,
  type StarDimension,
} from "./plan";
import { AddButton, ListHead, StepHead, WizardFooter } from "./WizardFrame";

const STEPS = ["process", "grain", "dimensions", "measures", "review"] as const;
const FACT_TYPES: FactType[] = ["transaction", "periodic", "accumulating"];
const SCDS: Scd[] = ["type1", "type2", "snapshot"];
const ADDITIVITY: Additivity[] = ["additive", "semiAdditive", "nonAdditive"];

let _uid = 0;
const newDimension = (): StarDimension => ({
  uid: `d${++_uid}`,
  name: "",
  conformed: null,
  source: null,
  key: [],
  attributes: [],
  scd: "type1",
  validTime: null,
  recordedTime: null,
  via: null,
});

const EMPTY: StarAnswers = {
  process: "",
  domainId: "",
  source: null,
  factType: "transaction",
  grainStatement: "",
  grain: [],
  dimensions: [],
  measures: [],
};

/** What a step still needs before Next, as the problem to show; null when it is answered. */
function missing(step: (typeof STEPS)[number], a: StarAnswers): string | null {
  switch (step) {
    case "process":
      return a.process.trim() && a.domainId && a.source ? null : "process";
    case "grain":
      return a.grain.length ? null : "grain";
    case "dimensions":
      return a.dimensions.every(
        (d) =>
          d.via &&
          d.name.trim() &&
          (d.conformed ||
            (d.source && d.key.length && (d.scd === "type1" || (d.validTime && d.recordedTime)))),
      )
        ? null
        : "dimensions";
    default:
      return null;
  }
}

function DimensionEntry({
  dim,
  index,
  tables,
  factColumns,
  onChange,
  onRemove,
}: {
  dim: StarDimension;
  index: number;
  tables: ModelTables;
  factColumns: { value: string; label: string }[];
  onChange: (d: StarDimension) => void;
  onRemove: () => void;
}) {
  const { t } = useTranslation();
  const cols = useColumnOptions(tables.columnsOf(dim.source));
  const set = (patch: Partial<StarDimension>) => onChange({ ...dim, ...patch });
  const noSource = !dim.source;
  return (
    <FormEntry data-testid={`star-dim-${index}`}>
      <FieldGroup row className="form-field-group--bare">
        <TextField
          label={t("modelingWizard.star.dimName")}
          help={t("modelingWizard.star.dimNameHelp")}
          required
          testId={`star-dim-name-${index}`}
          value={dim.name}
          onChange={(name) => set({ name })}
          placeholder="customer"
        />
        <ChoiceField
          label={t("modelingWizard.star.via")}
          help={t("modelingWizard.star.viaHelp")}
          required
          testId={`star-dim-via-${index}`}
          data={factColumns}
          value={dim.via}
          onChange={(via) => set({ via })}
        />
        <ActionIcon
          variant="subtle"
          color="red"
          onClick={onRemove}
          aria-label={t("modelingWizard.remove")}
          data-testid={`star-dim-remove-${index}`}
        >
          <Trash2 size={16} />
        </ActionIcon>
      </FieldGroup>
      <FieldGroup>
        <ChoiceField
          label={t("modelingWizard.star.conformed")}
          help={t("modelingWizard.star.conformedHelp")}
          size="long"
          testId={`star-dim-conformed-${index}`}
          data={tables.dimensionOptions}
          value={dim.conformed}
          onChange={(conformed) => set({ conformed })}
          placeholder={t("modelingWizard.star.conformedNew")}
        />
      </FieldGroup>
      {!dim.conformed && (
        <>
          <FieldGroup>
            <ChoiceField
              label={t("modelingWizard.star.dimSource")}
              help={t("modelingWizard.star.dimSourceHelp")}
              required
              size="long"
              testId={`star-dim-source-${index}`}
              data={tables.sourceOptions}
              value={dim.source}
              onChange={(source) => set({ source, key: [], attributes: [] })}
            />
            <ChoicesField
              label={t("tableEditForm.businessKeyLabel")}
              help={t("modelingWizard.star.dimKeyHelp")}
              required
              testId={`star-dim-key-${index}`}
              data={cols.keys}
              value={dim.key}
              onChange={(key) => set({ key })}
              disabled={noSource}
              placeholder={noSource ? t("modelingForm.selectSourceFirst") : undefined}
            />
            <ChoicesField
              label={t("modelingForm.attributesLabel")}
              help={t("modelingWizard.star.dimAttributesHelp")}
              testId={`star-dim-attributes-${index}`}
              data={cols.all}
              value={dim.attributes}
              onChange={(attributes) => set({ attributes })}
              disabled={noSource}
              placeholder={noSource ? t("modelingForm.selectSourceFirst") : undefined}
            />
          </FieldGroup>
          <FieldGroup>
            <ChoiceField
              label={t("modelingWizard.star.scdLabel")}
              help={t("modelingWizard.star.scdHelp")}
              required
              testId={`star-dim-scd-${index}`}
              data={SCDS.map((s) => ({ value: s, label: t(`modelingWizard.star.scd.${s}`) }))}
              value={dim.scd}
              onChange={(v) => set({ scd: (v as Scd | null) ?? "type1" })}
            />
            {dim.scd !== "type1" && (
              <>
                <ChoiceField
                  label={t("modelingWizard.validTime")}
                  help={t("modelingWizard.validTimeHelp")}
                  required
                  testId={`star-dim-valid-${index}`}
                  data={cols.all}
                  value={dim.validTime}
                  onChange={(validTime) => set({ validTime })}
                  disabled={noSource}
                />
                <ChoiceField
                  label={t("modelingWizard.recordedTime")}
                  help={t("modelingWizard.recordedTimeHelp")}
                  required
                  testId={`star-dim-recorded-${index}`}
                  data={cols.all}
                  value={dim.recordedTime}
                  onChange={(recordedTime) => set({ recordedTime })}
                  disabled={noSource}
                />
              </>
            )}
          </FieldGroup>
        </>
      )}
    </FormEntry>
  );
}

export function StarSchemaWizard({
  domains,
  onSuccess,
  onCancel,
}: {
  domains: Domain[];
  onSuccess: () => void;
  onCancel: () => void;
}) {
  const { t } = useTranslation();
  const tables = useModelTables();
  const [a, setA] = useState<StarAnswers>(EMPTY);
  const [step, setStep] = useState(0);
  const [problem, setProblem] = useState<string | null>(null);
  const [created, setCreated] = useState(false);
  const set = (patch: Partial<StarAnswers>) => setA((prev) => ({ ...prev, ...patch }));
  const factCols = useColumnOptions(tables.columnsOf(a.source));
  const plan = useMemo(() => planStar(a, t), [a, t]);
  usePanelDirty(!created && (Boolean(a.process || a.source) || step > 0), a.process);

  const id = STEPS[step];
  const next = () => {
    const gap = missing(id, a);
    if (gap) {
      setProblem(t(`modelingWizard.star.missing.${gap}`));
      return;
    }
    setProblem(null);
    setStep(step + 1);
  };
  const back = () => {
    setProblem(null);
    setStep(step - 1);
  };

  if (id === "review") {
    return (
      <ModelReview
        plan={plan}
        index={step}
        count={STEPS.length}
        onBack={back}
        onCancel={onCancel}
        onDone={onSuccess}
        onCreated={() => setCreated(true)}
      />
    );
  }

  const noSource = !a.source;
  return (
    <FormLayout>
      <StepHead
        index={step}
        count={STEPS.length}
        title={t(`modelingWizard.star.${id}.title`)}
        intro={t(`modelingWizard.star.${id}.intro`)}
        testId={`wizard-step-${id}`}
      />
      {id === "process" && (
        <>
          <TextField
            label={t("modelingWizard.star.processLabel")}
            help={t("modelingWizard.star.processHelp")}
            required
            testId="star-process"
            value={a.process}
            onChange={(process) => set({ process })}
            placeholder="order fulfillment"
          />
          <DomainField
            domains={domains}
            value={a.domainId}
            onChange={(domainId) => set({ domainId })}
            testId="star-domain"
          />
          <ChoiceField
            label={t("modelingWizard.star.sourceLabel")}
            help={t("modelingWizard.star.sourceHelp")}
            required
            size="long"
            testId="star-source"
            data={tables.sourceOptions}
            value={a.source}
            onChange={(source) =>
              set({
                source,
                grain: [],
                measures: [],
                dimensions: a.dimensions.map((d) => ({ ...d, via: null })),
              })
            }
          />
          <ChoiceField
            label={t("modelingWizard.star.factTypeLabel")}
            help={t("modelingWizard.star.factTypeHelp")}
            required
            testId="star-fact-type"
            data={FACT_TYPES.map((f) => ({
              value: f,
              label: t(`modelingWizard.star.factType.${f}`),
            }))}
            value={a.factType}
            onChange={(v) => set({ factType: (v as FactType | null) ?? "transaction" })}
          />
        </>
      )}
      {id === "grain" && (
        <>
          <TextField
            label={t("modelingWizard.star.grainStatement")}
            help={t("modelingWizard.star.grainStatementHelp")}
            size="long"
            testId="star-grain-statement"
            value={a.grainStatement}
            onChange={(grainStatement) => set({ grainStatement })}
            placeholder={t("modelingWizard.star.grainStatementPlaceholder")}
          />
          <ChoicesField
            label={t("modelingForm.grainLabel")}
            help={t("modelingWizard.star.grainHelp")}
            hint={t("modelingForm.keyHint")}
            required
            testId="star-grain"
            data={factCols.keys}
            value={a.grain}
            onChange={(grain) => set({ grain })}
            disabled={noSource}
          />
        </>
      )}
      {id === "dimensions" && (
        <>
          <ListHead
            text={t("modelingForm.dimensionsLabel")}
            help={t("modelingWizard.star.dimensionsHelp")}
          />
          {a.dimensions.map((d, i) => (
            <DimensionEntry
              key={d.uid}
              dim={d}
              index={i}
              tables={tables}
              factColumns={factCols.all}
              onChange={(nd) =>
                set({ dimensions: a.dimensions.map((x) => (x.uid === d.uid ? nd : x)) })
              }
              onRemove={() => set({ dimensions: a.dimensions.filter((x) => x.uid !== d.uid) })}
            />
          ))}
          <AddButton
            onClick={() => set({ dimensions: [...a.dimensions, newDimension()] })}
            testId="star-dim-add"
          >
            {t("modelingForm.addDimension")}
          </AddButton>
        </>
      )}
      {id === "measures" && (
        <>
          <ChoicesField
            label={t("modelingForm.measuresLabel")}
            help={t("modelingWizard.star.measuresHelp")}
            hint={t("modelingForm.measuresHint")}
            testId="star-measures"
            data={factCols.numeric}
            value={a.measures.map((m) => m.column)}
            onChange={(cols) =>
              set({
                measures: cols.map(
                  (column) =>
                    a.measures.find((m) => m.column === column) ?? {
                      column,
                      agg: "sum",
                      additivity: "additive",
                    },
                ),
              })
            }
          />
          {a.measures.map((m) => (
            // Each measure's aggregation and additivity share its row.
            <FieldGroup row className="form-field-group--bare" key={m.column}>
              <ChoiceField
                label={t("modelingWizard.star.aggOf", { column: m.column })}
                size="short"
                testId={`star-measure-agg-${m.column}`}
                data={AGGS.map((g) => ({ value: g, label: g }))}
                value={m.agg}
                onChange={(agg) =>
                  set({
                    measures: a.measures.map((x) =>
                      x.column === m.column ? { ...x, agg: agg ?? "sum" } : x,
                    ),
                  })
                }
              />
              <ChoiceField
                label={t("modelingWizard.star.additivityLabel")}
                help={t("modelingWizard.star.additivityHelp")}
                testId={`star-measure-additivity-${m.column}`}
                data={ADDITIVITY.map((v) => ({
                  value: v,
                  label: t(`modelingWizard.star.additivity.${v}`),
                }))}
                value={m.additivity}
                onChange={(v) =>
                  set({
                    measures: a.measures.map((x) =>
                      x.column === m.column
                        ? { ...x, additivity: (v as Additivity | null) ?? "additive" }
                        : x,
                    ),
                  })
                }
              />
            </FieldGroup>
          ))}
        </>
      )}
      <WizardFooter
        first={step === 0}
        nextLabel={
          STEPS[step + 1] === "review" ? t("modelingWizard.toReview") : t("modelingWizard.next")
        }
        onNext={next}
        onBack={back}
        onCancel={onCancel}
        problem={problem}
      />
    </FormLayout>
  );
}
