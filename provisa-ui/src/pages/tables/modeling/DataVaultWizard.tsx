// Copyright (c) 2026 Kenneth Stott
// Canary: cd364e1f-ec51-45da-950e-624aae58a7f4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2199: the Data Vault (Linstedt) wizard. It asks in the vault's words -- the hubs and their
// business keys and record sources (with same-as links), the links between hubs and whether they
// end (effectivity), the satellites grouped by source, rate of change or sensitivity (multi-active
// where several rows are active at once), how far back history goes, and the business vault's
// rules -- then reviews what they become: a hub_ entity per hub, a lnk_ fact with no measures per
// link related to each hub, and a sat_ entity per satellite keyed on its parent's key.

import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon } from "@mantine/core";
import { Trash2 } from "lucide-react";
import { usePanelDirty } from "../../../components/list/panelLeave";
import { FieldGroup, FormEntry, FormLayout } from "../../../components/form/FormLayout";
import type { Domain } from "../../../types/admin";
import { ModelReview } from "./ModelReview";
import { ChoiceField, ChoicesField, DomainField, TextField } from "./pickers";
import { useColumnOptions, useModelTables, type ModelTables } from "./useModelTables";
import {
  planVault,
  type Grouping,
  type HistoryDepth,
  type VaultAnswers,
  type VaultHub,
  type VaultLink,
  type VaultRule,
  type VaultSatellite,
} from "./plan";
import { AddButton, ListHead, StepHead, WizardFooter } from "./WizardFrame";

const STEPS = ["hubs", "links", "satellites", "rules", "review"] as const;
const GROUPINGS: Grouping[] = ["source", "rateOfChange", "sensitivity"];
const DEPTHS: HistoryDepth[] = ["full", "current"];
const ENDS = ["never", "effectivity"] as const;

let _uid = 0;
const uid = (p: string) => `${p}${++_uid}`;
const newHub = (): VaultHub => ({
  uid: uid("h"),
  concept: "",
  source: null,
  businessKey: [],
  sameAsSource: null,
  sameAsMaster: null,
  sameAsDuplicate: null,
});
const newLink = (): VaultLink => ({
  uid: uid("l"),
  name: "",
  source: null,
  hubs: [],
  via: {},
  ends: false,
  validTime: null,
  recordedTime: null,
});
const newSatellite = (): VaultSatellite => ({
  uid: uid("s"),
  name: "",
  parent: "",
  source: null,
  parentKey: [],
  attributes: [],
  groupedBy: "source",
  multiActive: [],
  validTime: null,
  recordedTime: null,
});
const newRule = (): VaultRule => ({ uid: uid("r"), target: "", text: "" });

const EMPTY: VaultAnswers = {
  domainId: "",
  hubs: [],
  links: [],
  historyDepth: "full",
  satellites: [],
  rules: [],
};

/** What a step still needs before Next, as the problem to show; null when it is answered. */
function missing(step: (typeof STEPS)[number], a: VaultAnswers): string | null {
  switch (step) {
    case "hubs":
      return a.domainId &&
        a.hubs.length > 0 &&
        a.hubs.every(
          (h) =>
            h.concept.trim() &&
            h.source &&
            h.businessKey.length &&
            // A same-as link is all three or none.
            [h.sameAsSource, h.sameAsMaster, h.sameAsDuplicate].filter(Boolean).length % 3 === 0,
        )
        ? null
        : "hubs";
    case "links":
      return a.links.every(
        (l) =>
          l.source &&
          l.hubs.length >= 2 &&
          l.hubs.every((h) => l.via[h]) &&
          (!l.ends || (l.validTime && l.recordedTime)),
      )
        ? null
        : "links";
    case "satellites":
      return a.satellites.every(
        (s) =>
          s.parent &&
          s.source &&
          s.parentKey.length &&
          s.attributes.length &&
          (a.historyDepth === "current" || (s.validTime && s.recordedTime)),
      )
        ? null
        : "satellites";
    case "rules":
      return a.rules.every((r) => !r.text.trim() || r.target) ? null : "rules";
    default:
      return null;
  }
}

function RemoveButton({ onClick, testId }: { onClick: () => void; testId: string }) {
  const { t } = useTranslation();
  return (
    <ActionIcon
      variant="subtle"
      color="red"
      onClick={onClick}
      aria-label={t("modelingWizard.remove")}
      data-testid={testId}
    >
      <Trash2 size={16} />
    </ActionIcon>
  );
}

function HubEntry({
  hub,
  index,
  tables,
  onChange,
  onRemove,
}: {
  hub: VaultHub;
  index: number;
  tables: ModelTables;
  onChange: (h: VaultHub) => void;
  onRemove: () => void;
}) {
  const { t } = useTranslation();
  const cols = useColumnOptions(tables.columnsOf(hub.source));
  const sameAsCols = useColumnOptions(tables.columnsOf(hub.sameAsSource));
  const set = (patch: Partial<VaultHub>) => onChange({ ...hub, ...patch });
  return (
    <FormEntry data-testid={`vault-hub-${index}`}>
      <FieldGroup row className="form-field-group--bare">
        <TextField
          label={t("modelingWizard.vault.concept")}
          help={t("modelingWizard.vault.conceptHelp")}
          required
          testId={`vault-hub-concept-${index}`}
          value={hub.concept}
          onChange={(concept) => set({ concept })}
          placeholder="customer"
        />
        <RemoveButton onClick={onRemove} testId={`vault-hub-remove-${index}`} />
      </FieldGroup>
      <FieldGroup>
        <ChoiceField
          label={t("modelingWizard.vault.recordSource")}
          help={t("modelingWizard.vault.recordSourceHelp")}
          required
          size="long"
          testId={`vault-hub-source-${index}`}
          data={tables.sourceOptions}
          value={hub.source}
          onChange={(source) => set({ source, businessKey: [] })}
        />
        <ChoicesField
          label={t("tableEditForm.businessKeyLabel")}
          help={t("modelingWizard.vault.businessKeyHelp")}
          hint={t("modelingForm.keyHint")}
          required
          testId={`vault-hub-key-${index}`}
          data={cols.keys}
          value={hub.businessKey}
          onChange={(businessKey) => set({ businessKey })}
          disabled={!hub.source}
          placeholder={hub.source ? undefined : t("modelingForm.selectSourceFirst")}
        />
      </FieldGroup>
      <FieldGroup>
        <ChoiceField
          label={t("modelingWizard.vault.sameAsSource")}
          help={t("modelingWizard.vault.sameAsHelp")}
          size="long"
          testId={`vault-hub-sameas-source-${index}`}
          data={tables.sourceOptions}
          value={hub.sameAsSource}
          onChange={(sameAsSource) =>
            set({ sameAsSource, sameAsMaster: null, sameAsDuplicate: null })
          }
          placeholder={t("modelingWizard.vault.sameAsNone")}
        />
        {hub.sameAsSource && (
          <>
            <ChoiceField
              label={t("modelingWizard.vault.sameAsMaster")}
              required
              testId={`vault-hub-sameas-master-${index}`}
              data={sameAsCols.keys}
              value={hub.sameAsMaster}
              onChange={(sameAsMaster) => set({ sameAsMaster })}
            />
            <ChoiceField
              label={t("modelingWizard.vault.sameAsDuplicate")}
              required
              testId={`vault-hub-sameas-duplicate-${index}`}
              data={sameAsCols.keys}
              value={hub.sameAsDuplicate}
              onChange={(sameAsDuplicate) => set({ sameAsDuplicate })}
            />
          </>
        )}
      </FieldGroup>
    </FormEntry>
  );
}

function LinkEntry({
  link,
  index,
  hubs,
  tables,
  onChange,
  onRemove,
}: {
  link: VaultLink;
  index: number;
  hubs: VaultHub[];
  tables: ModelTables;
  onChange: (l: VaultLink) => void;
  onRemove: () => void;
}) {
  const { t } = useTranslation();
  const cols = useColumnOptions(tables.columnsOf(link.source));
  const set = (patch: Partial<VaultLink>) => onChange({ ...link, ...patch });
  const hubOptions = hubs.map((h) => ({ value: h.uid, label: h.concept }));
  return (
    <FormEntry data-testid={`vault-link-${index}`}>
      <FieldGroup row className="form-field-group--bare">
        <TextField
          label={t("modelingWizard.vault.linkName")}
          help={t("modelingWizard.vault.linkNameHelp")}
          testId={`vault-link-name-${index}`}
          value={link.name}
          onChange={(name) => set({ name })}
          placeholder="customer order"
        />
        <RemoveButton onClick={onRemove} testId={`vault-link-remove-${index}`} />
      </FieldGroup>
      <FieldGroup>
        <ChoiceField
          label={t("modelingWizard.vault.linkSource")}
          help={t("modelingWizard.vault.linkSourceHelp")}
          required
          size="long"
          testId={`vault-link-source-${index}`}
          data={tables.sourceOptions}
          value={link.source}
          onChange={(source) => set({ source, via: {}, validTime: null, recordedTime: null })}
        />
        <ChoicesField
          label={t("modelingWizard.vault.linkHubs")}
          help={t("modelingWizard.vault.linkHubsHelp")}
          required
          testId={`vault-link-hubs-${index}`}
          data={hubOptions}
          value={link.hubs}
          onChange={(ids) => set({ hubs: ids })}
        />
      </FieldGroup>
      {link.hubs.length > 0 && (
        <FieldGroup>
          {link.hubs.map((h) => (
            <ChoiceField
              key={h}
              label={t("modelingWizard.vault.linkVia", {
                hub: hubs.find((x) => x.uid === h)?.concept ?? "",
              })}
              required
              testId={`vault-link-via-${index}-${h}`}
              data={cols.keys}
              value={link.via[h] ?? null}
              onChange={(v) => set({ via: { ...link.via, [h]: v ?? "" } })}
              disabled={!link.source}
              placeholder={link.source ? undefined : t("modelingForm.selectSourceFirst")}
            />
          ))}
        </FieldGroup>
      )}
      <FieldGroup>
        <ChoiceField
          label={t("modelingWizard.vault.endsLabel")}
          help={t("modelingWizard.vault.endsHelp")}
          required
          testId={`vault-link-ends-${index}`}
          data={ENDS.map((e) => ({ value: e, label: t(`modelingWizard.vault.ends.${e}`) }))}
          value={link.ends ? "effectivity" : "never"}
          onChange={(v) => set({ ends: v === "effectivity" })}
        />
        {link.ends && (
          <>
            <ChoiceField
              label={t("modelingWizard.validTime")}
              help={t("modelingWizard.validTimeHelp")}
              required
              testId={`vault-link-valid-${index}`}
              data={cols.all}
              value={link.validTime}
              onChange={(validTime) => set({ validTime })}
            />
            <ChoiceField
              label={t("modelingWizard.recordedTime")}
              help={t("modelingWizard.recordedTimeHelp")}
              required
              testId={`vault-link-recorded-${index}`}
              data={cols.all}
              value={link.recordedTime}
              onChange={(recordedTime) => set({ recordedTime })}
            />
          </>
        )}
      </FieldGroup>
    </FormEntry>
  );
}

function SatelliteEntry({
  sat,
  index,
  parents,
  keepsHistory,
  tables,
  onChange,
  onRemove,
}: {
  sat: VaultSatellite;
  index: number;
  parents: { value: string; label: string }[];
  keepsHistory: boolean;
  tables: ModelTables;
  onChange: (s: VaultSatellite) => void;
  onRemove: () => void;
}) {
  const { t } = useTranslation();
  const cols = useColumnOptions(tables.columnsOf(sat.source));
  const set = (patch: Partial<VaultSatellite>) => onChange({ ...sat, ...patch });
  const noSource = !sat.source;
  const pickFirst = noSource ? t("modelingForm.selectSourceFirst") : undefined;
  return (
    <FormEntry data-testid={`vault-sat-${index}`}>
      <FieldGroup row className="form-field-group--bare">
        <ChoiceField
          label={t("modelingWizard.vault.satParent")}
          help={t("modelingWizard.vault.satParentHelp")}
          required
          testId={`vault-sat-parent-${index}`}
          data={parents}
          value={sat.parent || null}
          onChange={(parent) => set({ parent: parent ?? "" })}
        />
        <TextField
          label={t("modelingWizard.vault.satName")}
          help={t("modelingWizard.vault.satNameHelp")}
          testId={`vault-sat-name-${index}`}
          value={sat.name}
          onChange={(name) => set({ name })}
          placeholder="crm"
        />
        <RemoveButton onClick={onRemove} testId={`vault-sat-remove-${index}`} />
      </FieldGroup>
      <FieldGroup>
        <ChoiceField
          label={t("modelingWizard.vault.satSource")}
          help={t("modelingWizard.vault.satSourceHelp")}
          required
          size="long"
          testId={`vault-sat-source-${index}`}
          data={tables.sourceOptions}
          value={sat.source}
          onChange={(source) =>
            set({
              source,
              parentKey: [],
              attributes: [],
              multiActive: [],
              validTime: null,
              recordedTime: null,
            })
          }
        />
        <ChoicesField
          label={t("modelingWizard.vault.satParentKey")}
          help={t("modelingWizard.vault.satParentKeyHelp")}
          required
          testId={`vault-sat-key-${index}`}
          data={cols.keys}
          value={sat.parentKey}
          onChange={(parentKey) => set({ parentKey })}
          disabled={noSource}
          placeholder={pickFirst}
        />
        <ChoicesField
          label={t("modelingForm.attributesLabel")}
          help={t("modelingWizard.vault.satAttributesHelp")}
          required
          testId={`vault-sat-attributes-${index}`}
          data={cols.all}
          value={sat.attributes}
          onChange={(attributes) => set({ attributes })}
          disabled={noSource}
          placeholder={pickFirst}
        />
      </FieldGroup>
      <FieldGroup>
        <ChoiceField
          label={t("modelingWizard.vault.groupedBy")}
          help={t("modelingWizard.vault.groupedByHelp")}
          required
          testId={`vault-sat-grouping-${index}`}
          data={GROUPINGS.map((g) => ({
            value: g,
            label: t(`modelingWizard.vault.grouping.${g}`),
          }))}
          value={sat.groupedBy}
          onChange={(v) => set({ groupedBy: (v as Grouping | null) ?? "source" })}
        />
        <ChoicesField
          label={t("modelingWizard.vault.multiActive")}
          help={t("modelingWizard.vault.multiActiveHelp")}
          size="medium"
          testId={`vault-sat-multiactive-${index}`}
          data={cols.keys}
          value={sat.multiActive}
          onChange={(multiActive) => set({ multiActive })}
          disabled={noSource}
          placeholder={noSource ? pickFirst : t("modelingWizard.vault.multiActiveNone")}
        />
      </FieldGroup>
      {keepsHistory && (
        <FieldGroup>
          <ChoiceField
            label={t("modelingWizard.validTime")}
            help={t("modelingWizard.validTimeHelp")}
            required
            testId={`vault-sat-valid-${index}`}
            data={cols.all}
            value={sat.validTime}
            onChange={(validTime) => set({ validTime })}
            disabled={noSource}
          />
          <ChoiceField
            label={t("modelingWizard.recordedTime")}
            help={t("modelingWizard.recordedTimeHelp")}
            required
            testId={`vault-sat-recorded-${index}`}
            data={cols.all}
            value={sat.recordedTime}
            onChange={(recordedTime) => set({ recordedTime })}
            disabled={noSource}
          />
        </FieldGroup>
      )}
    </FormEntry>
  );
}

export function DataVaultWizard({
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
  const [a, setA] = useState<VaultAnswers>(EMPTY);
  const [step, setStep] = useState(0);
  const [problem, setProblem] = useState<string | null>(null);
  const [created, setCreated] = useState(false);
  const set = (patch: Partial<VaultAnswers>) => setA((prev) => ({ ...prev, ...patch }));
  const plan = useMemo(() => planVault(a, t), [a, t]);
  usePanelDirty(!created && (a.hubs.length > 0 || step > 0), a.hubs[0]?.concept ?? "");

  const linkLabel = (l: VaultLink) =>
    l.name.trim() || l.hubs.map((h) => a.hubs.find((x) => x.uid === h)?.concept ?? "").join(" ");
  const parents = [
    ...a.hubs.map((h) => ({
      value: h.uid,
      label: t("modelingWizard.vault.parentHub", { name: h.concept }),
    })),
    ...a.links.map((l) => ({
      value: l.uid,
      label: t("modelingWizard.vault.parentLink", { name: linkLabel(l) }),
    })),
  ];
  const ruleTargets = [
    ...parents,
    ...a.satellites.map((s) => ({
      value: s.uid,
      label: t("modelingWizard.vault.parentSatellite", {
        name: s.name || parents.find((p) => p.value === s.parent)?.label || "",
      }),
    })),
  ];

  const id = STEPS[step];
  const next = () => {
    const gap = missing(id, a);
    if (gap) {
      setProblem(t(`modelingWizard.vault.missing.${gap}`));
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

  return (
    <FormLayout>
      <StepHead
        index={step}
        count={STEPS.length}
        title={t(`modelingWizard.vault.${id}.title`)}
        intro={t(`modelingWizard.vault.${id}.intro`)}
        testId={`wizard-step-${id}`}
      />
      {id === "hubs" && (
        <>
          <DomainField
            domains={domains}
            value={a.domainId}
            onChange={(domainId) => set({ domainId })}
            testId="vault-domain"
          />
          <ListHead
            text={t("modelingWizard.vault.hubsLabel")}
            help={t("modelingWizard.vault.hubsHelp")}
          />
          {a.hubs.map((h, i) => (
            <HubEntry
              key={h.uid}
              hub={h}
              index={i}
              tables={tables}
              onChange={(nh) => set({ hubs: a.hubs.map((x) => (x.uid === h.uid ? nh : x)) })}
              onRemove={() =>
                set({
                  hubs: a.hubs.filter((x) => x.uid !== h.uid),
                  links: a.links.map((l) => ({ ...l, hubs: l.hubs.filter((x) => x !== h.uid) })),
                  satellites: a.satellites.filter((s) => s.parent !== h.uid),
                })
              }
            />
          ))}
          <AddButton onClick={() => set({ hubs: [...a.hubs, newHub()] })} testId="vault-hub-add">
            {t("modelingWizard.vault.addHub")}
          </AddButton>
        </>
      )}
      {id === "links" && (
        <>
          <ListHead
            text={t("modelingWizard.vault.linksLabel")}
            help={t("modelingWizard.vault.linksHelp")}
          />
          {a.links.map((l, i) => (
            <LinkEntry
              key={l.uid}
              link={l}
              index={i}
              hubs={a.hubs}
              tables={tables}
              onChange={(nl) => set({ links: a.links.map((x) => (x.uid === l.uid ? nl : x)) })}
              onRemove={() =>
                set({
                  links: a.links.filter((x) => x.uid !== l.uid),
                  satellites: a.satellites.filter((s) => s.parent !== l.uid),
                })
              }
            />
          ))}
          <AddButton
            onClick={() => set({ links: [...a.links, newLink()] })}
            testId="vault-link-add"
          >
            {t("modelingWizard.vault.addLink")}
          </AddButton>
        </>
      )}
      {id === "satellites" && (
        <>
          <ChoiceField
            label={t("modelingWizard.vault.depthLabel")}
            help={t("modelingWizard.vault.depthHelp")}
            required
            testId="vault-history-depth"
            data={DEPTHS.map((d) => ({ value: d, label: t(`modelingWizard.vault.depth.${d}`) }))}
            value={a.historyDepth}
            onChange={(v) => set({ historyDepth: (v as HistoryDepth | null) ?? "full" })}
          />
          <ListHead
            text={t("modelingWizard.vault.satellitesLabel")}
            help={t("modelingWizard.vault.satellitesHelp")}
          />
          {a.satellites.map((s, i) => (
            <SatelliteEntry
              key={s.uid}
              sat={s}
              index={i}
              parents={parents}
              keepsHistory={a.historyDepth === "full"}
              tables={tables}
              onChange={(ns) =>
                set({ satellites: a.satellites.map((x) => (x.uid === s.uid ? ns : x)) })
              }
              onRemove={() => set({ satellites: a.satellites.filter((x) => x.uid !== s.uid) })}
            />
          ))}
          <AddButton
            onClick={() => set({ satellites: [...a.satellites, newSatellite()] })}
            testId="vault-sat-add"
          >
            {t("modelingWizard.vault.addSatellite")}
          </AddButton>
        </>
      )}
      {id === "rules" && (
        <>
          <ListHead
            text={t("modelingWizard.vault.rulesLabel")}
            help={t("modelingWizard.vault.rulesHelp")}
          />
          {a.rules.map((r, i) => (
            <FieldGroup row className="form-field-group--bare" key={r.uid}>
              <ChoiceField
                label={t("modelingWizard.vault.ruleTarget")}
                required
                testId={`vault-rule-target-${i}`}
                data={ruleTargets}
                value={r.target || null}
                onChange={(target) =>
                  set({
                    rules: a.rules.map((x) =>
                      x.uid === r.uid ? { ...x, target: target ?? "" } : x,
                    ),
                  })
                }
              />
              <TextField
                label={t("modelingWizard.vault.ruleText")}
                size="long"
                testId={`vault-rule-text-${i}`}
                value={r.text}
                onChange={(text) =>
                  set({ rules: a.rules.map((x) => (x.uid === r.uid ? { ...x, text } : x)) })
                }
                placeholder={t("modelingWizard.vault.ruleTextPlaceholder")}
              />
              <RemoveButton
                onClick={() => set({ rules: a.rules.filter((x) => x.uid !== r.uid) })}
                testId={`vault-rule-remove-${i}`}
              />
            </FieldGroup>
          ))}
          <AddButton
            onClick={() => set({ rules: [...a.rules, newRule()] })}
            testId="vault-rule-add"
          >
            {t("modelingWizard.vault.addRule")}
          </AddButton>
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
