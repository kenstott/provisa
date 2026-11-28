// Copyright (c) 2026 Kenneth Stott
// Canary: 740e92b6-5eeb-4322-9f00-c45ca986961c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2147: the profiler's classification kinds, each with its test, the share of values that must
// pass, whether it is sensitive and the mask it offers. A disabled kind is not tested by any
// profile run and raises no suggestions. (Amended 2026-11-25) The org adds its own custom kinds --
// a name, a regular expression with an optional shipped check, the share, sensitivity, mask and
// glossary term -- and edits and deletes them; a shipped kind keeps its coded test, but its share,
// sensitivity and mask are the org's, and it can be copied into a custom kind to change the test.

import { useCallback, useEffect, useState } from "react";
import {
  ActionIcon,
  Alert,
  Badge,
  Button,
  Code,
  Group,
  Loader,
  NumberInput,
  Select,
  Stack,
  Switch,
  Table,
  Text,
  TextInput,
} from "@mantine/core";
import { Pencil, ScanSearch, X } from "lucide-react";
import { useTranslation } from "react-i18next";
import {
  changeClassificationKind,
  createClassificationKind,
  deleteClassificationKind,
  fetchClassificationKinds,
  type ClassificationKind,
  type CustomKindInput,
} from "../../api/profiler";
import { fetchContextClassification, type ContextPlace } from "../../api/contextViews";
import { useCapability } from "../../hooks/useCapability";
import { ContextPlaces } from "../ContextPlaces";
import { contextPlaceTexts } from "../contextPlaceTexts";
import { PageTitle } from "../PageTitle";
import { Tip } from "../Tip";
import { DetailFields } from "../detail/DetailFields";
import { EntityPageLink } from "../EntityLink";
import { FormChecks, FormField, FormFooter, FormIdentifiers, FormLayout } from "../form/FormLayout";
import { ListRow } from "../list/ListTable";
import { PanelDelete } from "../list/PanelDelete";
import { PanelDuplicate } from "../list/PanelDuplicate";
import { SortGroupTable } from "../list/SortGroupTable";
import { PanelActions, SplitPane } from "../list/SplitPane";
import { useRememberedDetail } from "../list/useDetailParam";
import { detailListOf, searchNarrowing, shownBy } from "../list/leftOut";
import { useUrlFilter } from "../list/useUrlFilter";
import { FilterInput } from "./FilterInput";
import { ColumnRules } from "./ColumnRules";

/** A kind as either read answers it: the Admin list, or the Context page's, which adds where the
 * kind was detected (REQ-2259). */
type KindRow = ClassificationKind & {
  detections?: ContextPlace[];
  hiddenDetections?: number;
};

/** What both reads answer: the kinds, and what a custom kind's form offers. */
interface KindsRead {
  kinds: KindRow[];
  checks: string[];
  masks: string[];
}

/** The Context read answers every kind with where it was detected; one without is a defect. */
function detectedOn(k: KindRow): { places: ContextPlace[]; hidden: number } {
  if (k.detections === undefined || k.hiddenDetections === undefined) {
    throw new Error(`classification kind ${k.id} came without where it was detected`);
  }
  return { places: k.detections, hidden: k.hiddenDetections };
}

function message(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

/** The form's fields, as typed. The share is a percentage. */
interface Draft {
  label: string;
  pattern: string;
  check: string | null;
  ignore: string;
  percent: number | string;
  sensitive: boolean;
  maskPreset: string | null;
  term: string;
  enabled: boolean;
}

type DraftErrors = Partial<Record<keyof Draft, string>>;

const EMPTY: Draft = {
  label: "",
  pattern: "",
  check: null,
  ignore: "",
  percent: 90,
  sensitive: false,
  maskPreset: null,
  term: "",
  enabled: true,
};

function draftOf(k: KindRow): Draft {
  return {
    label: k.label,
    pattern: k.pattern ?? "",
    check: k.check,
    ignore: k.ignore ?? "",
    percent: Math.round(k.threshold * 100),
    sensitive: k.sensitive,
    maskPreset: k.maskPreset,
    term: k.term,
    enabled: k.enabled,
  };
}

/** The panel holds one thing: a kind read, a kind in its form, or the create form. */
type Mode = { form: "none" } | { form: "edit" } | { form: "create" };

/** REQ-2277: the standard page anatomy -- the header (title, filter, "+ Kind"), the list, and a
 * row opening the kind's detail panel (?kind=<id>), read, with Edit, Duplicate and (for a custom
 * kind) Delete in the panel's head. The kinds are changed by the right a classification decision
 * asks (sensitive_data, REQ-1327); a caller without it reads them. REQ-2259: ``detections`` is the
 * Context group's Classification page, which also lists where each kind was detected. */
export function ClassificationKindsTab({
  detections = false,
  title,
}: {
  detections?: boolean;
  title: string;
}) {
  const { t } = useTranslation();
  const canEdit = useCapability("sensitive_data");
  const [read, setRead] = useState<KindsRead | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useUrlFilter();
  const kinds = read?.kinds ?? null;
  const kindLabel = (k: KindRow) => (k.custom ? k.label : t(`classificationKinds.kind.${k.id}`));
  const checkLabel = (check: string) =>
    t("classificationKinds.checkOption", { kind: t(`classificationKinds.kind.${check}`) });
  const testText = (k: KindRow) =>
    k.custom
      ? [k.pattern ?? "", k.check === null ? null : checkLabel(k.check)]
          .filter((part) => part !== null)
          .join(" + ")
      : t(`classificationKinds.test.${k.id}`);
  const maskLabel = (k: KindRow) =>
    k.maskPreset ? t(`maskList.preset.${k.maskPreset}`) : t("classificationKinds.noMask");
  const enabledLabel = (k: KindRow) =>
    t(k.enabled ? "contextViews.enabled" : "contextViews.disabled");

  const detectedTexts = (k: KindRow) => {
    const { places, hidden } = detectedOn(k);
    return contextPlaceTexts(places, hidden, t);
  };
  // Every text a kind's row shows, and its id.
  const kindRowTexts = (k: KindRow) => [
    k.id,
    kindLabel(k),
    k.custom ? t("classificationKinds.custom") : "",
    k.sensitive ? t("classificationKinds.sensitive") : "",
    testText(k),
    `${Math.round(k.threshold * 100)}%`,
    maskLabel(k),
    enabledLabel(k),
    ...(detections ? detectedTexts(k) : []),
  ];
  // REQ-2277 rule 9: a kind is open only while the search shows its row.
  const narrowings = [searchNarrowing<KindRow>(t, search, () => setSearch(""), kindRowTexts)];
  const detail = useRememberedDetail(
    "classification",
    detailListOf(t, kinds, (k) => k.id, kindLabel, narrowings),
  );
  const [mode, setMode] = useState<Mode>({ form: "none" });
  const [draft, setDraft] = useState<Draft>(EMPTY);
  const [errors, setErrors] = useState<DraftErrors>({});
  const [saving, setSaving] = useState(false);
  const [problem, setProblem] = useState<string | null>(null);

  const load = useCallback(
    () => (detections ? fetchContextClassification() : fetchClassificationKinds()).then(setRead),
    [detections],
  );

  useEffect(() => {
    load().catch((e: unknown) => setError(message(e)));
  }, [load]);

  const open = kinds?.find((k) => k.id === detail.id) ?? null;
  const shown = shownBy(kinds ?? [], narrowings);

  const closeForm = () => {
    setMode({ form: "none" });
    setErrors({});
    setProblem(null);
  };

  const openCreate = () => {
    // One thing in the panel at a time: the create form replaces whatever kind was open.
    detail.close();
    setDraft(EMPTY);
    setErrors({});
    setProblem(null);
    setMode({ form: "create" });
  };

  // Duplicate: the create form filled from the kind under "<name> (copy)" -- the one way to change
  // a shipped kind's test. What depends on the kind (its decisions) is not copied.
  const openDuplicate = (k: KindRow) => {
    detail.close();
    setDraft({
      ...draftOf(k),
      label: t("classificationKinds.copyName", { kind: kindLabel(k) }),
      enabled: true,
    });
    setErrors({});
    setProblem(null);
    setMode({ form: "create" });
  };

  const openEdit = (k: KindRow) => {
    setDraft(draftOf(k));
    setErrors({});
    setProblem(null);
    setMode({ form: "edit" });
  };

  const set = <K extends keyof Draft>(key: K, value: Draft[K]) =>
    setDraft((d) => ({ ...d, [key]: value }));

  /** The draft's required fields, checked on Save (REQ-2277: never by disabling Save). */
  const check = (custom: boolean): DraftErrors => {
    const out: DraftErrors = {};
    const required = t("classificationKinds.required");
    if (custom) {
      if (!draft.label.trim()) out.label = required;
      if (!draft.pattern.trim()) out.pattern = required;
      if (!draft.term.trim()) out.term = required;
    }
    const percent = Number(draft.percent);
    if (draft.percent === "" || !(percent > 0 && percent <= 100)) {
      out.percent = t("classificationKinds.percentRange");
    }
    if (draft.sensitive && draft.maskPreset === null) {
      out.maskPreset = t("classificationKinds.maskRequired");
    }
    return out;
  };

  const settings = () => ({
    threshold: Number(draft.percent) / 100,
    sensitive: draft.sensitive,
    // A mask is a sensitive kind's: a kind made not sensitive offers none.
    maskPreset: draft.sensitive ? draft.maskPreset : null,
  });

  const customFields = (): CustomKindInput => ({
    label: draft.label.trim(),
    pattern: draft.pattern.trim(),
    check: draft.check,
    ignore: draft.ignore.trim() === "" ? null : draft.ignore.trim(),
    term: draft.term.trim(),
    ...settings(),
  });

  const save = async () => {
    const custom = mode.form === "create" || open?.custom === true;
    const found = check(custom);
    setErrors(found);
    if (Object.keys(found).length > 0) return;
    setSaving(true);
    setProblem(null);
    try {
      if (mode.form === "create") {
        const made = await createClassificationKind(customFields());
        await load();
        setMode({ form: "none" });
        detail.open(made.id);
      } else if (open !== null) {
        await changeClassificationKind(open.id, {
          ...(open.custom ? customFields() : settings()),
          enabled: draft.enabled,
        });
        await load();
        setMode({ form: "none" });
      }
    } catch (e: unknown) {
      setProblem(message(e));
    } finally {
      setSaving(false);
    }
  };

  const remove = async (k: KindRow) => {
    await deleteClassificationKind(k.id);
    await load();
  };

  const form = (custom: boolean, shipped: KindRow | null) => (
    <FormLayout data-testid="classification-kind-form">
      {custom && (
        <>
          <FormIdentifiers>
            <FormField hint={t("classificationKinds.nameHint")} size="medium">
              <TextInput
                label={t("classificationKinds.nameLabel")}
                withAsterisk
                value={draft.label}
                error={errors.label}
                onChange={(e) => set("label", e.currentTarget.value)}
                data-testid="classification-kind-name"
              />
            </FormField>
          </FormIdentifiers>
          <FormField hint={t("classificationKinds.patternHint")} size="long">
            <TextInput
              label={t("classificationKinds.patternLabel")}
              withAsterisk
              value={draft.pattern}
              error={errors.pattern}
              onChange={(e) => set("pattern", e.currentTarget.value)}
              styles={{ input: { fontFamily: "var(--mantine-font-family-monospace)" } }}
              data-testid="classification-kind-pattern"
            />
          </FormField>
          <FormField hint={t("classificationKinds.checkHint")} size="medium">
            <Select
              label={t("classificationKinds.checkLabel")}
              placeholder={t("classificationKinds.checkNone")}
              clearable
              data={(read?.checks ?? []).map((c) => ({ value: c, label: checkLabel(c) }))}
              value={draft.check}
              onChange={(v) => set("check", v)}
              data-testid="classification-kind-check"
            />
          </FormField>
          <FormField hint={t("classificationKinds.ignoreHint")} size="medium">
            <TextInput
              label={t("classificationKinds.ignoreLabel")}
              value={draft.ignore}
              onChange={(e) => set("ignore", e.currentTarget.value)}
              styles={{ input: { fontFamily: "var(--mantine-font-family-monospace)" } }}
              data-testid="classification-kind-ignore"
            />
          </FormField>
        </>
      )}
      {shipped !== null && (
        <Text size="sm" c="dimmed" data-testid="classification-kind-shipped-test">
          {t("classificationKinds.shippedTestNote", { test: testText(shipped) })}
        </Text>
      )}
      <FormField hint={t("classificationKinds.thresholdHint")} size="short">
        <NumberInput
          label={t("classificationKinds.thresholdHeader")}
          withAsterisk
          suffix="%"
          min={1}
          max={100}
          value={draft.percent}
          error={errors.percent}
          onChange={(v) => set("percent", v)}
          data-testid="classification-kind-threshold"
        />
      </FormField>
      <FormField hint={t("classificationKinds.maskHint")} size="medium">
        <Select
          label={t("classificationKinds.maskHeader")}
          withAsterisk={draft.sensitive}
          placeholder={t("classificationKinds.noMask")}
          disabled={!draft.sensitive}
          data={(read?.masks ?? []).map((m) => ({ value: m, label: t(`maskList.preset.${m}`) }))}
          value={draft.sensitive ? draft.maskPreset : null}
          error={errors.maskPreset}
          onChange={(v) => set("maskPreset", v)}
          data-testid="classification-kind-mask"
        />
      </FormField>
      {custom && (
        <FormField hint={t("classificationKinds.termHint")} size="medium">
          <TextInput
            label={t("classificationKinds.termLabel")}
            withAsterisk
            value={draft.term}
            error={errors.term}
            onChange={(e) => set("term", e.currentTarget.value)}
            data-testid="classification-kind-term"
          />
        </FormField>
      )}
      <FormChecks>
        <FormField help={t("classificationKinds.sensitiveHint")} size="medium">
          <Switch
            label={t("classificationKinds.sensitive")}
            checked={draft.sensitive}
            disabled={saving}
            onChange={(e) => set("sensitive", e.currentTarget.checked)}
            data-testid="classification-kind-sensitive"
          />
        </FormField>
        {mode.form === "edit" && open !== null && (
          <FormField size="medium">
            <Switch
              label={t("classificationKinds.enabledHeader")}
              checked={draft.enabled}
              disabled={saving}
              onChange={(e) => set("enabled", e.currentTarget.checked)}
              aria-label={t("classificationKinds.enabledAria", { kind: kindLabel(open) })}
              data-testid={`classification-kind-enabled-${open.id}`}
            />
          </FormField>
        )}
      </FormChecks>
      <FormFooter
        panel
        onSave={save}
        onCancel={closeForm}
        saving={saving}
        problem={problem ?? undefined}
        saveProps={{ "data-testid": "classification-kind-save" }}
      />
    </FormLayout>
  );

  const readView = (k: KindRow) => (
    <Stack gap="sm" data-testid={`classification-kind-detail-${k.id}`}>
      {canEdit && (
        <PanelActions>
          <fieldset className="panel-action-set">
            <Group gap="xs">
              <Tip text={t("classificationKinds.editAria", { kind: kindLabel(k) })} named>
                <ActionIcon
                  aria-label={t("classificationKinds.editAria", { kind: kindLabel(k) })}
                  data-testid={`classification-kind-edit-${k.id}`}
                  onClick={(e) => {
                    e.stopPropagation();
                    openEdit(k);
                  }}
                >
                  <Pencil size={14} />
                </ActionIcon>
              </Tip>
              {k.pattern !== null && (
                <PanelDuplicate
                  name={kindLabel(k)}
                  onDuplicate={() => openDuplicate(k)}
                  testId={`classification-kind-duplicate-${k.id}`}
                />
              )}
              {k.custom && (
                <PanelDelete
                  itemKey={k.id}
                  title={t("classificationKinds.deleteTitle", { kind: kindLabel(k) })}
                  consequence={t("classificationKinds.deleteConfirm", { kind: kindLabel(k) })}
                  ariaLabel={t("classificationKinds.deleteTitle", { kind: kindLabel(k) })}
                  testId={`classification-kind-delete-${k.id}`}
                  onDelete={() => remove(k)}
                />
              )}
            </Group>
          </fieldset>
        </PanelActions>
      )}
      <DetailFields
        testId={`classification-kind-read-${k.id}`}
        fields={[
          { label: t("classificationKinds.kindHeader"), value: kindLabel(k) },
          {
            label: t("classificationKinds.originHeader"),
            value: t(k.custom ? "classificationKinds.custom" : "classificationKinds.shipped"),
          },
          {
            label: t("classificationKinds.sensitive"),
            value: k.sensitive ? t("detailFields.yes") : t("detailFields.no"),
          },
          {
            label: t("classificationKinds.testHeader"),
            value: k.custom ? <Code>{testText(k)}</Code> : testText(k),
          },
          {
            label: t("classificationKinds.thresholdHeader"),
            value: `${Math.round(k.threshold * 100)}%`,
          },
          { label: t("classificationKinds.maskHeader"), value: maskLabel(k) },
          {
            label: t("classificationKinds.termLabel"),
            // REQ-2277 rule 6: the kind names its term by name, which a term of each domain may
            // take (glossary_repo._find_or_create_term), so it links to the Glossary narrowed by it.
            value: (
              <EntityPageLink
                href={`/context/glossary?search=${encodeURIComponent(k.term)}`}
                testId={`classification-kind-read-term-${k.id}`}
              >
                {k.term}
              </EntityPageLink>
            ),
          },
          { label: t("classificationKinds.enabledHeader"), value: enabledLabel(k) },
          ...(detections
            ? [
                {
                  label: t("contextViews.detectedHeader"),
                  value: (
                    <ContextPlaces
                      {...detectedOn(k)}
                      testId={`classification-kind-read-detected-${k.id}`}
                    />
                  ),
                },
              ]
            : []),
        ]}
      />
      {/* REQ-2278: the rules that hide or mask, per role, every column classified as the kind. */}
      <ColumnRules
        key={k.id}
        classification={k.id}
        readOnly={detections}
        testId={`classification-kind-column-rules-${k.id}`}
      />
    </Stack>
  );

  const creating = mode.form === "create";
  const panelTitle = creating
    ? t("classificationKinds.createTitle")
    : open === null
      ? ""
      : kindLabel(open);
  const panelBody = creating
    ? form(true, null)
    : open === null
      ? null
      : mode.form === "edit"
        ? form(open.custom, open.custom ? null : open)
        : readView(open);

  return (
    <>
      <div className="page-header">
        <PageTitle icon={ScanSearch}>{title}</PageTitle>
        <FilterInput
          value={search}
          onChange={setSearch}
          placeholder={t("classificationKinds.filterPlaceholder")}
          testId="classification-kinds-filter"
        />
        {canEdit && (
          <div className="page-actions">
            <Button
              variant={creating ? "outline" : "filled"}
              onClick={() => (creating ? closeForm() : openCreate())}
              data-testid="classification-kinds-create"
              aria-label={creating ? t("classificationKinds.closeForm") : undefined}
            >
              {creating ? <X size={14} /> : t("classificationKinds.addKind")}
            </Button>
          </div>
        )}
      </div>
      <Text size="sm" c="dimmed" mb="sm">
        {t("classificationKinds.intro")}
      </Text>
      {error && (
        <Alert color="red" data-testid="classification-kinds-error">
          {error}
        </Alert>
      )}
      {kinds == null && error == null && <Loader size="sm" />}
      {kinds != null && (
        <SplitPane
          outOfList={detail.outOfList}
          testId="classification-kinds-split"
          storageKey="provisa.classificationKinds.detailWidth"
          title={panelTitle}
          onClose={() => {
            closeForm();
            detail.close();
          }}
          // Escape closes a kind that is only being looked at; it never drops what is typed.
          closeOnEscape={mode.form === "none"}
          labels={{
            panel: t("classificationKinds.kindHeader"),
            resize: t("tablesPage.resizeDetail"),
            expand: t("tablesPage.expandDetail"),
            restore: t("tablesPage.restoreDetail"),
            close: t("tablesPage.closeDetail"),
            backToList: t("tablesPage.backToList"),
          }}
          list={
            // The table's own scroller is the list's direct child, so the list scrolls inside its
            // pane and its header stays under the panel's head (App.css .split-pane__list).
            <SortGroupTable
              testId="classification-kinds-list"
              emptyTestId="classification-kinds-empty"
              testPrefix="classification-kinds"
              minWidth={640}
              rows={shown}
              columns={[
                { key: "kind", label: t("classificationKinds.kindHeader"), sortValue: kindLabel },
                {
                  key: "test",
                  label: t("classificationKinds.testHeader"),
                  sortValue: testText,
                },
                {
                  key: "threshold",
                  label: t("classificationKinds.thresholdHeader"),
                  sortValue: (k) => k.threshold,
                },
                {
                  key: "mask",
                  label: t("classificationKinds.maskHeader"),
                  sortValue: maskLabel,
                  groupValue: maskLabel,
                },
                {
                  key: "enabled",
                  label: t("classificationKinds.enabledHeader"),
                  sortValue: enabledLabel,
                  groupValue: enabledLabel,
                },
              ]}
              headers={[
                { col: "kind" },
                { col: "test" },
                { col: "threshold" },
                { col: "mask" },
                { col: "enabled" },
                ...(detections ? [t("contextViews.detectedHeader")] : []),
              ]}
              colSpan={detections ? 6 : 5}
              rowKey={(k) => k.id}
              empty={t("classificationKinds.empty")}
              render={(k) => [
                <ListRow
                  key={k.id}
                  itemKey={k.id}
                  testId={`classification-kind-${k.id}`}
                  selected={!creating && k.id === detail.id}
                  onClick={() => {
                    closeForm();
                    if (detail.id === k.id) detail.close();
                    else detail.open(k.id);
                  }}
                >
                  <Table.Td>
                    <Group gap={4} wrap="nowrap">
                      {kindLabel(k)}
                      {k.custom && (
                        <Badge size="xs" variant="outline">
                          {t("classificationKinds.custom")}
                        </Badge>
                      )}
                      {k.sensitive && (
                        <Badge size="xs" color="red" variant="light">
                          {t("classificationKinds.sensitive")}
                        </Badge>
                      )}
                    </Group>
                  </Table.Td>
                  <Table.Td>{k.custom ? <Code>{testText(k)}</Code> : testText(k)}</Table.Td>
                  <Table.Td>{`${Math.round(k.threshold * 100)}%`}</Table.Td>
                  <Table.Td>{maskLabel(k)}</Table.Td>
                  <Table.Td>
                    <Text
                      size="sm"
                      c={k.enabled ? undefined : "dimmed"}
                      data-testid={`classification-kind-state-${k.id}`}
                    >
                      {enabledLabel(k)}
                    </Text>
                  </Table.Td>
                  {detections && (
                    <Table.Td>
                      <ContextPlaces
                        {...detectedOn(k)}
                        testId={`classification-kind-detected-${k.id}`}
                      />
                    </Table.Td>
                  )}
                </ListRow>,
              ]}
            />
          }
        >
          {panelBody}
        </SplitPane>
      )}
    </>
  );
}
