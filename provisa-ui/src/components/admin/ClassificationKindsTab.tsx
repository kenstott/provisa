// Copyright (c) 2026 Kenneth Stott
// Canary: 740e92b6-5eeb-4322-9f00-c45ca986961c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2147: the profiler's classification kinds, each with its test, its threshold, whether it is
// sensitive and the mask it offers. An organisation administrator enables or disables a kind here;
// a disabled kind is not tested by any profile run and raises no suggestions. Everything else about
// a kind is shipped with the product and shown as it is.

import { useEffect, useState } from "react";
import { ActionIcon, Alert, Badge, Group, Loader, Stack, Switch, Table, Text } from "@mantine/core";
import { Pencil, ScanSearch } from "lucide-react";
import { useTranslation } from "react-i18next";
import {
  fetchClassificationKinds,
  setEnabledClassificationKinds,
  type ClassificationKind,
} from "../../api/profiler";
import { fetchContextClassification, type ContextPlace } from "../../api/contextViews";
import { ContextPlaces } from "../ContextPlaces";
import { contextPlaceTexts } from "../contextPlaceTexts";
import { PageTitle } from "../PageTitle";
import { Tip } from "../Tip";
import { DetailFields } from "../detail/DetailFields";
import { FormChecks, FormField, FormFooter, FormLayout } from "../form/FormLayout";
import { ListRow } from "../list/ListTable";
import { SortGroupTable } from "../list/SortGroupTable";
import { PanelActions, SplitPane } from "../list/SplitPane";
import { useRememberedDetail } from "../list/useDetailParam";
import { useUrlFilter } from "../list/useUrlFilter";
import { matchesQuery } from "../../lib/searchItems";
import { FilterInput } from "./FilterInput";

/** A kind as either read answers it: the Admin list, or the Context page's, which adds where the
 * kind was detected (REQ-2259). */
type KindRow = Pick<
  ClassificationKind,
  "id" | "sensitive" | "threshold" | "maskPreset" | "enabled"
> & {
  detections?: ContextPlace[];
  hiddenDetections?: number;
};

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

/** REQ-2277: the standard page anatomy -- the header (title, filter), the list, and a row opening
 * the kind's detail panel (?kind=<id>), read, with Edit in the panel's head where the kind may be
 * switched. REQ-2259: ``readOnly`` is the Context group's Classification page -- the kinds and where
 * each was detected, with no Edit; enabling a kind stays in Admin. */
export function ClassificationKindsTab({
  readOnly = false,
  title,
}: {
  readOnly?: boolean;
  title: string;
}) {
  const { t } = useTranslation();
  const [kinds, setKinds] = useState<KindRow[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useUrlFilter();
  const detail = useRememberedDetail(
    "classification",
    kinds === null ? null : kinds.map((k) => k.id),
  );
  const [editing, setEditing] = useState(false);
  const [draftEnabled, setDraftEnabled] = useState(false);
  const [saving, setSaving] = useState(false);
  const [problem, setProblem] = useState<string | null>(null);

  useEffect(() => {
    (readOnly ? fetchContextClassification() : fetchClassificationKinds())
      .then((r) => setKinds(r.kinds))
      .catch((e: unknown) => setError(message(e)));
  }, [readOnly]);

  const kindLabel = (k: KindRow) => t(`classificationKinds.kind.${k.id}`);
  const maskLabel = (k: KindRow) =>
    k.maskPreset ? t(`maskList.preset.${k.maskPreset}`) : t("classificationKinds.noMask");
  const enabledLabel = (k: KindRow) =>
    t(k.enabled ? "contextViews.enabled" : "contextViews.disabled");

  const detectedTexts = (k: KindRow) => {
    const { places, hidden } = detectedOn(k);
    return contextPlaceTexts(places, hidden, t);
  };
  const open = kinds?.find((k) => k.id === detail.id) ?? null;
  const shown = (kinds ?? []).filter((k) =>
    // Every text a kind's row shows, and its id.
    matchesQuery(
      [
        k.id,
        kindLabel(k),
        k.sensitive ? t("classificationKinds.sensitive") : "",
        t(`classificationKinds.test.${k.id}`),
        `${Math.round(k.threshold * 100)}%`,
        maskLabel(k),
        enabledLabel(k),
        ...(readOnly ? detectedTexts(k) : []),
      ],
      search,
    ),
  );

  const closeForm = () => {
    setEditing(false);
    setProblem(null);
  };

  const save = async (kind: KindRow) => {
    if (kinds === null) return;
    const wanted = kinds
      .filter((k) => (k.id === kind.id ? draftEnabled : k.enabled))
      .map((k) => k.id);
    setSaving(true);
    setProblem(null);
    try {
      const r = await setEnabledClassificationKinds(wanted);
      setKinds(r.kinds);
      setEditing(false);
    } catch (e: unknown) {
      setProblem(message(e));
    } finally {
      setSaving(false);
    }
  };

  const form = (k: KindRow) => (
    <FormLayout data-testid="classification-kind-form">
      <FormChecks>
        <FormField size="medium">
          <Switch
            label={t("classificationKinds.enabledHeader")}
            checked={draftEnabled}
            disabled={saving}
            onChange={(e) => setDraftEnabled(e.currentTarget.checked)}
            aria-label={t("classificationKinds.enabledAria", { kind: kindLabel(k) })}
            data-testid={`classification-kind-enabled-${k.id}`}
          />
        </FormField>
      </FormChecks>
      <FormFooter
        panel
        onSave={() => save(k)}
        onCancel={closeForm}
        saving={saving}
        problem={problem ?? undefined}
        saveProps={{ "data-testid": "classification-kind-save" }}
      />
    </FormLayout>
  );

  const readView = (k: KindRow) => (
    <Stack gap="sm" data-testid={`classification-kind-detail-${k.id}`}>
      {!readOnly && (
        <PanelActions>
          <fieldset className="panel-action-set">
            <Tip text={t("classificationKinds.enabledAria", { kind: kindLabel(k) })} named>
              <ActionIcon
                aria-label={t("classificationKinds.enabledAria", { kind: kindLabel(k) })}
                data-testid={`classification-kind-edit-${k.id}`}
                onClick={(e) => {
                  e.stopPropagation();
                  setDraftEnabled(k.enabled);
                  setProblem(null);
                  setEditing(true);
                }}
              >
                <Pencil size={14} />
              </ActionIcon>
            </Tip>
          </fieldset>
        </PanelActions>
      )}
      <DetailFields
        testId={`classification-kind-read-${k.id}`}
        fields={[
          { label: t("classificationKinds.kindHeader"), value: kindLabel(k) },
          {
            label: t("classificationKinds.sensitive"),
            value: k.sensitive ? t("detailFields.yes") : t("detailFields.no"),
          },
          {
            label: t("classificationKinds.testHeader"),
            value: t(`classificationKinds.test.${k.id}`),
          },
          {
            label: t("classificationKinds.thresholdHeader"),
            value: `${Math.round(k.threshold * 100)}%`,
          },
          { label: t("classificationKinds.maskHeader"), value: maskLabel(k) },
          { label: t("classificationKinds.enabledHeader"), value: enabledLabel(k) },
          ...(readOnly
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
    </Stack>
  );

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
          testId="classification-kinds-split"
          storageKey="provisa.classificationKinds.detailWidth"
          title={open === null ? "" : kindLabel(open)}
          onClose={() => {
            closeForm();
            detail.close();
          }}
          closeOnEscape={!editing}
          labels={{
            panel: t("classificationKinds.kindHeader"),
            resize: t("tablesPage.resizeDetail"),
            expand: t("tablesPage.expandDetail"),
            restore: t("tablesPage.restoreDetail"),
            close: t("tablesPage.closeDetail"),
            backToList: t("tablesPage.backToList"),
          }}
          list={
            <div data-testid="classification-kinds">
              <SortGroupTable
                testPrefix="classification-kinds"
                testId="classification-kinds-list"
                emptyTestId="classification-kinds-empty"
                minWidth={640}
                rows={shown}
                columns={[
                  { key: "kind", label: t("classificationKinds.kindHeader"), sortValue: kindLabel },
                  {
                    key: "test",
                    label: t("classificationKinds.testHeader"),
                    sortValue: (k) => t(`classificationKinds.test.${k.id}`),
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
                  ...(readOnly ? [t("contextViews.detectedHeader")] : []),
                ]}
                colSpan={readOnly ? 6 : 5}
                rowKey={(k) => k.id}
                empty={t("classificationKinds.empty")}
                render={(k) => [
                  <ListRow
                    key={k.id}
                    testId={`classification-kind-${k.id}`}
                    selected={k.id === detail.id}
                    onClick={() => {
                      closeForm();
                      if (detail.id === k.id) detail.close();
                      else detail.open(k.id);
                    }}
                  >
                    <Table.Td>
                      <Group gap={4} wrap="nowrap">
                        {kindLabel(k)}
                        {k.sensitive && (
                          <Badge size="xs" color="red" variant="light">
                            {t("classificationKinds.sensitive")}
                          </Badge>
                        )}
                      </Group>
                    </Table.Td>
                    <Table.Td>{t(`classificationKinds.test.${k.id}`)}</Table.Td>
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
                    {readOnly && (
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
            </div>
          }
        >
          {open === null ? null : editing ? form(open) : readView(open)}
        </SplitPane>
      )}
    </>
  );
}
