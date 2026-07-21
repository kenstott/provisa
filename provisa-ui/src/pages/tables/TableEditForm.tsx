// Copyright (c) 2026 Kenneth Stott
// Canary: 4a8c1f59-6e3d-4b2a-9f7c-0d5e8b1a3c72
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Fragment, useEffect, useRef, useState } from "react";
import { Check, X, Loader2 } from "lucide-react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Alert,
  Badge,
  Checkbox,
  Collapse,
  Group,
  NumberInput,
  Select,
  Table,
  TagsInput,
  Text,
  Textarea,
  TextInput,
  Tooltip,
} from "@mantine/core";
import { MultiSelect } from "../../components/MultiSelect";
import { ColumnPresetsEditor } from "../../components/admin/ColumnPresetsEditor";
import { UniquesPanel } from "../../components/admin/UniquesPanel";
import type { RefreshPolicySummary, RegisteredTable, Source } from "../../types/admin";
import { useCalendars, useRefreshPolicyPreview } from "../../hooks/useAdminQueries";
import type { Role } from "../../types/auth";
import type { PlatformSettings } from "../../api/admin";
import { sourceProbeTypes } from "../../liveCapability";
import { IANA_TIME_ZONES, NAMING_CONVENTIONS } from "./constants";
import { DescriptionField } from "./DescriptionField";
import { LiveDeliveryFieldset } from "./LiveDeliveryFieldset";
import { TimeInput } from "@mantine/dates";
import { CalendarCreateModal } from "./CalendarCreateModal";
import { CollapsibleSection } from "./CollapsibleSection";

interface CacheTtlEdit {
  value: string;
  dirty: boolean;
  saving: boolean;
}

interface TableEditFormProps {
  editingTable: RegisteredTable;
  setEditingTable: React.Dispatch<React.SetStateAction<RegisteredTable | null>>;
  editingColumnTypes: Record<string, string>;
  cacheTtlEdits: Record<number, CacheTtlEdit>;
  setCacheTtlEdits: React.Dispatch<React.SetStateAction<Record<number, CacheTtlEdit>>>;
  sources: Source[];
  roles: Role[];
  settings: PlatformSettings | null;
  saving: boolean;
  generatingDesc: boolean;
  setGeneratingDesc: React.Dispatch<React.SetStateAction<boolean>>;
  generatingColDesc: string | null;
  setGeneratingColDesc: React.Dispatch<React.SetStateAction<string | null>>;
  generateTableDescription: (id: number) => Promise<string>;
  generateColumnDescription: (id: number, colName: string) => Promise<string>;
  cancelEditing: () => void;
  handleSaveEdit: () => void;
  updateEditCol: (i: number, key: string, value: string | string[] | boolean) => void;
}

function FieldLabel({ text, help }: { text: string; help: string }) {
  return (
    <Group gap={4} wrap="nowrap">
      <Text component="span" size="sm">
        {text}
      </Text>
      <Tooltip label={help} multiline w={320}>
        <Text
          component="span"
          size="xs"
          c="dimmed"
          style={{ cursor: "help", lineHeight: 1 }}
        >
          ⓘ
        </Text>
      </Tooltip>
    </Group>
  );
}

export function TableEditForm({
  editingTable,
  setEditingTable,
  editingColumnTypes,
  cacheTtlEdits,
  setCacheTtlEdits,
  sources,
  roles,
  settings,
  saving,
  generatingDesc,
  setGeneratingDesc,
  generatingColDesc,
  setGeneratingColDesc,
  generateTableDescription,
  generateColumnDescription,
  cancelEditing,
  handleSaveEdit,
  updateEditCol,
}: TableEditFormProps) {
  const { t } = useTranslation();
  const roleOptions = roles.map((r) => ({ id: r.id, label: r.id }));

  // REQ-1143: keep the top-of-form refresh-policy summary in sync with the draft knobs. The tree is
  // never re-derived client-side — a debounced preview query re-runs describe_refresh_policy server-
  // side with the in-flight values, seeded from the persisted summary so it renders before the first
  // fetch resolves.
  const previewPolicy = useRefreshPolicyPreview();
  const { calendars, refetch: refetchCalendars } = useCalendars(); // REQ-962: snapshot-schedule picker
  const [snapshotOpen, setSnapshotOpen] = useState(
    Boolean(editingTable.mvCalendar), // open the panel when a schedule is already configured
  );
  const [calendarModalOpen, setCalendarModalOpen] = useState(false);
  const [livePolicy, setLivePolicy] = useState<RefreshPolicySummary | null>(
    editingTable.refreshPolicySummary,
  );
  // Effective cache_ttl mirrors the Cache TTL input's resolution: staged edit wins, then the row.
  const stagedTtl = cacheTtlEdits[editingTable.id]?.value;
  const effCacheTtl =
    stagedTtl != null && stagedTtl !== ""
      ? Number(stagedTtl)
      : stagedTtl === ""
        ? null
        : editingTable.cacheTtl;
  const {
    id: tableId,
    sourceId,
    domainId,
    schemaName,
    tableName,
    preferMaterialized,
    loadProtected,
    offPeakWindow,
    offPeakTz,
    changeSignal,
    refreshPolicySummary,
  } = editingTable;
  const previewRef = useRef(previewPolicy);
  previewRef.current = previewPolicy;
  useEffect(() => {
    let cancelled = false;
    const handle = setTimeout(() => {
      previewRef
        .current({
          sourceId,
          domainId,
          schemaName,
          tableName,
          cacheTtl: effCacheTtl,
          preferMaterialized,
          loadProtected,
          offPeakWindow,
          offPeakTz,
          changeSignal,
        })
        .then((summary) => {
          // A null preview means the engine is not yet connected (startup) — keep the persisted
          // summary rather than blanking the banner.
          if (!cancelled && summary) setLivePolicy(summary);
        });
    }, 300);
    return () => {
      cancelled = true;
      clearTimeout(handle);
    };
  }, [
    tableId,
    sourceId,
    domainId,
    schemaName,
    tableName,
    effCacheTtl,
    preferMaterialized,
    loadProtected,
    offPeakWindow,
    offPeakTz,
    changeSignal,
    refreshPolicySummary,
  ]);
  const shownPolicy = livePolicy ?? refreshPolicySummary;

  // REQ-1141: the off-peak window/zone only gate the load-protected scheduled snapshot. When load
  // protection resolves off (table override, else source default) they have no effect, so they are
  // hidden — surfaced only once the table is actually load-protected.
  const editSource = sources.find((s) => s.id === sourceId);
  const effLoadProtected =
    loadProtected == null ? (editSource?.loadProtected ?? false) : loadProtected;

  // A __provisa__ virtual view has no external source, so the source-freshness controls (cache TTL,
  // prefer_materialized, load protection, off-peak) don't apply — the view/MV rebuild path reads only
  // materialize + mv_refresh_interval + change_signal. Materialization is driven by the "Materialized
  // View" checkbox, not prefer_materialized. Hide those fields for a view to avoid contradictory knobs.
  const isView = editingTable.viewSql != null;

  return (
    <>
      {shownPolicy && (
        <Alert
          // REQ-1143: effective refresh/serving policy, live-previewed from the draft knobs (server-
          // derived). `serving` drives the color; a non-null `warning` flags an inert
          // prefer_materialized or an accidental frozen table.
          color={
            shownPolicy.warning
              ? "yellow"
              : shownPolicy.serving === "live"
                ? "blue"
                : shownPolicy.serving === "scheduled"
                  ? "teal"
                  : shownPolicy.serving === "cache"
                    ? "gray"
                    : "orange"
          }
          title={t("tableEditForm.refreshPolicyTitle")}
          style={{ marginBottom: "0.75rem", gridColumn: "1 / -1" }}
          data-testid="refresh-policy-summary"
        >
          <Text size="sm">{shownPolicy.text}</Text>
          {shownPolicy.warning && (
            <Text size="sm" c="yellow.8" mt={4} fw={500}>
              ⚠ {shownPolicy.warning}
            </Text>
          )}
        </Alert>
      )}
      <div className="form-card" style={{ marginBottom: "0.75rem" }}>
        <TextInput
          label={
            <FieldLabel
              text={t("tableEditForm.sqlAliasLabel")}
              help={t("tableEditForm.sqlAliasHelp")}
            />
          }
          value={editingTable.alias || ""}
          onChange={(e) =>
            setEditingTable({
              ...editingTable,
              alias: e.target.value || null,
            })
          }
          placeholder={t("tableEditForm.sqlAliasPlaceholder")}
        />
        <Select
          label={
            <FieldLabel
              text={t("tableEditForm.namingConventionLabel")}
              help={t("tableEditForm.namingConventionHelp")}
            />
          }
          data={NAMING_CONVENTIONS.map((nc) => ({
            value: nc.value,
            label: nc.label,
          }))}
          value={editingTable.gqlNamingConvention ?? ""}
          onChange={(v) =>
            setEditingTable({
              ...editingTable,
              gqlNamingConvention: v || null,
            })
          }
          comboboxProps={{ withinPortal: true }}
          allowDeselect={false}
        />
        {!isView && (
          <NumberInput
            label={
              <FieldLabel
                text={t("tableEditForm.cacheTtlLabel")}
                help={t("tableEditForm.cacheTtlHelp")}
              />
            }
            min={0}
            value={
              cacheTtlEdits[editingTable.id]?.value ??
              (editingTable.cacheTtl != null ? editingTable.cacheTtl : "")
            }
            onChange={(v) =>
              setCacheTtlEdits((prev) => ({
                ...prev,
                [editingTable.id]: {
                  ...prev[editingTable.id],
                  value: v === "" ? "" : String(v),
                  dirty: true,
                },
              }))
            }
            placeholder={t("tableEditForm.cacheTtlPlaceholder")}
          />
        )}
        {!isView && (
          <Select
            label={
              <FieldLabel
                text={t("tableEditForm.preferMaterializedLabel")}
                help={t("tableEditForm.preferMaterializedHelp")}
              />
            }
            data={[
              { value: "inherit", label: t("tableEditForm.inheritSource") },
              { value: "on", label: t("tableEditForm.on") },
              { value: "off", label: t("tableEditForm.off") },
            ]}
            value={
              editingTable.preferMaterialized == null
                ? "inherit"
                : editingTable.preferMaterialized
                  ? "on"
                  : "off"
            }
            onChange={(v) =>
              setEditingTable({
                ...editingTable,
                preferMaterialized: v === "inherit" ? null : v === "on",
              })
            }
            comboboxProps={{ withinPortal: true }}
            allowDeselect={false}
          />
        )}
        {!isView && (
          <Select
            // REQ-1141: load protection — scheduled-refresh-only; the query path never pulls the source.
            label={
              <FieldLabel
                text={t("tableEditForm.loadProtectedLabel")}
                help={t("tableEditForm.loadProtectedHelp")}
              />
            }
            data={[
              { value: "inherit", label: t("tableEditForm.inheritSource") },
              { value: "on", label: t("tableEditForm.on") },
              { value: "off", label: t("tableEditForm.off") },
            ]}
            value={
              editingTable.loadProtected == null
                ? "inherit"
                : editingTable.loadProtected
                  ? "on"
                  : "off"
            }
            onChange={(v) =>
              setEditingTable({
                ...editingTable,
                loadProtected: v === "inherit" ? null : v === "on",
              })
            }
            comboboxProps={{ withinPortal: true }}
            allowDeselect={false}
          />
        )}
        {!isView && effLoadProtected && (
          <CollapsibleSection
            title={t("tableEditForm.sourceProtectionPanel")}
            testId="mv-protection-panel"
            defaultOpen
          >
            {/* REQ-1141: off-peak window "HH:MM-HH:MM"; the scheduler refreshes only while it is
                open. Two time widgets (opens/closes) compose the string; both blank = no window. */}
            <div data-testid="off-peak-window">
              <FieldLabel
                text={t("tableEditForm.offPeakWindowLabel")}
                help={t("tableEditForm.offPeakWindowHelp")}
              />
              <Group gap="xs" grow>
                <TimeInput
                  aria-label={t("tableEditForm.offPeakOpensAria")}
                  data-testid="off-peak-opens"
                  label={t("tableEditForm.offPeakOpens")}
                  value={(editingTable.offPeakWindow ?? "").split("-")[0] ?? ""}
                  onChange={(e) => {
                    const end = (editingTable.offPeakWindow ?? "").split("-")[1] ?? "";
                    const start = e.currentTarget.value;
                    setEditingTable({
                      ...editingTable,
                      offPeakWindow: start || end ? `${start}-${end}` : null,
                    });
                  }}
                />
                <TimeInput
                  aria-label={t("tableEditForm.offPeakClosesAria")}
                  data-testid="off-peak-closes"
                  label={t("tableEditForm.offPeakCloses")}
                  value={(editingTable.offPeakWindow ?? "").split("-")[1] ?? ""}
                  onChange={(e) => {
                    const start = (editingTable.offPeakWindow ?? "").split("-")[0] ?? "";
                    const end = e.currentTarget.value;
                    setEditingTable({
                      ...editingTable,
                      offPeakWindow: start || end ? `${start}-${end}` : null,
                    });
                  }}
                />
              </Group>
            </div>
            <Select
              // REQ-1141: IANA zone for the off-peak window. Picklist of the runtime's supported zones
              // (Intl.supportedValuesOf) — the same identifiers ZoneInfo accepts server-side — so the
              // window can never be saved against an unparseable zone.
              label={
                <FieldLabel
                  text={t("tableEditForm.offPeakTzLabel")}
                  help={t("tableEditForm.offPeakTzHelp")}
                />
              }
              data={IANA_TIME_ZONES}
              value={editingTable.offPeakTz ?? ""}
              onChange={(v) =>
                setEditingTable({
                  ...editingTable,
                  offPeakTz: v || null,
                })
              }
              searchable
              clearable
              placeholder="UTC"
              comboboxProps={{ withinPortal: true }}
            />
          </CollapsibleSection>
        )}
        <div style={{ gridColumn: "1 / -1" }}>
          <FieldLabel
            text={t("tableEditForm.descriptionLabel")}
            help={t("tableEditForm.descriptionHelp")}
          />
          <DescriptionField
            value={editingTable.description || ""}
            onChange={(v) =>
              setEditingTable({ ...editingTable, description: v || null })
            }
            placeholder={t("tableEditForm.descriptionPlaceholder")}
            rows={2}
            generating={generatingDesc}
            onGenerate={async () => {
              setGeneratingDesc(true);
              try {
                const desc = await generateTableDescription(editingTable.id);
                if (desc) setEditingTable({ ...editingTable, description: desc });
              } finally {
                setGeneratingDesc(false);
              }
            }}
          />
        </div>
        {editingTable.viewSql && (
          <>
            <Group
              gap="xs"
              wrap="nowrap"
              style={{ gridColumn: "1 / -1" }}
            >
              <Checkbox
                checked={editingTable.materialize}
                onChange={(e) =>
                  setEditingTable({
                    ...editingTable,
                    materialize: e.currentTarget.checked,
                  })
                }
                label={t("tableEditForm.materializedViewLabel")}
              />
              <Text size="sm" c="dimmed">
                {t("tableEditForm.materializedViewDesc")}
              </Text>
              <Tooltip label={t("tableEditForm.materializedViewHelp")} multiline w={320}>
                <Text
                  component="span"
                  size="xs"
                  c="dimmed"
                  style={{ cursor: "help", lineHeight: 1 }}
                >
                  ⓘ
                </Text>
              </Tooltip>
            </Group>
            {editingTable.materialize && (
              <CollapsibleSection
                title={t("tableEditForm.refreshPanel")}
                testId="mv-refresh-panel"
                defaultOpen
              >
                <NumberInput
                  label={
                    <FieldLabel
                      text={t("tableEditForm.refreshIntervalLabel")}
                      help={t("tableEditForm.refreshIntervalHelp")}
                    />
                  }
                  min={30}
                  value={editingTable.mvRefreshInterval}
                  onChange={(v) =>
                    setEditingTable({
                      ...editingTable,
                      mvRefreshInterval:
                        typeof v === "number" ? v : parseInt(String(v), 10) || 300,
                    })
                  }
                />
                <div title={t("tableEditForm.nrtDebounceTitle")}>
                  <Text size="sm" c="dimmed" mb={4}>
                    {t("tableEditForm.nrtDebounceLabel")}
                  </Text>
                  <Group gap="xs">
                    <NumberInput
                      min={0}
                      step={0.5}
                      placeholder={t("tableEditForm.nrtQuietPlaceholder")}
                      aria-label={t("tableEditForm.nrtQuietAria")}
                      data-testid="mv-debounce-quiet"
                      value={editingTable.mvDebounceQuiet}
                      onChange={(v) =>
                        setEditingTable({
                          ...editingTable,
                          mvDebounceQuiet:
                            typeof v === "number" ? v : parseFloat(String(v)) || 0,
                        })
                      }
                    />
                    <NumberInput
                      min={0}
                      step={0.5}
                      placeholder={t("tableEditForm.nrtMaxDelayPlaceholder")}
                      aria-label={t("tableEditForm.nrtMaxDelayAria")}
                      data-testid="mv-debounce-max-delay"
                      value={editingTable.mvDebounceMaxDelay}
                      onChange={(v) =>
                        setEditingTable({
                          ...editingTable,
                          mvDebounceMaxDelay:
                            typeof v === "number" ? v : parseFloat(String(v)) || 0,
                        })
                      }
                    />
                  </Group>
                </div>
              </CollapsibleSection>
            )}
            {editingTable.materialize && (
              <Select
                label={
                  <FieldLabel
                    text={t("tableEditForm.consistencyLabel")}
                    help={t("tableEditForm.consistencyHelp")}
                  />
                }
                aria-label={t("tableEditForm.mvConsistencyAria")}
                data-testid="mv-consistency"
                data={[
                  { value: "shared", label: t("tableEditForm.consistencyShared") },
                  {
                    value: "distributed",
                    label: t("tableEditForm.consistencyDistributed"),
                  },
                ]}
                value={editingTable.mvConsistency}
                onChange={(v) =>
                  setEditingTable({
                    ...editingTable,
                    mvConsistency: v ?? editingTable.mvConsistency,
                  })
                }
                comboboxProps={{ withinPortal: true }}
                allowDeselect={false}
              />
            )}
            {editingTable.materialize && (
              <CollapsibleSection
                title={t("tableEditForm.timeTravelPanel")}
                testId="mv-timetravel-panel"
                defaultOpen={Boolean(editingTable.mvBitemporalMode)}
                badge={editingTable.mvBitemporalMode || undefined}
              >
                <Select
                  label={
                    <FieldLabel
                      text={t("tableEditForm.bitemporalLabel")}
                      help={t("tableEditForm.bitemporalHelp")}
                    />
                  }
                  aria-label={t("tableEditForm.bitemporalAria")}
                  data-testid="mv-bitemporal-mode"
                  data={[
                    { value: "", label: t("tableEditForm.bitemporalNone") },
                    { value: "snapshot", label: t("tableEditForm.bitemporalSnapshot") },
                    { value: "delta", label: t("tableEditForm.bitemporalDelta") },
                  ]}
                  value={editingTable.mvBitemporalMode ?? ""}
                  onChange={(v) =>
                    setEditingTable({
                      ...editingTable,
                      mvBitemporalMode: v ? v : null,
                    })
                  }
                  comboboxProps={{ withinPortal: true }}
                  allowDeselect={false}
                />
              </CollapsibleSection>
            )}
            {editingTable.materialize && (
              <div style={{ gridColumn: "1 / -1" }}>
                <Group
                  gap="xs"
                  wrap="nowrap"
                  style={{ cursor: "pointer" }}
                  onClick={() => setSnapshotOpen((o) => !o)}
                  data-testid="mv-snapshot-panel-toggle"
                  role="button"
                  aria-expanded={snapshotOpen}
                >
                  <ActionIcon variant="subtle" size="sm" aria-hidden>
                    {snapshotOpen ? "−" : "+"}
                  </ActionIcon>
                  <Text fw={600} size="sm">
                    {t("tableEditForm.snapshotPanel")}
                  </Text>
                  {editingTable.mvCalendar && (
                    <Badge size="xs" variant="light" color="grape">
                      {editingTable.mvCalendar}
                      {editingTable.mvGrain ? ` · ${editingTable.mvGrain}` : ""}
                    </Badge>
                  )}
                </Group>
                <Collapse in={snapshotOpen}>
                  <div
                    style={{
                      display: "grid",
                      gap: "var(--mantine-spacing-sm)",
                      paddingTop: "var(--mantine-spacing-xs)",
                    }}
                  >
                    <Group gap="xs" align="flex-end" wrap="nowrap">
                      <Select
                        style={{ flex: 1 }}
                        label={
                          <FieldLabel
                            text={t("tableEditForm.calendarLabel")}
                            help={t("tableEditForm.calendarHelp")}
                          />
                        }
                        data-testid="mv-calendar"
                        placeholder={
                          calendars.length
                            ? t("tableEditForm.calendarPlaceholder")
                            : t("tableEditForm.calendarPlaceholderEmpty")
                        }
                        data={calendars.map((c) => ({
                          value: c.name,
                          label: `${c.name} (v${c.version}, ${c.baseSystem}, ${c.tz})`,
                        }))}
                        value={editingTable.mvCalendar}
                        onChange={(v) =>
                          setEditingTable({ ...editingTable, mvCalendar: v || null })
                        }
                        comboboxProps={{ withinPortal: true }}
                        clearable
                      />
                      <ActionIcon
                        variant="light"
                        size="lg"
                        aria-label={t("tableEditForm.calendarNewAria")}
                        data-testid="mv-calendar-new"
                        onClick={() => setCalendarModalOpen(true)}
                      >
                        +
                      </ActionIcon>
                    </Group>
                    {editingTable.mvCalendar && (
                      <Select
                        label={
                          <FieldLabel
                            text={t("tableEditForm.grainLabel")}
                            help={t("tableEditForm.grainHelp")}
                          />
                        }
                        data-testid="mv-grain"
                        placeholder={t("tableEditForm.grainPlaceholder")}
                        data={[
                          { value: "daily", label: t("tableEditForm.grainDaily") },
                          { value: "weekly", label: t("tableEditForm.grainWeekly") },
                          { value: "monthly", label: t("tableEditForm.grainMonthly") },
                          { value: "quarterly", label: t("tableEditForm.grainQuarterly") },
                          { value: "annual", label: t("tableEditForm.grainAnnual") },
                          { value: "3WE", label: t("tableEditForm.grain3we") },
                          { value: "1MO", label: t("tableEditForm.grain1mo") },
                          { value: "LFR", label: t("tableEditForm.grainLfr") },
                        ]}
                        value={editingTable.mvGrain}
                        onChange={(v) => setEditingTable({ ...editingTable, mvGrain: v || null })}
                        comboboxProps={{ withinPortal: true }}
                      />
                    )}
                    {editingTable.mvCalendar && (
                      <NumberInput
                        label={
                          <FieldLabel
                            text={t("tableEditForm.allowedLatenessLabel")}
                            help={t("tableEditForm.allowedLatenessHelp")}
                          />
                        }
                        data-testid="mv-allowed-lateness"
                        min={0}
                        step={60}
                        value={editingTable.mvAllowedLateness}
                        onChange={(v) =>
                          setEditingTable({
                            ...editingTable,
                            mvAllowedLateness:
                              typeof v === "number" ? v : parseFloat(String(v)) || 0,
                          })
                        }
                      />
                    )}
                    {editingTable.mvCalendar && (
                      <Checkbox
                        data-testid="mv-business-day-grain"
                        checked={editingTable.mvBusinessDayGrain}
                        onChange={(e) =>
                          setEditingTable({
                            ...editingTable,
                            mvBusinessDayGrain: e.currentTarget.checked,
                          })
                        }
                        label={t("tableEditForm.businessDayGrainLabel")}
                      />
                    )}
                    {editingTable.mvCalendar && (
                      <Checkbox
                        data-testid="mv-expected-all"
                        checked={editingTable.mvExpectedEvents === null}
                        onChange={(e) =>
                          setEditingTable({
                            ...editingTable,
                            // null = verify every lineage input (default); [] = a custom set (verify
                            // nothing until inputs are named) — REQ-961 preflight contract.
                            mvExpectedEvents: e.currentTarget.checked ? null : [],
                          })
                        }
                        label={
                          <FieldLabel
                            text={t("tableEditForm.expectedAllLabel")}
                            help={t("tableEditForm.expectedAllHelp")}
                          />
                        }
                      />
                    )}
                    {editingTable.mvCalendar && editingTable.mvExpectedEvents !== null && (
                      <TagsInput
                        label={t("tableEditForm.expectedEventsLabel")}
                        data-testid="mv-expected-events"
                        placeholder={t("tableEditForm.expectedEventsPlaceholder")}
                        value={editingTable.mvExpectedEvents ?? []}
                        onChange={(v) =>
                          setEditingTable({ ...editingTable, mvExpectedEvents: v })
                        }
                        comboboxProps={{ withinPortal: true }}
                        clearable
                      />
                    )}
                  </div>
                </Collapse>
                <CalendarCreateModal
                  opened={calendarModalOpen}
                  onClose={() => setCalendarModalOpen(false)}
                  onCreated={(nm) => {
                    refetchCalendars();
                    setEditingTable({ ...editingTable, mvCalendar: nm });
                  }}
                />
              </div>
            )}
            {editingTable.materialize && editingTable.mvBitemporalMode && (
              <TextInput
                label={
                  <FieldLabel
                    text={t("tableEditForm.bitemporalKeyLabel")}
                    help={t("tableEditForm.bitemporalKeyHelp")}
                  />
                }
                aria-label={t("tableEditForm.bitemporalKeyAria")}
                data-testid="mv-bitemporal-key"
                placeholder={t("tableEditForm.bitemporalKeyPlaceholder")}
                value={editingTable.mvBitemporalKey.join(", ")}
                onChange={(e) =>
                  setEditingTable({
                    ...editingTable,
                    mvBitemporalKey: e.currentTarget.value
                      .split(",")
                      .map((s) => s.trim())
                      .filter(Boolean),
                  })
                }
              />
            )}
            {/* REQ-965/969/970: MV persistence outcome + incremental. Not shown for bitemporal
                views, whose append-only strategy supersedes the persist axis. */}
            {editingTable.materialize && !editingTable.mvBitemporalMode && (
              <Select
                label={
                  <FieldLabel
                    text={t("tableEditForm.persistLabel")}
                    help={t("tableEditForm.persistHelp")}
                  />
                }
                aria-label={t("tableEditForm.persistAria")}
                data-testid="mv-persist"
                data={[
                  { value: "replace", label: t("tableEditForm.persistReplace") },
                  { value: "append", label: t("tableEditForm.persistAppend") },
                  { value: "upsert", label: t("tableEditForm.persistUpsert") },
                ]}
                value={editingTable.mvPersist || "replace"}
                onChange={(v) => setEditingTable({ ...editingTable, mvPersist: v || "replace" })}
                comboboxProps={{ withinPortal: true }}
                allowDeselect={false}
              />
            )}
            {editingTable.materialize && !editingTable.mvBitemporalMode && (
              <Checkbox
                mt="1.75rem"
                data-testid="mv-incremental"
                checked={editingTable.mvIncremental}
                label={
                  <FieldLabel
                    text={t("tableEditForm.incrementalLabel")}
                    help={t("tableEditForm.incrementalHelp")}
                  />
                }
                onChange={(e) =>
                  setEditingTable({ ...editingTable, mvIncremental: e.currentTarget.checked })
                }
              />
            )}
            {editingTable.materialize &&
              !editingTable.mvBitemporalMode &&
              (editingTable.mvPersist === "upsert" || editingTable.mvIncremental) && (
                <TextInput
                  label={
                    <FieldLabel
                      text={t("tableEditForm.mvPrimaryKeyLabel")}
                      help={t("tableEditForm.mvPrimaryKeyHelp")}
                    />
                  }
                  aria-label={t("tableEditForm.mvPrimaryKeyAria")}
                  data-testid="mv-primary-key"
                  placeholder="id"
                  value={editingTable.mvPrimaryKey.join(", ")}
                  onChange={(e) =>
                    setEditingTable({
                      ...editingTable,
                      mvPrimaryKey: e.currentTarget.value
                        .split(",")
                        .map((s) => s.trim())
                        .filter(Boolean),
                    })
                  }
                />
              )}
            {editingTable.materialize && (
              <Textarea
                style={{ gridColumn: "1 / -1" }}
                label={
                  <FieldLabel
                    text={t("tableEditForm.preprocessLabel")}
                    help={t("tableEditForm.preprocessHelp")}
                  />
                }
                aria-label={t("tableEditForm.preprocessAria")}
                data-testid="mv-preprocess"
                placeholder={t("tableEditForm.preprocessPlaceholder")}
                autosize
                minRows={4}
                maxRows={16}
                spellCheck={false}
                styles={{
                  input: { fontFamily: "var(--mantine-font-family-monospace)" },
                }}
                value={editingTable.mvPreprocess ?? ""}
                onChange={(e) =>
                  setEditingTable({
                    ...editingTable,
                    mvPreprocess: e.currentTarget.value || null,
                  })
                }
              />
            )}
          </>
        )}
        <Group gap="xs" wrap="nowrap" style={{ gridColumn: "1 / -1" }}>
          <Checkbox
            checked={editingTable.dataProduct}
            onChange={(e) =>
              setEditingTable({
                ...editingTable,
                dataProduct: e.currentTarget.checked,
              })
            }
            label={t("tableEditForm.dataProductLabel")}
          />
          <Text size="sm" c="dimmed">
            {t("tableEditForm.dataProductDesc")}
          </Text>
        </Group>
        <Group gap="xs" wrap="nowrap" style={{ gridColumn: "1 / -1" }}>
          <Checkbox
            checked={editingTable.enableAggregates}
            onChange={(e) =>
              setEditingTable({
                ...editingTable,
                enableAggregates: e.currentTarget.checked,
              })
            }
            label={t("tableEditForm.enableAggregatesLabel")}
          />
          <Text size="sm" c="dimmed">
            {t("tableEditForm.enableAggregatesDesc")}
          </Text>
        </Group>
        <Group gap="xs" wrap="nowrap" style={{ gridColumn: "1 / -1" }}>
          <Checkbox
            checked={editingTable.enableGroupBy}
            onChange={(e) =>
              setEditingTable({
                ...editingTable,
                enableGroupBy: e.currentTarget.checked,
              })
            }
            label={t("tableEditForm.enableGroupByLabel")}
          />
          <Text size="sm" c="dimmed">
            {t("tableEditForm.enableGroupByDesc")}
          </Text>
        </Group>
        {editingTable.apiEndpoint && (
          <TextInput
            style={{ gridColumn: "1 / -1" }}
            label={t("tableEditForm.apiEndpointLabel")}
            readOnly
            value={editingTable.apiEndpoint}
            styles={{ input: { color: "var(--text-muted)", cursor: "default" } }}
          />
        )}
        <Select
          label={
            <FieldLabel
              text={t("tableEditForm.changeSignalLabel")}
              help={t("tableEditForm.changeSignalHelp")}
            />
          }
          data={[
            { value: "", label: t("tableEditForm.csInherit") },
            { value: "ttl", label: t("tableEditForm.csTtl") },
            { value: "probe", label: t("tableEditForm.csProbe") },
            { value: "ttl_probe", label: t("tableEditForm.csTtlProbe") },
            { value: "native", label: t("tableEditForm.csNative") },
            { value: "debezium", label: t("tableEditForm.csDebezium") },
            { value: "kafka", label: t("tableEditForm.csKafka") },
          ]}
          value={editingTable.changeSignal ?? ""}
          onChange={(v) =>
            setEditingTable({
              ...editingTable,
              changeSignal: v || null,
            })
          }
          comboboxProps={{ withinPortal: true }}
          allowDeselect={false}
        />
        {(editingTable.changeSignal === "ttl" ||
          editingTable.changeSignal === "ttl_probe") &&
          (() => {
            // A __provisa__ view has no Cache TTL — its ttl cadence is the materialized view's
            // Refresh Interval. A non-materialized view can't honor ttl (nothing to refresh).
            if (isView) {
              return editingTable.materialize ? (
                <Text style={{ gridColumn: "1 / -1" }} size="xs" c="dimmed">
                  {t("tableEditForm.ttlViewRefreshes", {
                    sec: editingTable.mvRefreshInterval,
                  })}
                </Text>
              ) : (
                <Text
                  style={{ gridColumn: "1 / -1" }}
                  size="xs"
                  c="var(--warning, #d19a00)"
                >
                  {t("tableEditForm.ttlViewNeedsMv")}{" "}
                  <strong>{t("tableEditForm.materializedViewLabel")}</strong>{" "}
                  {t("tableEditForm.ttlViewNeedsMvPost")}
                </Text>
              );
            }
            const cs = sources.find((s) => s.id === editingTable.sourceId);
            // Mirror the Cache TTL input's value resolution: the staged edit
            // (cacheTtlEdits) wins, then the table value, then the source.
            const staged = cacheTtlEdits[editingTable.id]?.value;
            const tableTtl =
              staged != null && staged !== ""
                ? Number(staged)
                : staged === ""
                  ? null
                  : editingTable.cacheTtl;
            const effTtl = tableTtl ?? cs?.cacheTtl ?? null;
            const fromTable = tableTtl != null;
            return effTtl == null ? (
              <Text
                style={{ gridColumn: "1 / -1" }}
                size="xs"
                c="var(--warning, #d19a00)"
              >
                {t("tableEditForm.ttlNeedsIntervalPre")}{" "}
                <strong>{t("tableEditForm.cacheTtlLabel")}</strong>{" "}
                {t("tableEditForm.ttlNeedsIntervalPost")}
              </Text>
            ) : (
              <Text style={{ gridColumn: "1 / -1" }} size="xs" c="dimmed">
                {t("tableEditForm.refreshesEvery", {
                  sec: effTtl,
                  source: fromTable
                    ? t("tableEditForm.sourceTable")
                    : t("tableEditForm.sourceSource"),
                })}
              </Text>
            );
          })()}
        {(editingTable.changeSignal === "debezium" ||
          editingTable.changeSignal === "kafka") &&
          (() => {
            const cs = sources.find((s) => s.id === editingTable.sourceId);
            const hasCdc = !!cs?.cdc?.bootstrapServers;
            const hasPk = editingTable.columns.some((c) => c.isPrimaryKey);
            // Debezium derives {prefix}.{schema}.{table}; a plain Kafka feed
            // consumes the topic named for the table (kafka_provider: topic=table).
            const topic =
              editingTable.changeSignal === "debezium"
                ? `${cs?.cdc?.topicPrefix}.${editingTable.schemaName}.${editingTable.tableName}`
                : editingTable.tableName;
            return (
              <>
                {hasCdc ? (
                  <Text style={{ gridColumn: "1 / -1" }} size="xs" c="dimmed">
                    {t("tableEditForm.cdcTransportPre")}
                    {cs!.cdc!.bootstrapServers}
                    {t("tableEditForm.cdcTransportMid")}{" "}
                    <code>{topic}</code>. {t("tableEditForm.cdcTransportPost")}
                  </Text>
                ) : (
                  <Text
                    style={{ gridColumn: "1 / -1" }}
                    size="xs"
                    c="var(--warning, #d19a00)"
                  >
                    {editingTable.changeSignal} {t("tableEditForm.cdcMissingPre")}{" "}
                    <strong>{t("tableEditForm.cdcMissingBold")}</strong>.{" "}
                    {t("tableEditForm.cdcMissingPost")}
                  </Text>
                )}
                {!hasPk && (
                  <Text
                    style={{ gridColumn: "1 / -1" }}
                    size="xs"
                    c="var(--warning, #d19a00)"
                  >
                    {t("tableEditForm.noPkPre")}{" "}
                    <strong>{t("tableEditForm.noPkBold")}</strong>{" "}
                    {t("tableEditForm.noPkPost")}
                  </Text>
                )}
              </>
            );
          })()}
        {(editingTable.changeSignal === "probe" ||
          editingTable.changeSignal === "ttl_probe") &&
          (() => {
            const src = sources.find((s) => s.id === editingTable.sourceId);
            const caps = sourceProbeTypes(src?.type);
            if (caps.length === 0) return null;
            return (
              <Select
                style={{ gridColumn: "1 / -1" }}
                label={
                  <FieldLabel
                    text={t("tableEditForm.probeTypeLabel")}
                    help={t("tableEditForm.probeTypeHelp")}
                  />
                }
                data={[
                  { value: "", label: t("tableEditForm.probeTypeAuto") },
                  ...caps.map((pt) => ({
                    value: pt,
                    label:
                      pt +
                      (pt === "watermark"
                        ? t("tableEditForm.probeAppendSuffix")
                        : t("tableEditForm.probeReplaceSuffix")),
                  })),
                ]}
                value={editingTable.probeType ?? ""}
                onChange={(v) =>
                  setEditingTable({
                    ...editingTable,
                    probeType: v || null,
                  })
                }
                comboboxProps={{ withinPortal: true }}
                allowDeselect={false}
              />
            );
          })()}
        {(editingTable.changeSignal === "probe" ||
          editingTable.changeSignal === "ttl_probe") && (
          <TextInput
            style={{ gridColumn: "1 / -1" }}
            label={
              <FieldLabel
                text={t("tableEditForm.freshnessProbeLabel")}
                help={t("tableEditForm.freshnessProbeHelp")}
              />
            }
            value={editingTable.probeQuery ?? ""}
            onChange={(e) =>
              setEditingTable({
                ...editingTable,
                probeQuery: e.target.value || null,
              })
            }
            placeholder={t("tableEditForm.freshnessProbePlaceholder")}
          />
        )}
        <LiveDeliveryFieldset
          editingTable={editingTable}
          setEditingTable={setEditingTable}
          editingColumnTypes={editingColumnTypes}
          sources={sources}
          settings={settings}
        />
      </div>
      {(() => {
        const NOSQL = new Set(["mongodb", "cassandra"]);
        const src = sources.find((s) => s.id === editingTable.sourceId);
        // Views and materialized views are read-only — no INSERT/UPDATE path, so presets never apply.
        const isReadOnlyView = editingTable.viewSql != null;
        const isMutable = src && !NOSQL.has((src.type ?? "").toLowerCase()) && !isReadOnlyView;
        return isMutable ? (
          <ColumnPresetsEditor
            presets={editingTable.columnPresets}
            columns={editingTable.columns.map((c) => c.columnName)}
            columnTypes={editingColumnTypes}
            onChange={(presets) =>
              setEditingTable({ ...editingTable, columnPresets: presets })
            }
          />
        ) : null;
      })()}
      {/* REQ-1093: table-level UNIQUE constraints editor */}
      <UniquesPanel
        uniques={editingTable.uniqueConstraints ?? []}
        columns={editingTable.columns.map((c) => c.columnName)}
        onChange={(uniques) => setEditingTable({ ...editingTable, uniqueConstraints: uniques })}
      />
      <Table className="data-table" style={{ margin: "0 0 0.5rem" }}>
        <Table.Thead>
          <Table.Tr>
            <Table.Th>{t("tableEditForm.columnHeader")}</Table.Th>
            <Table.Th>{t("tableEditForm.pkHeader")}</Table.Th>
            <Table.Th>{t("tableEditForm.sqlAliasHeader")}</Table.Th>
            <Table.Th>{t("tableEditForm.descriptionHeader")}</Table.Th>
            <Table.Th>{t("tableEditForm.visibleToHeader")}</Table.Th>
            <Table.Th>{t("tableEditForm.writableByHeader")}</Table.Th>
            <Table.Th>{t("tableEditForm.maskingHeader")}</Table.Th>
            <Table.Th>{t("tableEditForm.scopeHeader")}</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {editingTable.columns.map((c, i) => (
            <Fragment key={c.id}>
              <Table.Tr>
                <Table.Td>
                  <code>{c.columnName}</code>
                  {c.nativeFilterType && (
                    <Badge
                      ml={6}
                      size="xs"
                      variant="light"
                      color={c.nativeFilterType === "path_param" ? "yellow" : "blue"}
                      style={{ fontFamily: "monospace" }}
                    >
                      {c.nativeFilterType === "path_param"
                        ? t("tableEditForm.pathBadge")
                        : t("tableEditForm.queryBadge")}
                    </Badge>
                  )}
                  {c.isForeignKey && (
                    <Badge
                      ml={6}
                      size="xs"
                      variant="light"
                      color="green"
                      style={{ fontFamily: "monospace" }}
                    >
                      {t("tableEditForm.fkBadge")}
                    </Badge>
                  )}
                  {c.isAlternateKey && (
                    <Badge
                      ml={6}
                      size="xs"
                      variant="light"
                      color="yellow"
                      style={{ fontFamily: "monospace" }}
                    >
                      {t("tableEditForm.akBadge")}
                    </Badge>
                  )}
                </Table.Td>
                <Table.Td style={{ textAlign: "center" }}>
                  <Checkbox
                    aria-label={t("tableEditForm.primaryKeyAria")}
                    title={t("tableEditForm.primaryKeyAria")}
                    checked={c.isPrimaryKey || false}
                    onChange={(e) =>
                      updateEditCol(i, "isPrimaryKey", e.currentTarget.checked)
                    }
                  />
                </Table.Td>
                <Table.Td>
                  <TextInput
                    aria-label={t("tableEditForm.sqlAliasHeader")}
                    value={c.alias || c.computedSqlAlias}
                    onChange={(e) =>
                      updateEditCol(i, "alias", e.target.value)
                    }
                  />
                </Table.Td>
                <Table.Td>
                  <DescriptionField
                    value={c.description || ""}
                    onChange={(v) => updateEditCol(i, "description", v)}
                    placeholder={t("tableEditForm.descriptionHeader")}
                    rows={1}
                    generating={generatingColDesc === c.columnName}
                    onGenerate={async () => {
                      setGeneratingColDesc(c.columnName);
                      try {
                        const desc = await generateColumnDescription(
                          editingTable.id,
                          c.columnName,
                        );
                        if (desc) updateEditCol(i, "description", desc);
                      } catch (err) {
                        console.error(
                          "generateColumnDescription failed:",
                          err,
                        );
                      } finally {
                        setGeneratingColDesc(null);
                      }
                    }}
                  />
                </Table.Td>
                <Table.Td>
                  <MultiSelect
                    options={roleOptions}
                    value={c.visibleTo}
                    onChange={(selected) =>
                      updateEditCol(i, "visibleTo", selected)
                    }
                    label={t("tableEditForm.visibleToHeader")}
                  />
                </Table.Td>
                <Table.Td>
                  <MultiSelect
                    options={roleOptions}
                    value={c.writableBy}
                    onChange={(selected) =>
                      updateEditCol(i, "writableBy", selected)
                    }
                    label={t("tableEditForm.writableByHeader")}
                  />
                </Table.Td>
                <Table.Td>
                  <Select
                    aria-label={t("tableEditForm.maskingHeader")}
                    data={[
                      { value: "", label: t("tableEditForm.maskNone") },
                      { value: "regex", label: t("tableEditForm.maskRegex") },
                      { value: "constant", label: t("tableEditForm.maskConstant") },
                      { value: "truncate", label: t("tableEditForm.maskTruncate") },
                    ]}
                    value={c.maskType || ""}
                    onChange={(v) => updateEditCol(i, "maskType", v ?? "")}
                    comboboxProps={{ withinPortal: true }}
                    allowDeselect={false}
                  />
                </Table.Td>
                <Table.Td>
                  <Select
                    aria-label={t("tableEditForm.scopeHeader")}
                    data={[
                      { value: "domain", label: t("tableEditForm.scopeDomain") },
                      { value: "public", label: t("tableEditForm.scopePublic") },
                      { value: "restricted", label: t("tableEditForm.scopeRestricted") },
                    ]}
                    value={c.scope || "domain"}
                    onChange={(v) => updateEditCol(i, "scope", v ?? "domain")}
                    comboboxProps={{ withinPortal: true }}
                    allowDeselect={false}
                  />
                </Table.Td>
              </Table.Tr>
              {c.maskType && (
                <Table.Tr>
                  <Table.Td
                    colSpan={2}
                    style={{
                      paddingLeft: "1.5rem",
                      color: "var(--text-muted)",
                      fontSize: "0.75rem",
                    }}
                  >
                    {t("tableEditForm.maskingTemplateLabel")}
                  </Table.Td>
                  {c.maskType === "regex" && (
                    <>
                      <Table.Td>
                        <TextInput
                          aria-label={t("tableEditForm.regexPatternPlaceholder")}
                          value={c.maskPattern || ""}
                          onChange={(e) =>
                            updateEditCol(i, "maskPattern", e.target.value)
                          }
                          placeholder={t("tableEditForm.regexPatternPlaceholder")}
                        />
                      </Table.Td>
                      <Table.Td>
                        <TextInput
                          aria-label={t("tableEditForm.regexReplacementPlaceholder")}
                          value={c.maskReplace || ""}
                          onChange={(e) =>
                            updateEditCol(i, "maskReplace", e.target.value)
                          }
                          placeholder={t("tableEditForm.regexReplacementPlaceholder")}
                        />
                      </Table.Td>
                    </>
                  )}
                  {c.maskType === "constant" && (
                    <Table.Td colSpan={2}>
                      <TextInput
                        aria-label={t("tableEditForm.constantValuePlaceholder")}
                        value={c.maskValue || ""}
                        onChange={(e) =>
                          updateEditCol(i, "maskValue", e.target.value)
                        }
                        placeholder={t("tableEditForm.constantValuePlaceholder")}
                      />
                    </Table.Td>
                  )}
                  {c.maskType === "truncate" && (
                    <Table.Td colSpan={2}>
                      <Select
                        aria-label={t("tableEditForm.truncatePrecisionPlaceholder")}
                        data={[
                          { value: "year", label: t("tableEditForm.precisionYear") },
                          { value: "month", label: t("tableEditForm.precisionMonth") },
                          { value: "day", label: t("tableEditForm.precisionDay") },
                          { value: "hour", label: t("tableEditForm.precisionHour") },
                        ]}
                        placeholder={t("tableEditForm.truncatePrecisionPlaceholder")}
                        value={c.maskPrecision || null}
                        onChange={(v) => updateEditCol(i, "maskPrecision", v ?? "")}
                        comboboxProps={{ withinPortal: true }}
                      />
                    </Table.Td>
                  )}
                  <Table.Td colSpan={2}>
                    <MultiSelect
                      options={roleOptions}
                      value={c.unmaskedTo}
                      onChange={(selected) =>
                        updateEditCol(i, "unmaskedTo", selected)
                      }
                      label={t("tableEditForm.unmaskedToAria")}
                    />
                  </Table.Td>
                </Table.Tr>
              )}
            </Fragment>
          ))}
        </Table.Tbody>
      </Table>
      <Group justify="flex-end" gap="sm" p="0.75rem 0.5rem">
        <Tooltip label={t("tableEditForm.cancel")}>
          <ActionIcon
            variant="subtle"
            aria-label={t("tableEditForm.cancel")}
            data-testid="table-edit-cancel"
            onClick={cancelEditing}
            disabled={saving}
          >
            <X size={14} />
          </ActionIcon>
        </Tooltip>
        <Tooltip label={t("tableEditForm.save")}>
          <ActionIcon
            variant="filled"
            aria-label={t("tableEditForm.save")}
            data-testid="table-edit-save"
            onClick={handleSaveEdit}
            disabled={saving}
          >
            {saving ? (
              <Loader2
                size={14}
                style={{ animation: "spin 1s linear infinite" }}
              />
            ) : (
              <Check size={14} />
            )}
          </ActionIcon>
        </Tooltip>
      </Group>
    </>
  );
}
