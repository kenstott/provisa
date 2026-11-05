// Copyright (c) 2026 Kenneth Stott
// Canary: 3a7f5e12-8b46-4d09-b2c1-9d4e6f0a1c85
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1913: the settings catalog on the admin page. One generic field renders any
 * {@link CatalogSetting}; a card groups them and owns the drafts; the panel reads
 * `GET /admin/settings/catalog`, saves through `PUT`, and shows the pending-restart banner.
 *
 * Labels and help are i18n keys `settings.<key>.label` / `.help`, owned by this UI.
 */

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import type { TFunction } from "i18next";
import {
  ActionIcon,
  Alert,
  Badge,
  Button,
  Checkbox,
  Loader,
  Modal,
  PasswordInput,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
} from "@mantine/core";
import { Plus, X } from "lucide-react";
import { fetchSettingsCatalog, updateSettingsCatalog } from "../../api/admin";
import type { CatalogSetting, SettingsCatalog } from "../../api/admin";
import { SaveRow } from "./settingsCards";
import { CollapsibleItem, Collapsibles } from "../collapsible/Collapsible";
import { CARD_GROUPS, GROUPS_OPEN_AT_FIRST } from "./settingGroups";
import { FormChecks, FormField, FormFooter, FormLayout, LabelWithHelp } from "../form/FormLayout";
import { useHelpNaming } from "../form/useHelpNaming";

const isLong = (s: CatalogSetting) => s.type === "list" || s.type === "map";

const present = (v: unknown) => v !== null && v !== undefined;

function toDraft(v: unknown, type: CatalogSetting["type"]): string {
  if (!present(v)) return "";
  if (type === "list") return (v as unknown[]).join(", ");
  if (type === "map") return JSON.stringify(v);
  return String(v);
}

/** The draft a field starts from: a restart setting edits what the next start will use. */
function initialDraft(s: CatalogSetting): string {
  if (s.type === "secret") return "";
  const useStored = present(s.stored) && (s.restart_required || !present(s.value));
  return toDraft(useStored ? s.stored : s.value, s.type);
}

type Parsed = { ok: true; value: unknown } | { ok: false; error: string };

function parse(s: CatalogSetting, draft: string, t: TFunction): Parsed {
  if (s.type === "int" || s.type === "float") {
    const text = draft.trim();
    const n = Number(text);
    if (text === "" || !Number.isFinite(n))
      return { ok: false, error: t("adminPage.setting.mustBeNumber") };
    if (s.type === "int" && !Number.isInteger(n)) {
      return { ok: false, error: t("adminPage.setting.mustBeInteger") };
    }
    if (present(s.min) && n < (s.min as number)) {
      return { ok: false, error: t("adminPage.setting.belowMin", { min: s.min }) };
    }
    if (present(s.max) && n > (s.max as number)) {
      return { ok: false, error: t("adminPage.setting.aboveMax", { max: s.max }) };
    }
    return { ok: true, value: n };
  }
  if (s.type === "bool") return { ok: true, value: draft === "true" };
  if (s.type === "list") {
    return {
      ok: true,
      value: draft
        .split(",")
        .map((x) => x.trim())
        .filter((x) => x !== ""),
    };
  }
  if (s.type === "map") {
    try {
      const v = JSON.parse(draft);
      if (v === null || typeof v !== "object" || Array.isArray(v)) throw new Error("not an object");
      return { ok: true, value: v };
    } catch {
      return { ok: false, error: t("adminPage.setting.invalidJson") };
    }
  }
  return { ok: true, value: draft };
}

const SERVER_REASON_KEY: Record<string, string> = {
  below_min: "adminPage.setting.belowMin",
  above_max: "adminPage.setting.aboveMax",
  not_a_number: "adminPage.setting.mustBeNumber",
};

function reasonMessage(err: unknown, t: TFunction): { field?: string; message: string } {
  const message = err instanceof Error ? err.message : String(err);
  const params = (
    err as {
      params?: {
        field?: string;
        reason?: string;
        min?: number;
        max?: number;
        other?: string;
        entry?: string;
      };
    }
  ).params;
  if (!params?.reason) return { field: params?.field, message };
  const key = SERVER_REASON_KEY[params.reason] ?? `adminPage.setting.reason.${params.reason}`;
  return {
    field: params.field,
    message: t(key, {
      min: params.min,
      max: params.max,
      other: params.other,
      entry: params.entry,
    }),
  };
}

const rowKeys = (s: CatalogSetting): string[] | null =>
  s.type === "map" && s.map_keys && s.map_keys.length > 0 ? s.map_keys : null;
const rowId = (s: CatalogSetting, mk: string) => `${s.key}.${mk}`;
const mapStored = (s: CatalogSetting, mk: string) =>
  toDraft((s.stored as Record<string, unknown> | undefined)?.[mk], "str");

/**
 * One row per entry of a fixed-key map, in columns inside an expandable panel; an empty row means
 * the entry uses its default. The panel starts collapsed and opens by itself when a row has an error.
 */
function MapRows({
  setting: s,
  keys,
  drafts,
  errors,
  onChange,
}: {
  setting: CatalogSetting;
  keys: string[];
  drafts: Record<string, string>;
  errors: Record<string, string>;
  onChange: (rowKey: string, draft: string) => void;
}) {
  const { t } = useTranslation();
  const value = (s.value ?? {}) as Record<string, unknown>;
  const hasError = keys.some((mk) => Boolean(errors[rowId(s, mk)]));
  return (
    <Collapsibles id={`admin.settings.${s.key}`} testId={`setting-${s.key}`}>
      <CollapsibleItem
        value={s.key}
        defaultOpen={false}
        // A value it holds that the save would refuse keeps it open.
        forceOpen={hasError}
        controlTestId={`setting-${s.key}-toggle`}
        title={
          <>
            <span>{t(`settings.${s.key}.label`)}</span>
            {s.restart_required && (
              <Badge size="xs" color="orange" ml={4} data-testid={`setting-${s.key}-restart`}>
                {t("adminPage.setting.restartRequired")}
              </Badge>
            )}
          </>
        }
      >
        <Text fz="xs" c="dimmed" mb="xs">
          {t(`settings.${s.key}.help`)}
        </Text>
        <FormLayout data-testid={`setting-${s.key}-grid`}>
          {keys.map((mk) => {
            const id = `setting-${rowId(s, mk)}`;
            const source = s.sources?.[mk];
            const error = errors[rowId(s, mk)];
            return (
              <FormField size="short" key={mk}>
                <TextInput
                  label={t(`settings.${s.key}.keys.${mk}`)}
                  placeholder={toDraft(value[mk], "str")}
                  value={drafts[rowId(s, mk)] ?? ""}
                  disabled={!s.editable}
                  onChange={(e) => onChange(rowId(s, mk), e.currentTarget.value)}
                  error={error ? <span data-testid={`${id}-error`}>{error}</span> : undefined}
                  data-testid={id}
                />
                {source && source !== "stored" && (
                  <div className="form-status" data-testid={`${id}-source`}>
                    {t("adminPage.setting.sourceLabel", {
                      source: t(`adminPage.setting.source.${source}`),
                    })}
                  </div>
                )}
              </FormField>
            );
          })}
        </FormLayout>
      </CollapsibleItem>
    </Collapsibles>
  );
}

/** A list whose entries are rows of columns (REQ-2012), as its draft: rows joined by ", ", each
 * row's cells by a space -- the same text the plain list field holds, so it saves the same way. */
const tableRows = (draft: string, width: number): string[][] =>
  draft
    .split(",")
    .map((r) => r.trim())
    .filter((r) => r !== "")
    .map((r) => {
      const cells = r.split(/\s+/);
      return [...cells, ...Array<string>(Math.max(width - cells.length, 0)).fill("")];
    });
const tableDraft = (rows: string[][]): string =>
  rows
    .map((r) =>
      r
        .map((c) => c.trim())
        .join(" ")
        .trim(),
    )
    .filter((r) => r !== "")
    .join(", ");

/** A list setting shown as a table: one row per entry, one column per part, rows added and
 * removed in place. */
function TableRows({
  setting: s,
  columns,
  draft,
  readOnly,
  onChange,
}: {
  setting: CatalogSetting;
  columns: string[];
  draft: string;
  readOnly: boolean;
  onChange: (draft: string) => void;
}) {
  const { t } = useTranslation();
  const [rows, setRows] = useState<string[][]>(() => tableRows(draft, columns.length));
  const update = (next: string[][]) => {
    setRows(next);
    onChange(tableDraft(next));
  };
  const id = `setting-${s.key}`;
  return (
    <Stack gap={4}>
      <Table withTableBorder data-testid={`${id}-table`}>
        <Table.Thead>
          <Table.Tr>
            {columns.map((c, j) => (
              // The first column names the row (a model id) and takes the room the others leave.
              <Table.Th key={c} w={j === 0 ? "36%" : undefined}>
                {t(`settings.${s.key}.columns.${c}`)}
              </Table.Th>
            ))}
            <Table.Th />
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {rows.map((row, i) => (
            <Table.Tr key={i} data-testid={`${id}-row-${i}`}>
              {columns.map((c, j) => (
                <Table.Td key={c}>
                  <TextInput
                    size="xs"
                    aria-label={`${t(`settings.${s.key}.columns.${c}`)} ${i + 1}`}
                    value={row[j] ?? ""}
                    disabled={readOnly}
                    onChange={(e) => {
                      const value = e.currentTarget.value;
                      update(
                        rows.map((r, k) => (k === i ? r.map((x, m) => (m === j ? value : x)) : r)),
                      );
                    }}
                    data-testid={`${id}-${i}-${c}`}
                  />
                </Table.Td>
              ))}
              <Table.Td>
                <ActionIcon
                  variant="subtle"
                  color="red"
                  aria-label={t("adminPage.setting.removeRow")}
                  disabled={readOnly}
                  onClick={() => update(rows.filter((_r, k) => k !== i))}
                  data-testid={`${id}-${i}-remove`}
                >
                  <X size={14} />
                </ActionIcon>
              </Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
      <div>
        <Button
          variant="subtle"
          size="compact-xs"
          leftSection={<Plus size={14} />}
          disabled={readOnly}
          onClick={() => setRows([...rows, Array<string>(columns.length).fill("")])}
          data-testid={`${id}-add`}
        >
          {t("adminPage.setting.addRow")}
        </Button>
      </div>
    </Stack>
  );
}

export function SettingField({
  setting: s,
  draft,
  error,
  cleared,
  onChange,
  onClear,
}: {
  setting: CatalogSetting;
  draft: string;
  error: string;
  cleared: boolean;
  onChange: (draft: string) => void;
  onClear: () => void;
}) {
  const { t } = useTranslation();
  const id = `setting-${s.key}`;
  const errorNode = error ? <span data-testid={`${id}-error`}>{error}</span> : undefined;
  const readOnly = !s.editable;
  const showRunning =
    s.restart_required &&
    present(s.stored) &&
    present(s.value) &&
    String(s.stored) !== String(s.value);
  const help = t(`settings.${s.key}.help`);
  const { textId, naming } = useHelpNaming(help);
  const sourceText =
    s.source !== "stored"
      ? t("adminPage.setting.sourceLabel", { source: t(`adminPage.setting.source.${s.source}`) })
      : "";
  const runningText = showRunning
    ? t("adminPage.setting.runningValue", { value: toDraft(s.value, s.type) })
    : "";
  const reasonText =
    readOnly && s.readonly_reason
      ? t(`settings.readonlyReasons.${s.readonly_reason}`, { value: s.chart_value })
      : "";
  // The name, with its unit, then the (?) that holds the explanation: nothing sits between a
  // field's label and its control.
  const name = t(`settings.${s.key}.label`);
  const label = (
    <LabelWithHelp label={s.unit ? `${name} (${s.unit})` : name} help={help} textId={textId} />
  );
  const canReset = s.editable && s.type !== "secret" && present(s.stored);
  const reset = canReset && (
    <Button
      variant="subtle"
      size="compact-xs"
      onClick={onClear}
      disabled={cleared}
      data-testid={`${id}-reset`}
    >
      {cleared ? t("adminPage.setting.resetPending") : t("adminPage.setting.reset")}
    </Button>
  );
  // Where the value comes from and what a change needs, under the control.
  const status = (
    <div className="form-status">
      {s.restart_required && (
        <Badge size="xs" color="orange" data-testid={`${id}-restart`}>
          {t("adminPage.setting.restartRequired")}
        </Badge>
      )}
      {sourceText && <span data-testid={`${id}-source`}>{sourceText}</span>}
      {runningText && <span data-testid={`${id}-running`}>{runningText}</span>}
      {reasonText && <span data-testid={`${id}-reason`}>{reasonText}</span>}
      {s.type === "secret" && (
        <span data-testid={`${id}-secret-state`}>
          {s.set ? t("adminPage.setting.secretSet") : t("adminPage.setting.secretNotSet")}
        </span>
      )}
      {reset}
    </div>
  );

  if (s.type === "bool") {
    // A tick is its label and its (?): where its value comes from is said in the help.
    const told = [help, sourceText, runningText, reasonText].filter(Boolean).join(" · ");
    return (
      <FormField size="short" className="form-field--action">
        <Checkbox
          label={<LabelWithHelp label={name} help={told} textId={textId} />}
          aria-labelledby={textId}
          aria-description={told}
          checked={draft === "true"}
          disabled={readOnly}
          onChange={(e) => onChange(String(e.currentTarget.checked))}
          error={errorNode}
          data-testid={id}
        />
        {/* A change that needs a restart says so beside the tick, in sight. */}
        {s.restart_required && (
          <Badge size="xs" color="orange" data-testid={`${id}-restart`}>
            {t("adminPage.setting.restartRequired")}
          </Badge>
        )}
        {reset}
      </FormField>
    );
  }

  if (s.type === "enum") {
    return (
      <FormField size="medium" data-testid={`${id}-field`}>
        <Select
          label={label}
          {...naming}
          data={s.choices ?? []}
          value={draft}
          disabled={readOnly}
          onChange={(v) => v !== null && onChange(v)}
          allowDeselect={false}
          error={errorNode}
          data-testid={id}
        />
        {status}
      </FormField>
    );
  }
  if (s.type === "secret") {
    return (
      <FormField size="medium" data-testid={`${id}-field`}>
        <PasswordInput
          label={label}
          {...naming}
          placeholder={t("adminPage.setting.secretPlaceholder")}
          value={draft}
          disabled={readOnly}
          onChange={(e) => onChange(e.currentTarget.value)}
          error={errorNode}
          data-testid={id}
        />
        {status}
      </FormField>
    );
  }
  if (s.credential) {
    // A credential field keeps what was typed (a value, or a reference to a secret) and shows
    // it again, behind the reveal control.
    return (
      <FormField size="medium" data-testid={`${id}-field`}>
        <PasswordInput
          label={label}
          {...naming}
          value={draft}
          disabled={readOnly}
          onChange={(e) => onChange(e.currentTarget.value)}
          error={errorNode}
          data-testid={id}
        />
        {status}
      </FormField>
    );
  }
  if (s.type === "list" && s.columns && s.columns.length > 0) {
    return (
      <FormField size="full" data-testid={`${id}-field`}>
        {label}
        <TableRows
          setting={s}
          columns={s.columns}
          draft={draft}
          readOnly={readOnly}
          onChange={onChange}
        />
        {errorNode && (
          <Text c="red" size="xs">
            {errorNode}
          </Text>
        )}
        {status}
      </FormField>
    );
  }
  // A number is a short field, a list or a document a long one, the rest medium.
  const size = s.type === "int" || s.type === "float" ? "short" : isLong(s) ? "long" : "medium";
  return (
    <FormField size={size} data-testid={`${id}-field`}>
      <TextInput
        label={label}
        {...naming}
        value={draft}
        disabled={readOnly}
        onChange={(e) => onChange(e.currentTarget.value)}
        error={errorNode}
        data-testid={id}
      />
      {status}
    </FormField>
  );
}

/**
 * A card of catalog settings. `onSave(values, confirm)` receives the changed settings, typed
 * (`null` for a reset, a secret only when a new value was typed) and the keys of guarded settings
 * the operator confirmed. It may reject with an Error whose `params` carry `field` and `reason`.
 */
export function CatalogCard({
  cardId,
  title,
  settings,
  onSave,
  groupBy,
}: {
  /** The card's catalog id: names its toggle. */
  cardId: string;
  title: string;
  settings: CatalogSetting[];
  onSave: (values: Record<string, unknown>, confirm: string[]) => Promise<unknown>;
  /** When given, the settings are shown in one expandable panel per group, in first-seen order. */
  groupBy?: (s: CatalogSetting) => string;
}) {
  const { t } = useTranslation();
  const [drafts, setDrafts] = useState<Record<string, string>>(() =>
    Object.fromEntries(
      settings.flatMap((s) => {
        const keys = rowKeys(s);
        return keys
          ? keys.map((mk) => [rowId(s, mk), mapStored(s, mk)])
          : [[s.key, initialDraft(s)]];
      }),
    ),
  );
  const [cleared, setCleared] = useState<Record<string, boolean>>({});
  // A setting whose current value is unusable lists its own error, shown until it is corrected.
  const [errors, setErrors] = useState<Record<string, string>>(() =>
    Object.fromEntries(
      settings
        .filter((s) => s.error)
        .map((s) => [s.key, reasonMessage({ params: s.error }, t).message]),
    ),
  );
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");
  const [pending, setPending] = useState<{
    values: Record<string, unknown>;
    guarded: string[];
  } | null>(null);

  const send = async (values: Record<string, unknown>, confirm: string[]) => {
    setPending(null);
    setSaving(true);
    try {
      await onSave(values, confirm);
    } catch (e: unknown) {
      const { field, message } = reasonMessage(e, t);
      const known = (f: string) =>
        settings.some((s) => s.key === f || (rowKeys(s) ?? []).some((mk) => rowId(s, mk) === f));
      if (field && known(field)) setErrors({ [field]: message });
      else setMsg(message);
    } finally {
      setSaving(false);
    }
  };

  const save = async () => {
    const next: Record<string, string> = {};
    const values: Record<string, unknown> = {};
    for (const s of settings) {
      if (!s.editable) continue;
      const keys = rowKeys(s);
      if (keys) {
        // The map store is partial: a key given is set, a key given as null is cleared, a key not
        // given keeps its stored value. So only changed rows are sent.
        const entries: Record<string, number | null> = {};
        for (const mk of keys) {
          const draft = drafts[rowId(s, mk)] ?? "";
          if (draft === mapStored(s, mk)) continue;
          if (draft.trim() === "") {
            entries[mk] = null;
            continue;
          }
          const parsed = parse({ ...s, type: "float" }, draft, t);
          if (parsed.ok) entries[mk] = parsed.value as number;
          else next[rowId(s, mk)] = parsed.error;
        }
        if (Object.keys(entries).length) values[s.key] = entries;
        continue;
      }
      if (cleared[s.key]) {
        values[s.key] = null;
        continue;
      }
      const draft = drafts[s.key] ?? "";
      if (s.type === "secret" ? draft === "" : draft === initialDraft(s)) continue;
      const parsed = parse(s, draft, t);
      if (parsed.ok) values[s.key] = parsed.value;
      else next[s.key] = parsed.error;
    }
    setErrors(next);
    setMsg("");
    if (Object.keys(next).length) return;
    const guarded = settings
      .filter((s) => s.guard === "confirm" && s.key in values)
      .map((s) => s.key);
    if (guarded.length) setPending({ values, guarded });
    else await send(values, []);
  };

  const field = (s: CatalogSetting) => {
    const keys = rowKeys(s);
    if (keys) {
      return (
        <MapRows
          key={s.key}
          setting={s}
          keys={keys}
          drafts={drafts}
          errors={errors}
          onChange={(rk, d) => setDrafts({ ...drafts, [rk]: d })}
        />
      );
    }
    return (
      <SettingField
        key={s.key}
        setting={s}
        draft={drafts[s.key] ?? ""}
        error={errors[s.key] ?? ""}
        cleared={cleared[s.key] === true}
        onChange={(d) => {
          setDrafts({ ...drafts, [s.key]: d });
          setCleared({ ...cleared, [s.key]: false });
        }}
        onClear={() => setCleared({ ...cleared, [s.key]: true })}
      />
    );
  };
  // A setting has an error when its own field has one or, for a map, when one of its rows has.
  const hasError = (s: CatalogSetting) =>
    Boolean(errors[s.key]) || (rowKeys(s) ?? []).some((mk) => Boolean(errors[rowId(s, mk)]));
  // The shared form layout: fields in rows by their size, an expandable map on a line of its own,
  // then the ticks together.
  const isTick = (s: CatalogSetting) => s.type === "bool";
  const fieldGrid = (list: CatalogSetting[], testId: string) => (
    <FormLayout data-testid={testId}>
      {list
        .filter((s) => !isTick(s))
        .map((s) =>
          rowKeys(s) ? (
            <FormField size="full" key={s.key}>
              {field(s)}
            </FormField>
          ) : (
            field(s)
          ),
        )}
      {list.some(isTick) && <FormChecks>{list.filter(isTick).map(field)}</FormChecks>}
    </FormLayout>
  );
  const groups: { id: string; settings: CatalogSetting[] }[] = [];
  if (groupBy) {
    for (const s of settings) {
      const id = groupBy(s);
      const group = groups.find((g) => g.id === id);
      if (group) group.settings.push(s);
      else groups.push({ id, settings: [s] });
    }
  }

  return (
    <>
      {/* The card folds as its groups do: a contained accordion item, open to begin with. */}
      <Collapsibles id="admin.deployment-settings" testId="settings-card">
        <CollapsibleItem
          value={cardId}
          title={title}
          controlTestId={`settings-card-${cardId}-toggle`}
        >
          {groupBy ? (
            <Collapsibles id={`admin.deployment-settings.${cardId}`}>
              {groups.map((g) => (
                <CollapsibleItem
                  key={g.id}
                  value={g.id}
                  defaultOpen={GROUPS_OPEN_AT_FIRST.has(g.id)}
                  // A group holding a value the save would refuse is kept open.
                  forceOpen={g.settings.some(hasError)}
                  controlTestId={`settings-group-${g.id}-toggle`}
                  title={t(`adminPage.setting.group.${g.id}`)}
                >
                  {fieldGrid(g.settings, `settings-group-${g.id}-flow`)}
                </CollapsibleItem>
              ))}
            </Collapsibles>
          ) : (
            fieldGrid(settings, "settings-card-grid")
          )}
          <SaveRow save={save} saving={saving} msg={msg} />
        </CollapsibleItem>
      </Collapsibles>
      <Modal
        opened={pending !== null}
        onClose={() => setPending(null)}
        title={t("adminPage.setting.confirmTitle")}
        transitionProps={{ duration: 0 }}
      >
        <Text mb="md">
          {t("adminPage.setting.confirmBody", {
            settings: (pending?.guarded ?? []).map((k) => t(`settings.${k}.label`)).join(", "),
          })}
        </Text>
        <FormFooter
          onCancel={() => setPending(null)}
          action={
            <Button onClick={() => pending && send(pending.values, pending.guarded)}>
              {t("adminPage.setting.confirm")}
            </Button>
          }
        />
      </Modal>
    </>
  );
}

/** Lists the saved settings that wait for a restart; renders nothing when none do. */
export function PendingRestartBanner({ keys }: { keys: string[] }) {
  const { t } = useTranslation();
  if (!keys.length) return null;
  return (
    <Alert
      color="orange"
      title={t("adminPage.setting.pendingRestartTitle")}
      data-testid="pending-restart-banner"
    >
      {t("adminPage.setting.pendingRestartBody", {
        settings: keys.map((k) => t(`settings.${k}.label`)).join(", "),
      })}
    </Alert>
  );
}

/** The whole catalog: platform administrators only (the routes answer 403 to anyone else). */
export function SettingsCatalogPanel() {
  const { t } = useTranslation();
  const [catalog, setCatalog] = useState<SettingsCatalog | null>(null);
  const [state, setState] = useState<"loading" | "ready" | "forbidden" | "error">("loading");
  const [error, setError] = useState("");
  const [version, setVersion] = useState(0);

  const load = useCallback(
    () =>
      fetchSettingsCatalog()
        .then((c) => {
          setCatalog(c);
          setState("ready");
          setVersion((v) => v + 1);
        })
        .catch((e: unknown) => {
          if ((e as { status?: number }).status === 403) setState("forbidden");
          else {
            setError(e instanceof Error ? e.message : String(e));
            setState("error");
          }
        }),
    [],
  );

  useEffect(() => {
    load();
  }, [load]);

  if (state === "loading") return <Loader size="sm" />;
  if (state === "forbidden") {
    return (
      <Alert color="gray" data-testid="catalog-forbidden">
        {t("adminPage.setting.forbidden")}
      </Alert>
    );
  }
  if (state === "error" || !catalog) {
    return (
      <Alert color="red" data-testid="catalog-error">
        {t("adminPage.setting.loadFailed", { error })}
      </Alert>
    );
  }
  const save = async (values: Record<string, unknown>, confirm: string[]) => {
    await updateSettingsCatalog(values, confirm);
    await load();
  };
  return (
    <Stack gap="md" data-testid="settings-catalog">
      <PendingRestartBanner keys={catalog.pending_restart} />
      {catalog.cards.map((c) => (
        <div key={`${c.id}-${version}`} data-testid={`catalog-card-${c.id}`}>
          <CatalogCard
            cardId={c.id}
            title={t(`adminPage.setting.card.${c.id}`)}
            settings={c.settings}
            onSave={save}
            groupBy={CARD_GROUPS[c.id]}
          />
        </div>
      ))}
    </Stack>
  );
}
