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
  Accordion,
  Alert,
  Badge,
  Box,
  Button,
  Checkbox,
  Group,
  Loader,
  Modal,
  PasswordInput,
  Select,
  SimpleGrid,
  Stack,
  Text,
  TextInput,
  Title,
  Card,
} from "@mantine/core";
import { fetchSettingsCatalog, updateSettingsCatalog } from "../../api/admin";
import type { CatalogSetting, SettingsCatalog } from "../../api/admin";
import { SaveRow } from "./settingsCards";
import { CARD_GROUPS } from "./settingGroups";

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
      params?: { field?: string; reason?: string; min?: number; max?: number; other?: string };
    }
  ).params;
  if (!params?.reason) return { field: params?.field, message };
  const key = SERVER_REASON_KEY[params.reason] ?? `adminPage.setting.reason.${params.reason}`;
  return {
    field: params.field,
    message: t(key, { min: params.min, max: params.max, other: params.other }),
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
  const [opened, setOpened] = useState(false);
  const value = (s.value ?? {}) as Record<string, unknown>;
  const hasError = keys.some((mk) => Boolean(errors[rowId(s, mk)]));
  return (
    <Accordion
      variant="contained"
      value={opened || hasError ? s.key : null}
      onChange={(v) => setOpened(v === s.key)}
      data-testid={`setting-${s.key}`}
    >
      <Accordion.Item value={s.key}>
        <Accordion.Control data-testid={`setting-${s.key}-toggle`}>
          <Text fw={500} fz="sm" component="span">
            {t(`settings.${s.key}.label`)}
          </Text>
          {s.restart_required && (
            <Badge size="xs" color="orange" ml={4} data-testid={`setting-${s.key}-restart`}>
              {t("adminPage.setting.restartRequired")}
            </Badge>
          )}
        </Accordion.Control>
        <Accordion.Panel>
          <Text fz="xs" c="dimmed" mb="xs">
            {t(`settings.${s.key}.help`)}
          </Text>
          <SimpleGrid
            cols={{ base: 1, sm: 2, lg: 3 }}
            spacing="sm"
            data-testid={`setting-${s.key}-grid`}
          >
            {keys.map((mk) => {
              const id = `setting-${rowId(s, mk)}`;
              const source = s.sources?.[mk];
              const error = errors[rowId(s, mk)];
              return (
                <TextInput
                  key={mk}
                  label={t(`settings.${s.key}.keys.${mk}`)}
                  placeholder={toDraft(value[mk], "str")}
                  description={
                    source && source !== "stored" ? (
                      <span data-testid={`${id}-source`}>
                        {t("adminPage.setting.sourceLabel", {
                          source: t(`adminPage.setting.source.${source}`),
                        })}
                      </span>
                    ) : undefined
                  }
                  value={drafts[rowId(s, mk)] ?? ""}
                  disabled={!s.editable}
                  onChange={(e) => onChange(rowId(s, mk), e.currentTarget.value)}
                  error={error ? <span data-testid={`${id}-error`}>{error}</span> : undefined}
                  data-testid={id}
                />
              );
            })}
          </SimpleGrid>
        </Accordion.Panel>
      </Accordion.Item>
    </Accordion>
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
  const description = (
    <>
      {t(`settings.${s.key}.help`)}
      {s.unit && (
        <Text span fz="xs" c="dimmed" ml={4}>
          ({s.unit})
        </Text>
      )}
      {s.restart_required && (
        <Badge size="xs" color="orange" ml={4} data-testid={`${id}-restart`}>
          {t("adminPage.setting.restartRequired")}
        </Badge>
      )}
      {s.source !== "stored" && (
        <Text span fz="xs" c="dimmed" ml={4} data-testid={`${id}-source`}>
          {t("adminPage.setting.sourceLabel", {
            source: t(`adminPage.setting.source.${s.source}`),
          })}
        </Text>
      )}
      {showRunning && (
        <Text span fz="xs" c="dimmed" ml={4} data-testid={`${id}-running`}>
          {t("adminPage.setting.runningValue", { value: toDraft(s.value, s.type) })}
        </Text>
      )}
      {readOnly && s.readonly_reason && (
        <Text span fz="xs" c="dimmed" ml={4} data-testid={`${id}-reason`}>
          {t(`settings.readonlyReasons.${s.readonly_reason}`)}
        </Text>
      )}
    </>
  );
  const label = t(`settings.${s.key}.label`);

  let control: React.ReactNode;
  if (s.type === "bool") {
    control = (
      <Checkbox
        label={label}
        description={description}
        checked={draft === "true"}
        disabled={readOnly}
        onChange={(e) => onChange(String(e.currentTarget.checked))}
        error={errorNode}
        data-testid={id}
      />
    );
  } else if (s.type === "enum") {
    control = (
      <Select
        label={label}
        description={description}
        data={s.choices ?? []}
        value={draft}
        disabled={readOnly}
        onChange={(v) => v !== null && onChange(v)}
        allowDeselect={false}
        error={errorNode}
        data-testid={id}
      />
    );
  } else if (s.type === "secret") {
    control = (
      <Stack gap={2}>
        <PasswordInput
          label={label}
          description={description}
          placeholder={t("adminPage.setting.secretPlaceholder")}
          value={draft}
          disabled={readOnly}
          onChange={(e) => onChange(e.currentTarget.value)}
          error={errorNode}
          data-testid={id}
        />
        <Text fz="xs" data-testid={`${id}-secret-state`}>
          {s.set ? t("adminPage.setting.secretSet") : t("adminPage.setting.secretNotSet")}
        </Text>
      </Stack>
    );
  } else {
    control = (
      <TextInput
        label={label}
        description={description}
        value={draft}
        disabled={readOnly}
        onChange={(e) => onChange(e.currentTarget.value)}
        error={errorNode}
        data-testid={id}
      />
    );
  }

  const canReset = s.editable && s.type !== "secret" && present(s.stored);
  return (
    <Stack gap={2}>
      {control}
      {canReset && (
        <Button
          variant="subtle"
          size="compact-xs"
          onClick={onClear}
          disabled={cleared}
          data-testid={`${id}-reset`}
        >
          {cleared ? t("adminPage.setting.resetPending") : t("adminPage.setting.reset")}
        </Button>
      )}
    </Stack>
  );
}

/**
 * A card of catalog settings. `onSave(values, confirm)` receives the changed settings, typed
 * (`null` for a reset, a secret only when a new value was typed) and the keys of guarded settings
 * the operator confirmed. It may reject with an Error whose `params` carry `field` and `reason`.
 */
export function CatalogCard({
  title,
  settings,
  onSave,
  groupBy,
}: {
  title: string;
  settings: CatalogSetting[];
  onSave: (values: Record<string, unknown>, confirm: string[]) => Promise<unknown>;
  /** When given, the settings are shown in one expandable panel per group, in first-seen order. */
  groupBy?: (s: CatalogSetting) => string;
}) {
  const { t } = useTranslation();
  const [openGroups, setOpenGroups] = useState<string[]>([]);
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
    <Card withBorder padding="md" data-testid="settings-card">
      <Title order={4} mb="sm">
        {title}
      </Title>
      {groupBy ? (
        <Accordion
          multiple
          variant="contained"
          value={groups
            .filter((g) => openGroups.includes(g.id) || g.settings.some(hasError))
            .map((g) => g.id)}
          onChange={setOpenGroups}
        >
          {groups.map((g) => (
            <Accordion.Item key={g.id} value={g.id}>
              <Accordion.Control data-testid={`settings-group-${g.id}-toggle`}>
                <Text fw={500} fz="sm" component="span">
                  {t(`adminPage.setting.group.${g.id}`)}
                </Text>
              </Accordion.Control>
              <Accordion.Panel>
                {/* A flow layout: fields sit side by side and wrap to the next line as needed. */}
                <Group
                  wrap="wrap"
                  align="flex-start"
                  gap="md"
                  style={{ flexWrap: "wrap" }}
                  data-testid={`settings-group-${g.id}-flow`}
                >
                  {g.settings.map((s) => (
                    <Box key={s.key} style={{ flex: "1 1 260px", minWidth: 260 }}>
                      {field(s)}
                    </Box>
                  ))}
                </Group>
              </Accordion.Panel>
            </Accordion.Item>
          ))}
        </Accordion>
      ) : (
        <Stack gap="sm">{settings.map(field)}</Stack>
      )}
      <SaveRow save={save} saving={saving} msg={msg} />
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
        <Group justify="flex-end">
          <Button variant="default" onClick={() => setPending(null)}>
            {t("adminPage.setting.cancel")}
          </Button>
          <Button onClick={() => pending && send(pending.values, pending.guarded)}>
            {t("adminPage.setting.confirm")}
          </Button>
        </Group>
      </Modal>
    </Card>
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
