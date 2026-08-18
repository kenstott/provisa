// Copyright (c) 2026 Kenneth Stott
// Canary: 4b7d1a5f-1735-4307-b713-e1f2db113892
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-917: the hot-cache (Redis), warm-tier and materialize-store settings, split out of the
 * single "Setup" tab they used to share into one expandable Settings panel per cache type — the
 * store connection with the response cache it backs, the promotion thresholds with the hot tables
 * they promote, the MV TTL and store URL with the materialized store.
 *
 * PUT /admin/cache-storage applies whichever blocks the body carries, so each panel sends only its
 * own; every panel is deployment-wide, which is why they render nothing without platform_settings.
 */

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Accordion,
  Alert,
  Button,
  Checkbox,
  Code,
  Group,
  NumberInput,
  SimpleGrid,
  Stack,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { Check, TriangleAlert } from "lucide-react";
import { fetchCacheStorage, setCacheStorage, type CacheStorageState } from "../../api/admin";
import { SaveRow } from "./settingsCards";
import { useSettingsBlocks } from "./useSettingsBlocks";
import { usePanelState } from "../../hooks/usePanelState";

type StorageBlock = "cache" | "hot_tables" | "warm_tables" | "materialized_views" | "materialize";

/** Load the cache-storage config and save back only `blocks`. */
function useCacheStorage(blocks: StorageBlock[]) {
  const { t } = useTranslation();
  const [s, setS] = useState<CacheStorageState | null>(null);
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");
  const [error, setError] = useState("");
  // The block list is a literal at every call site, so the joined form is what `save` depends on.
  const blockKey = blocks.join(",");

  useEffect(() => {
    fetchCacheStorage()
      .then(setS)
      .catch((e) => setError(String(e)));
  }, []);

  const save = useCallback(async () => {
    if (!s) return;
    setSaving(true);
    setMsg("");
    setError("");
    const payload: Record<string, unknown> = {};
    for (const block of blockKey.split(",") as StorageBlock[]) payload[block] = s[block];
    try {
      const res = await setCacheStorage(payload);
      setMsg(
        res.restart_required
          ? t("cacheStorageTab.savedRestartRequired")
          : t("cacheStorageTab.saved"),
      );
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  }, [s, blockKey, t]);

  return { s, setS, save, saving, msg, error };
}

function SettingsPanel({ testId, children }: { testId: string; children: React.ReactNode }) {
  const { t } = useTranslation();
  const [panel, setPanel] = usePanelState(testId);
  return (
    <Accordion variant="separated" value={panel} onChange={setPanel} data-testid={testId}>
      <Accordion.Item value="settings">
        <Accordion.Control>
          <Title order={4}>{t("adminPage.settingsPanel")}</Title>
        </Accordion.Control>
        <Accordion.Panel>
          <Stack maw={720} gap="md">
            {children}
          </Stack>
        </Accordion.Panel>
      </Accordion.Item>
    </Accordion>
  );
}

function StorageSaveRow({
  save,
  saving,
  msg,
  error,
  note,
}: {
  save: () => void;
  saving: boolean;
  msg: string;
  error: string;
  note: string;
}) {
  const { t } = useTranslation();
  return (
    <>
      <Alert color="yellow" icon={<TriangleAlert size={16} />}>
        {note}
      </Alert>
      <Group gap="sm" align="center">
        <Button
          onClick={save}
          disabled={saving}
          loading={saving}
          title={t("cacheStorageTab.saveButtonLabel")}
          aria-label={t("cacheStorageTab.saveButtonLabel")}
          data-testid="cache-storage-save"
          leftSection={saving ? undefined : <Check size={14} />}
        >
          {t("cacheStorageTab.saveButtonLabel")}
        </Button>
        {msg && (
          <Text c="green" size="sm">
            {msg}
          </Text>
        )}
        {error && (
          <Text c="red" size="sm">
            {error}
          </Text>
        )}
      </Group>
    </>
  );
}

/**
 * Response Cache → Settings: the org's response TTL, and for a platform administrator the store
 * that TTL is written into. Two endpoints behind one Save, because a reader of this panel is
 * setting up one thing: where cached responses live and how long they last.
 */
export function ResponseCacheSettingsPanel({ platform }: { platform: boolean }) {
  const { t } = useTranslation();
  const { settings, setSettings, save: saveSettings, saving, msg } = useSettingsBlocks(["cache"]);
  const {
    s,
    setS,
    save: saveStorage,
    saving: savingStorage,
    msg: storageMsg,
    error,
  } = useCacheStorage(["cache"]);
  if (!settings) return null;

  const save = async () => {
    await saveSettings();
    if (platform && s) await saveStorage();
  };

  return (
    <SettingsPanel testId="cache-ttl-settings">
      <NumberInput
        label={t("adminPage.defaultTtl")}
        description={t("adminPage.defaultTtlHint")}
        value={settings.cache.default_ttl}
        onChange={(v) =>
          setSettings({ ...settings, cache: { default_ttl: typeof v === "number" ? v : 0 } })
        }
      />
      {platform && s && (
        <>
          <Title order={5}>{t("cacheStorageTab.hotCacheHeading")}</Title>
          <Text c="dimmed" size="sm">
            {t("cacheStorageTab.hotCacheIntro")}
          </Text>
          <Checkbox
            label={t("cacheStorageTab.enableHotCache")}
            checked={s.cache.enabled}
            onChange={(e) =>
              setS({ ...s, cache: { ...s.cache, enabled: e.currentTarget.checked } })
            }
          />
          <TextInput
            label={t("cacheStorageTab.redisUrlLabel")}
            placeholder={t("cacheStorageTab.redisUrlPlaceholder")}
            value={s.cache.redis_url}
            onChange={(e) =>
              setS({ ...s, cache: { ...s.cache, redis_url: e.currentTarget.value } })
            }
          />
          <Alert color="yellow" icon={<TriangleAlert size={16} />}>
            {s.restart_required_note}
          </Alert>
          {(storageMsg || error) && (
            <Text c={error ? "red" : "green"} size="sm">
              {error || storageMsg}
            </Text>
          )}
        </>
      )}
      <SaveRow save={save} saving={saving || savingStorage} msg={msg} />
    </SettingsPanel>
  );
}

/** Hot Tables → Settings: the promotion thresholds for the hot tier and the warm tier below it. */
export function HotTablesSettingsPanel() {
  const { t } = useTranslation();
  const { s, setS, save, saving, msg, error } = useCacheStorage(["hot_tables", "warm_tables"]);
  if (!s) return null;

  return (
    <SettingsPanel testId="hot-tables-settings">
      <SimpleGrid cols={{ base: 1, sm: 3 }}>
        <NumberInput
          label={t("cacheStorageTab.promoteThresholdLabel")}
          value={s.hot_tables.auto_threshold}
          onChange={(v) =>
            setS({ ...s, hot_tables: { ...s.hot_tables, auto_threshold: Number(v) } })
          }
        />
        <NumberInput
          label={t("cacheStorageTab.maxRowsLabel")}
          value={s.hot_tables.max_rows}
          onChange={(v) => setS({ ...s, hot_tables: { ...s.hot_tables, max_rows: Number(v) } })}
        />
        <NumberInput
          label={t("cacheStorageTab.maxBytesLabel")}
          value={s.hot_tables.max_bytes}
          onChange={(v) => setS({ ...s, hot_tables: { ...s.hot_tables, max_bytes: Number(v) } })}
        />
      </SimpleGrid>

      <Title order={5}>{t("cacheStorageTab.warmHeading")}</Title>
      <Text c="dimmed" size="sm">
        {t("cacheStorageTab.warmIntro")}
      </Text>
      <SimpleGrid cols={{ base: 1, sm: 3 }}>
        <NumberInput
          label={t("cacheStorageTab.warmQueryThresholdLabel")}
          value={s.warm_tables.query_threshold}
          onChange={(v) =>
            setS({ ...s, warm_tables: { ...s.warm_tables, query_threshold: Number(v) } })
          }
        />
        <NumberInput
          label={t("cacheStorageTab.warmMaxRowsLabel")}
          value={s.warm_tables.max_rows}
          onChange={(v) => setS({ ...s, warm_tables: { ...s.warm_tables, max_rows: Number(v) } })}
        />
        <NumberInput
          label={t("cacheStorageTab.warmRefreshLabel")}
          value={s.warm_tables.refresh_interval ?? ""}
          onChange={(v) =>
            setS({
              ...s,
              warm_tables: { ...s.warm_tables, refresh_interval: v === "" ? null : Number(v) },
            })
          }
        />
      </SimpleGrid>
      <Checkbox
        label={t("cacheStorageTab.fsCacheEnabledLabel")}
        checked={s.warm_tables.fs_cache_enabled}
        onChange={(e) =>
          setS({
            ...s,
            warm_tables: { ...s.warm_tables, fs_cache_enabled: e.currentTarget.checked },
          })
        }
      />
      <SimpleGrid cols={{ base: 1, sm: 2 }}>
        <TextInput
          label={t("cacheStorageTab.fsCacheDirsLabel")}
          value={s.warm_tables.fs_cache_directories}
          onChange={(e) =>
            setS({
              ...s,
              warm_tables: { ...s.warm_tables, fs_cache_directories: e.currentTarget.value },
            })
          }
        />
        <TextInput
          label={t("cacheStorageTab.fsCacheMaxSizesLabel")}
          value={s.warm_tables.fs_cache_max_sizes}
          onChange={(e) =>
            setS({
              ...s,
              warm_tables: { ...s.warm_tables, fs_cache_max_sizes: e.currentTarget.value },
            })
          }
        />
      </SimpleGrid>
      <StorageSaveRow
        save={save}
        saving={saving}
        msg={msg}
        error={error}
        note={s.restart_required_note}
      />
    </SettingsPanel>
  );
}

/** Materialized Store → Settings: the default MV refresh TTL and the store the results land in. */
export function MaterializedSettingsPanel() {
  const { t } = useTranslation();
  const { s, setS, save, saving, msg, error } = useCacheStorage([
    "materialized_views",
    "materialize",
  ]);
  if (!s) return null;

  return (
    <SettingsPanel testId="materialized-settings">
      <NumberInput
        label={t("cacheStorageTab.mvDefaultTtlLabel")}
        description={t("cacheStorageTab.mvDefaultTtlHint")}
        value={s.materialized_views.default_ttl ?? ""}
        onChange={(v) =>
          setS({ ...s, materialized_views: { default_ttl: v === "" ? null : Number(v) } })
        }
      />

      <Title order={5}>{t("cacheStorageTab.materializeHeading")}</Title>
      <Text c="dimmed" size="sm">
        {t("cacheStorageTab.materializeIntroPrefix")}
        {s.materialize.default_store_url ? (
          <>
            {" "}
            — <Code>{s.materialize.default_store_url}</Code>
          </>
        ) : (
          t("cacheStorageTab.materializeIntroNoDefault")
        )}
        {t("cacheStorageTab.materializeIntroSuffix")}
      </Text>
      <TextInput
        label={t("cacheStorageTab.storeUrlLabel")}
        placeholder={
          s.materialize.default_store_url
            ? t("cacheStorageTab.storeUrlPlaceholderDefault", {
                url: s.materialize.default_store_url,
              })
            : t("cacheStorageTab.storeUrlPlaceholderRequired")
        }
        value={s.materialize.store_url}
        onChange={(e) =>
          setS({ ...s, materialize: { ...s.materialize, store_url: e.currentTarget.value } })
        }
      />
      <StorageSaveRow
        save={save}
        saving={saving}
        msg={msg}
        error={error}
        note={s.restart_required_note}
      />
    </SettingsPanel>
  );
}
