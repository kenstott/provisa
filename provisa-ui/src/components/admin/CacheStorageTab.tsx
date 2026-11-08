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
 * REQ-917: the hot-cache (Redis), Hot replication, read-cache and materialize-store settings, split out of the
 * single "Setup" tab they used to share into one expandable Settings panel per cache type — the
 * store connection with the response cache it backs, the promotion thresholds with the hot tables
 * they promote, the MV TTL and store URL with the materialized store.
 *
 * REQ-1327: the org's settings and the platform's are separate panels, each behind its own right.
 * The response cache TTL and the hot-table and replicate settings are the org's
 * (GET/PUT /admin/settings); the store connections and the materialized-view defaults are the
 * platform's (GET/PUT /admin/cache-storage), rendered only for it.
 */

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Checkbox, Code, NumberInput, Stack, Text, TextInput, Title } from "@mantine/core";
import { TriangleAlert } from "lucide-react";
import { fetchCacheStorage, setCacheStorage, type CacheStorageState } from "../../api/admin";
import { SaveRow } from "./settingsCards";
import { useSettingsBlocks } from "./useSettingsBlocks";
import { CollapsibleItem, Collapsibles } from "../collapsible/Collapsible";
import { PlatformRequired } from "./PlatformRequired";
import { isForbidden } from "./isForbidden";
import { FormChecks, FormField, FormFooter, FormLayout } from "../form/FormLayout";

type StorageBlock = "cache" | "materialized_views" | "materialize";

/** Load the cache-storage config and save back only `blocks`. */
function useCacheStorage(blocks: StorageBlock[]) {
  const { t } = useTranslation();
  const [s, setS] = useState<CacheStorageState | null>(null);
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");
  const [error, setError] = useState("");
  const [forbidden, setForbidden] = useState(false);
  // The block list is a literal at every call site, so the joined form is what `save` depends on.
  const blockKey = blocks.join(",");

  useEffect(() => {
    fetchCacheStorage()
      .then(setS)
      .catch((e) => (isForbidden(e) ? setForbidden(true) : setError(String(e))));
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

  return { s, setS, save, saving, msg, error, forbidden };
}

function SettingsPanel({ testId, children }: { testId: string; children: React.ReactNode }) {
  const { t } = useTranslation();
  return (
    <Collapsibles id={`admin.cache-storage.${testId}`} testId={testId}>
      <CollapsibleItem value="settings" defaultOpen={false} title={t("adminPage.settingsPanel")}>
        <Stack gap="md">{children}</Stack>
      </CollapsibleItem>
    </Collapsibles>
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
  return (
    <>
      <Alert color="yellow" icon={<TriangleAlert size={16} />}>
        {note}
      </Alert>
      <FormFooter onSave={save} saving={saving} saveProps={{ "data-testid": "cache-storage-save" }}>
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
      </FormFooter>
    </>
  );
}

/** Response Cache → Settings: the org's response TTL. */
export function ResponseCacheSettingsPanel() {
  const { t } = useTranslation();
  const { settings, setSettings, save, saving, msg } = useSettingsBlocks(["cache"]);
  if (!settings) return null;

  return (
    <SettingsPanel testId="cache-ttl-settings">
      <FormLayout>
        <FormField help={t("adminPage.defaultTtlHint")} size="short">
          <NumberInput
            label={t("adminPage.defaultTtl")}
            value={settings.cache.default_ttl}
            onChange={(v) =>
              setSettings({ ...settings, cache: { default_ttl: typeof v === "number" ? v : 0 } })
            }
          />
        </FormField>
      </FormLayout>
      <SaveRow save={save} saving={saving} msg={msg} />
    </SettingsPanel>
  );
}

/** Response Cache → the store connection that TTL is written into (the platform's, REQ-1327). */
export function HotCacheStoreSettingsPanel() {
  const { t } = useTranslation();
  const { s, setS, save, saving, msg, error, forbidden } = useCacheStorage(["cache"]);
  if (forbidden) return <PlatformRequired />;
  if (!s) return error ? <Alert color="red">{error}</Alert> : null;

  return (
    <SettingsPanel testId="hot-cache-store-settings">
      <Title order={5}>{t("cacheStorageTab.hotCacheHeading")}</Title>
      <Text c="dimmed" size="sm">
        {t("cacheStorageTab.hotCacheIntro")}
      </Text>
      <FormLayout>
        <FormChecks>
          <FormField size="short">
            <Checkbox
              label={t("cacheStorageTab.enableHotCache")}
              checked={s.cache.enabled}
              onChange={(e) =>
                setS({ ...s, cache: { ...s.cache, enabled: e.currentTarget.checked } })
              }
            />
          </FormField>
        </FormChecks>
        <FormField size="long">
          <TextInput
            label={t("cacheStorageTab.redisUrlLabel")}
            placeholder={t("cacheStorageTab.redisUrlPlaceholder")}
            value={s.cache.redis_url}
            onChange={(e) =>
              setS({ ...s, cache: { ...s.cache, redis_url: e.currentTarget.value } })
            }
          />
        </FormField>
      </FormLayout>
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

/**
 * Hot Tables → Settings: the org's hot-tier thresholds and when a busy table is replicated
 * (REQ-826, REQ-1327).
 */
export function HotTablesSettingsPanel() {
  const { t } = useTranslation();
  const { settings, setSettings, save, saving, msg } = useSettingsBlocks([
    "hot_tables",
    "replication",
  ]);
  if (!settings) return null;
  const { hot_tables: hot, replication } = settings;

  return (
    <SettingsPanel testId="hot-tables-settings">
      <FormLayout>
        <FormField size="short">
          <NumberInput
            label={t("cacheStorageTab.promoteThresholdLabel")}
            value={hot.auto_threshold}
            onChange={(v) =>
              setSettings({ ...settings, hot_tables: { ...hot, auto_threshold: Number(v) } })
            }
          />
        </FormField>
        <FormField size="short">
          <NumberInput
            label={t("cacheStorageTab.maxRowsLabel")}
            value={hot.max_rows}
            onChange={(v) =>
              setSettings({ ...settings, hot_tables: { ...hot, max_rows: Number(v) } })
            }
          />
        </FormField>
        <FormField size="short">
          <NumberInput
            label={t("cacheStorageTab.maxBytesLabel")}
            value={hot.max_bytes}
            onChange={(v) =>
              setSettings({ ...settings, hot_tables: { ...hot, max_bytes: Number(v) } })
            }
          />
        </FormField>
      </FormLayout>

      <Title order={5}>{t("cacheStorageTab.hotReplicationHeading")}</Title>
      <Text c="dimmed" size="sm">
        {t("cacheStorageTab.hotReplicationIntro")}
      </Text>
      <FormLayout>
        <FormField size="short">
          <NumberInput
            label={t("cacheStorageTab.hotThresholdLabel")}
            data-testid="replication-hot-threshold"
            min={1}
            value={replication.hot_threshold}
            onChange={(v) =>
              setSettings({
                ...settings,
                replication: { ...replication, hot_threshold: Number(v) },
              })
            }
          />
        </FormField>
        <FormField size="short">
          <NumberInput
            label={t("cacheStorageTab.hotIntervalLabel")}
            data-testid="replication-hot-interval"
            min={1}
            value={replication.hot_interval}
            onChange={(v) =>
              setSettings({ ...settings, replication: { ...replication, hot_interval: Number(v) } })
            }
          />
        </FormField>
        <FormField size="short">
          <NumberInput
            label={t("cacheStorageTab.hotMaxRowsLabel")}
            data-testid="replication-hot-max-rows"
            min={1}
            value={replication.hot_max_rows}
            onChange={(v) =>
              setSettings({ ...settings, replication: { ...replication, hot_max_rows: Number(v) } })
            }
          />
        </FormField>
      </FormLayout>
      <SaveRow save={save} saving={saving} msg={msg} />
    </SettingsPanel>
  );
}

/** Materialized Store → Settings: the default MV refresh TTL and the store the results land in. */
export function MaterializedSettingsPanel() {
  const { t } = useTranslation();
  const { s, setS, save, saving, msg, error, forbidden } = useCacheStorage([
    "materialized_views",
    "materialize",
  ]);
  if (forbidden) return <PlatformRequired />;
  if (!s) return error ? <Alert color="red">{error}</Alert> : null;

  return (
    <SettingsPanel testId="materialized-settings">
      <FormLayout>
        <FormField help={t("cacheStorageTab.mvDefaultTtlHint")} size="short">
          <NumberInput
            label={t("cacheStorageTab.mvDefaultTtlLabel")}
            value={s.materialized_views.default_ttl ?? ""}
            onChange={(v) =>
              setS({ ...s, materialized_views: { default_ttl: v === "" ? null : Number(v) } })
            }
          />
        </FormField>
      </FormLayout>

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
      <FormLayout>
        <FormField size="long">
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
        </FormField>
      </FormLayout>
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
