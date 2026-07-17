// Copyright (c) 2026 Kenneth Stott
// Canary: 4b7d1a5f-1735-4307-b713-e1f2db113892
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Button,
  Checkbox,
  Code,
  Group,
  Loader,
  NumberInput,
  SimpleGrid,
  Stack,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { Check, TriangleAlert } from "lucide-react";
import { fetchCacheStorage, setCacheStorage, type CacheStorageState } from "../../api/admin";

// REQ-917: configure the Redis hot cache + materialize store. Both bind connections at startup,
// so changes take effect on the next service restart.
export function CacheStorageTab() {
  const { t } = useTranslation();
  const [s, setS] = useState<CacheStorageState | null>(null);
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");
  const [error, setError] = useState("");

  useEffect(() => {
    fetchCacheStorage().then(setS).catch((e) => setError(String(e)));
  }, []);

  const save = async () => {
    if (!s) return;
    setSaving(true);
    setMsg("");
    setError("");
    try {
      const res = await setCacheStorage({
        cache: s.cache,
        hot_tables: s.hot_tables,
        warm_tables: s.warm_tables,
        materialized_views: s.materialized_views,
        materialize: s.materialize,
      });
      setMsg(
        res.restart_required
          ? t("cacheStorageTab.savedRestartRequired")
          : t("cacheStorageTab.saved")
      );
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  if (error && !s) return <Alert color="red">{error}</Alert>;
  if (!s)
    return (
      <Group gap="xs">
        <Loader size="sm" />
        <Text>{t("cacheStorageTab.loading")}</Text>
      </Group>
    );

  return (
    <Stack maw={720} gap="md">
      <Title order={4}>{t("cacheStorageTab.hotCacheHeading")}</Title>
      <Text c="dimmed" size="sm">
        {t("cacheStorageTab.hotCacheIntro")}
      </Text>

      <Stack gap="sm">
        <Checkbox
          label={t("cacheStorageTab.enableHotCache")}
          checked={s.cache.enabled}
          onChange={(e) => setS({ ...s, cache: { ...s.cache, enabled: e.currentTarget.checked } })}
        />

        <TextInput
          label={t("cacheStorageTab.redisUrlLabel")}
          placeholder={t("cacheStorageTab.redisUrlPlaceholder")}
          value={s.cache.redis_url}
          onChange={(e) => setS({ ...s, cache: { ...s.cache, redis_url: e.currentTarget.value } })}
        />

        <SimpleGrid cols={{ base: 1, sm: 3 }}>
          <NumberInput
            label={t("cacheStorageTab.defaultTtlLabel")}
            value={s.cache.default_ttl ?? ""}
            onChange={(v) =>
              setS({
                ...s,
                cache: { ...s.cache, default_ttl: v === "" ? null : Number(v) },
              })
            }
          />
          <NumberInput
            label={t("cacheStorageTab.promoteThresholdLabel")}
            value={s.hot_tables.auto_threshold}
            onChange={(v) =>
              setS({
                ...s,
                hot_tables: { ...s.hot_tables, auto_threshold: Number(v) },
              })
            }
          />
          <NumberInput
            label={t("cacheStorageTab.maxRowsLabel")}
            value={s.hot_tables.max_rows}
            onChange={(v) =>
              setS({ ...s, hot_tables: { ...s.hot_tables, max_rows: Number(v) } })
            }
          />
        </SimpleGrid>
        <NumberInput
          label={t("cacheStorageTab.maxBytesLabel")}
          value={s.hot_tables.max_bytes}
          onChange={(v) =>
            setS({ ...s, hot_tables: { ...s.hot_tables, max_bytes: Number(v) } })
          }
        />
      </Stack>

      <Title order={4}>{t("cacheStorageTab.warmHeading")}</Title>
      <Text c="dimmed" size="sm">
        {t("cacheStorageTab.warmIntro")}
      </Text>
      <Stack gap="sm">
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
            onChange={(v) =>
              setS({ ...s, warm_tables: { ...s.warm_tables, max_rows: Number(v) } })
            }
          />
          <NumberInput
            label={t("cacheStorageTab.warmRefreshLabel")}
            value={s.warm_tables.refresh_interval ?? ""}
            onChange={(v) =>
              setS({
                ...s,
                warm_tables: {
                  ...s.warm_tables,
                  refresh_interval: v === "" ? null : Number(v),
                },
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
      </Stack>

      <Title order={4}>{t("cacheStorageTab.mvHeading")}</Title>
      <NumberInput
        label={t("cacheStorageTab.mvDefaultTtlLabel")}
        description={t("cacheStorageTab.mvDefaultTtlHint")}
        value={s.materialized_views.default_ttl ?? ""}
        onChange={(v) =>
          setS({
            ...s,
            materialized_views: { default_ttl: v === "" ? null : Number(v) },
          })
        }
      />

      <Title order={4}>{t("cacheStorageTab.materializeHeading")}</Title>
      <Text c="dimmed" size="sm">
        {t("cacheStorageTab.materializeIntroPrefix")}
        {s.materialize.default_store_url ? (
          <>
            {" "}
            —{" "}
            <Code>{s.materialize.default_store_url}</Code>
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

      <Alert color="yellow" icon={<TriangleAlert size={16} />}>
        {s.restart_required_note}
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
    </Stack>
  );
}
