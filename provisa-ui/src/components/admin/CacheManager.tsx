// Copyright (c) 2026 Kenneth Stott
// Canary: a8e46a16-e119-41fb-96ab-bdbd3e691995
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Badge,
  Button,
  Card,
  Group,
  Pagination,
  SimpleGrid,
  Stack,
  Table,
  Tabs,
  Text,
} from "@mantine/core";
import { usePurgeCache, useTables } from "../../hooks/useAdminQueries";
import {
  useCacheStats,
  useCacheTableStats,
  useHotTables,
  useMaterializeStoreInfo,
  useMVList,
  usePurgeCacheByTable,
  useRefreshMV,
  useToggleMV,
} from "../../hooks/useAdminOpsQueries";
import {
  HotTablesSettingsPanel,
  MaterializedSettingsPanel,
  ResponseCacheSettingsPanel,
} from "./CacheStorageTab";
import { RedirectSettingsCard } from "./settingsCards";
import { fetchSettings } from "../../api/admin";
import { FilterInput } from "./FilterInput";
import { useRegionChoices } from "../../hooks/useRegionQueries";
import { useRegionSelection } from "../../hooks/useRegionSelection";
import { filterByRegion } from "../../hooks/regionFilter";
import { RegionSelector } from "../RegionSelector";
import { displayMvName } from "./mvDisplay";
import { ListRow } from "../list/ListTable";
import { SortGroupTable } from "../list/SortGroupTable";

const PAGE_SIZE = 50;

function fmtBytes(n: number | null, unknown: string): string {
  if (n == null) return unknown;
  const units = ["B", "KB", "MB", "GB", "TB"];
  let v = n;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

// REQ-1349: "redirect" sits here because a redirected result IS a cached result handed back as a
// URL — the org setting that governs it belongs beside the response cache it is cut from, not on
// the Admin overview. The deployment-wide store settings that used to be one "Setup" tab are now a
// Settings panel on the tab of the cache type each one configures.
type TabKey = "response" | "hot" | "materialized" | "redirect";

const TAB_KEYS: TabKey[] = ["response", "hot", "materialized", "redirect"];

const MV_STATUS_COLOR: Record<string, string> = {
  fresh: "green",
  stale: "yellow",
  refreshing: "blue",
  disabled: "gray",
};

function StatCard({ value, label }: { value: string | number; label: string }) {
  return (
    <Card withBorder padding="sm" radius="md" data-testid="stat-card">
      <Text fw={700} size="lg">
        {value}
      </Text>
      <Text size="xs" c="dimmed">
        {label}
      </Text>
    </Card>
  );
}

export function CacheManager() {
  const { t } = useTranslation();
  const [tab, setTab] = useState<TabKey>("response");
  const [platform, setPlatform] = useState(false);
  useEffect(() => {
    fetchSettings().then((s) => setPlatform(Boolean(s.features?.deployment_settings)));
  }, []);
  return (
    <div>
      <Tabs value={tab} onChange={(v) => setTab((v as TabKey) ?? "response")} mb="md">
        <Tabs.List>
          {TAB_KEYS.map((k) => (
            <Tabs.Tab key={k} value={k} data-testid={`cache-tab-${k}`}>
              {t(`cacheManager.tabs.${k}`)}
            </Tabs.Tab>
          ))}
        </Tabs.List>
      </Tabs>
      {tab === "response" && <ResponseCacheTab platform={platform} />}
      {tab === "hot" && <HotTablesTab platform={platform} />}
      {tab === "materialized" && <MaterializedStoreTab platform={platform} />}
      {tab === "redirect" && <RedirectSettingsCard />}
    </div>
  );
}

function ResponseCacheTab({ platform }: { platform: boolean }) {
  const { t } = useTranslation();
  const unknown = t("cacheManager.response.unknown");
  const { cacheStats: stats, refetch: refetchStats } = useCacheStats();
  const { cacheTableStats, refetch: refetchTableStats } = useCacheTableStats();
  const { tables } = useTables();
  const entriesByTable = new Map(cacheTableStats.map((s) => [s.tableId, s.cachedEntries]));
  const { purgeCache } = usePurgeCache();
  const { purgeCacheByTable } = usePurgeCacheByTable();
  const [purging, setPurging] = useState(false);
  const [msg, setMsg] = useState("");
  const [tableSearch, setTableSearch] = useState("");
  const [tablePage, setTablePage] = useState(0);
  const [tablesGrouped, setTablesGrouped] = useState(false);
  const handlePurgeAll = async () => {
    setPurging(true);
    setMsg("");
    const result = await purgeCache();
    setMsg(result.message);
    setPurging(false);
    await refetchStats();
    await refetchTableStats();
  };

  const handlePurgeTable = async (tableId: number, tableName: string) => {
    setMsg("");
    const result = await purgeCacheByTable(tableId);
    setMsg(`${tableName}: ${result.message}`);
    await refetchStats();
    await refetchTableStats();
  };

  if (!stats) return <Text>{t("cacheManager.response.loading")}</Text>;

  const hitRate =
    stats.hitCount + stats.missCount > 0
      ? ((stats.hitCount / (stats.hitCount + stats.missCount)) * 100).toFixed(1)
      : unknown;

  // Logical cached-result count. stats.totalKeys is a raw Redis DBSIZE (data + :meta
  // per entry + one table-index set per referenced table), so it overcounts entries by
  // a non-constant factor. The per-table index sums to the real entry total.
  const totalEntries = cacheTableStats.reduce((n, s) => n + s.cachedEntries, 0);

  const isRedis = stats.storeType === "redis";
  // "memory" = embedded fakeredis: an enabled store, just without Redis INFO metrics.
  const isEnabled = stats.storeType !== "noop";
  const memUsed = fmtBytes(stats.usedMemoryBytes, unknown);
  const memPct =
    stats.usedMemoryBytes != null && stats.maxMemoryBytes
      ? ` / ${((stats.usedMemoryBytes / stats.maxMemoryBytes) * 100).toFixed(0)}%`
      : "";

  const q = tableSearch.toLowerCase();
  // Hide Provisa's own internal catalog (meta/ops system views) — matches TablesPage.
  const userTables = tables.filter(
    (tbl) => tbl.sourceId !== "provisa-admin" && tbl.sourceId !== "provisa-otel",
  );
  const filtered = userTables.filter(
    (tbl) =>
      (tbl.alias || tbl.tableName).toLowerCase().includes(q) ||
      (tbl.domainId ?? "").toLowerCase().includes(q),
  );
  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const safePage = Math.min(tablePage, totalPages - 1);

  return (
    <Stack gap="md">
      {!isEnabled && (
        <Alert color="yellow" data-testid="response-cache-disabled-banner">
          {t("cacheManager.response.disabledBanner", { storeType: stats.storeType })}
        </Alert>
      )}
      <SimpleGrid cols={{ base: 2, sm: 3, md: isRedis ? 6 : 5 }}>
        <StatCard value={totalEntries} label={t("cacheManager.response.cachedEntries")} />
        <StatCard value={`${hitRate}%`} label={t("cacheManager.response.hitRate")} />
        <StatCard value={stats.hitCount} label={t("cacheManager.response.hits")} />
        <StatCard value={stats.missCount} label={t("cacheManager.response.misses")} />
        <StatCard value={stats.storeType} label={t("cacheManager.response.store")} />
        {isRedis && (
          <>
            <StatCard value={stats.totalKeys} label={t("cacheManager.response.redisKeysRaw")} />
            <StatCard value={`${memUsed}${memPct}`} label={t("cacheManager.response.memory")} />
            <StatCard
              value={stats.evictedKeys ?? unknown}
              label={t("cacheManager.response.evicted")}
            />
            <StatCard
              value={stats.expiredKeys ?? unknown}
              label={t("cacheManager.response.expired")}
            />
            <StatCard
              value={stats.connectedClients ?? unknown}
              label={t("cacheManager.response.clients")}
            />
            <StatCard
              value={stats.opsPerSec ?? unknown}
              label={t("cacheManager.response.opsPerSec")}
            />
          </>
        )}
      </SimpleGrid>

      <ResponseCacheSettingsPanel platform={platform} />

      {tables.length > 0 && (
        <Stack gap="sm">
          <Group gap="sm" align="center">
            <FilterInput
              value={tableSearch}
              onChange={(v) => {
                setTableSearch(v);
                setTablePage(0);
              }}
              placeholder={t("cacheManager.response.filterPlaceholder")}
            />
            {msg && (
              <Text size="sm" c="dimmed" data-testid="response-cache-msg">
                {msg}
              </Text>
            )}
            <Button
              color="red"
              variant="light"
              onClick={handlePurgeAll}
              disabled={purging}
              data-testid="purge-all-cache-btn"
            >
              {purging ? t("cacheManager.response.purging") : t("cacheManager.response.purgeAll")}
            </Button>
          </Group>
          <SortGroupTable
            testPrefix="cache-tables"
            rows={filtered}
            columns={[
              {
                key: "table",
                label: t("cacheManager.response.table"),
                sortValue: (x) => x.alias || x.tableName,
              },
              {
                key: "domain",
                label: t("cacheManager.response.domain"),
                sortValue: (x) => x.domainId ?? "",
                groupValue: (x) => x.domainId ?? "",
              },
              {
                key: "entries",
                label: t("cacheManager.response.cachedEntries"),
                sortValue: (x) => entriesByTable.get(x.id) ?? 0,
              },
            ]}
            headers={[{ col: "table" }, { col: "domain" }, { col: "entries" }, ""]}
            colSpan={4}
            rowKey={(tbl) => tbl.id}
            page={{ index: safePage, size: PAGE_SIZE }}
            onGroupedChange={setTablesGrouped}
            render={(tbl) => (
              <ListRow key={tbl.id}>
                <Table.Td>{tbl.alias || tbl.tableName}</Table.Td>
                <Table.Td>{tbl.domainId}</Table.Td>
                <Table.Td>{entriesByTable.get(tbl.id) ?? 0}</Table.Td>
                <Table.Td>
                  <Button
                    size="xs"
                    variant="subtle"
                    onClick={() => handlePurgeTable(tbl.id, tbl.tableName)}
                    data-testid={`purge-table-btn-${tbl.id}`}
                  >
                    {t("cacheManager.response.purgeTable")}
                  </Button>
                </Table.Td>
              </ListRow>
            )}
          />
          {totalPages > 1 && !tablesGrouped && (
            <Group justify="flex-end">
              <Pagination
                total={totalPages}
                value={safePage + 1}
                onChange={(p) => setTablePage(p - 1)}
                size="sm"
              />
            </Group>
          )}
        </Stack>
      )}
    </Stack>
  );
}

/** Exported for its component test. */
export function HotTablesTab({ platform }: { platform: boolean }) {
  const { t } = useTranslation();
  const unknown = t("cacheManager.hot.unknown");
  const { hotTables } = useHotTables();
  const loaded = hotTables.filter((h) => h.kind === "hot");
  // REQ-826: past their Hot threshold — served from a replica, or read live while it is built.
  const busy = hotTables.filter((h) => h.kind === "replica" || h.kind === "replica_building");
  const candidates = hotTables.filter((h) => h.kind === "hot_candidate");
  const totalRows = [...loaded, ...busy].reduce((n, h) => n + h.rowCount, 0);
  // REQ-1922: the shared region selector filters the list (the stat totals stay whole-estate).
  const { regions, connected, error: regionError } = useRegionChoices();
  const [regionSel, setRegionSel] = useRegionSelection(regions, connected);
  const hasRegions = regions.length > 0;
  const { visible: shownHot, hidden: hotHidden } = filterByRegion(
    hotTables,
    regionSel,
    connected,
    (h) => h.region ?? null,
  );
  type HotRow = (typeof hotTables)[number];
  return (
    <Stack gap="md">
      <Text size="sm" c="dimmed">
        {t("cacheManager.hot.description")}
      </Text>
      <Text size="sm" c="dimmed">
        {t("cacheManager.hot.busyDescription")}
      </Text>
      <SimpleGrid cols={{ base: 2, sm: 4 }}>
        <StatCard value={loaded.length} label={t("cacheManager.hot.hotTables")} />
        <StatCard value={busy.length} label={t("cacheManager.hot.busyReplicas")} />
        <StatCard value={candidates.length} label={t("cacheManager.hot.hotCandidates")} />
        <StatCard value={totalRows} label={t("cacheManager.hot.cachedRows")} />
      </SimpleGrid>
      <RegionSelector
        regions={regions}
        connected={connected}
        value={regionSel}
        onChange={setRegionSel}
        hidden={hotHidden}
        error={regionError}
      />
      {hotTables.length === 0 ? (
        <Text c="dimmed">{t("cacheManager.hot.empty")}</Text>
      ) : (
        <SortGroupTable
          testPrefix="cache-hot"
          rows={shownHot}
          columns={[
            { key: "table", label: t("cacheManager.hot.table"), sortValue: (h) => h.tableName },
            {
              key: "catalog",
              label: t("cacheManager.hot.catalog"),
              sortValue: (h) => h.catalog,
              groupValue: (h) => h.catalog,
            },
            {
              key: "schema",
              label: t("cacheManager.hot.schema"),
              sortValue: (h) => h.schemaName,
              groupValue: (h) => h.schemaName,
            },
            // REQ-1922: the table's region, when the platform declares regions.
            ...(hasRegions
              ? [
                  {
                    key: "region",
                    label: t("regionSelector.columnHeader"),
                    sortValue: (h: HotRow) => h.region ?? "",
                    groupValue: (h: HotRow) => h.region ?? t("regionSelector.noRegion"),
                  },
                ]
              : []),
            { key: "rows", label: t("cacheManager.hot.rows"), sortValue: (h) => h.rowCount },
            {
              key: "kind",
              label: t("cacheManager.hot.kind"),
              sortValue: (h) => t(`cacheManager.hot.kind_${h.kind}`),
              groupValue: (h) => t(`cacheManager.hot.kind_${h.kind}`),
            },
          ]}
          headers={[
            { col: "table" },
            { col: "catalog" },
            { col: "schema" },
            ...(hasRegions ? [{ col: "region" }] : []),
            { col: "rows" },
            { col: "kind" },
          ]}
          colSpan={hasRegions ? 6 : 5}
          rowKey={(h) => `${h.kind}:${h.catalog}.${h.schemaName}.${h.tableName}`}
          render={(h) => (
            <ListRow key={`${h.kind}:${h.catalog}.${h.schemaName}.${h.tableName}`}>
              <Table.Td>{h.tableName}</Table.Td>
              <Table.Td>{h.catalog}</Table.Td>
              <Table.Td>{h.schemaName}</Table.Td>
              {hasRegions && <Table.Td>{h.region ?? t("regionSelector.noRegion")}</Table.Td>}
              {/* A candidate has nothing mirrored yet, and a replica still being built has
                    nothing to read yet: neither has a row count to report. */}
              <Table.Td>
                {h.kind === "hot_candidate" || h.kind === "replica_building" ? unknown : h.rowCount}
              </Table.Td>
              <Table.Td>{t(`cacheManager.hot.kind_${h.kind}`)}</Table.Td>
            </ListRow>
          )}
        />
      )}
      {platform && <HotTablesSettingsPanel />}
    </Stack>
  );
}

function MaterializedStoreTab({ platform }: { platform: boolean }) {
  const { t } = useTranslation();
  const unknown = t("cacheManager.materialized.unknown");
  const navigate = useNavigate();
  const { materializeStoreInfo: info } = useMaterializeStoreInfo();
  const { mvList } = useMVList();
  const { refreshMV } = useRefreshMV();
  const { toggleMV } = useToggleMV();
  const [refreshing, setRefreshing] = useState<string | null>(null);
  const [mvPage, setMvPage] = useState(0);

  const handleRefresh = async (id: string) => {
    setRefreshing(id);
    await refreshMV(id);
    setRefreshing(null);
  };

  const [mvGrouped, setMvGrouped] = useState(false);
  const totalPages = Math.max(1, Math.ceil(mvList.length / PAGE_SIZE));

  return (
    <Stack gap="md">
      <Text size="sm" c="dimmed">
        {t("cacheManager.materialized.description")}
      </Text>
      <SimpleGrid cols={{ base: 2, sm: 2 }}>
        <StatCard
          value={info?.engineName ?? unknown}
          label={t("cacheManager.materialized.federationEngine")}
        />
        <StatCard
          value={info?.mvCount ?? unknown}
          label={t("cacheManager.materialized.materializedViews")}
        />
      </SimpleGrid>
      {info?.storeRef && (
        <Text size="sm" c="dimmed">
          {t("cacheManager.materialized.storeLabel")} <code>{info.storeRef}</code>
        </Text>
      )}
      <Group justify="flex-end">
        <Button variant="light" onClick={() => navigate("/sql")} data-testid="mv-view-btn">
          {t("cacheManager.materialized.viewButton")}
        </Button>
      </Group>
      {mvList.length === 0 ? (
        <Text c="dimmed">{t("cacheManager.materialized.empty")}</Text>
      ) : (
        <>
          <SortGroupTable
            testPrefix="cache-mv"
            rows={mvList}
            columns={[
              {
                key: "view",
                label: t("cacheManager.materialized.view"),
                sortValue: (mv) => displayMvName(mv.id),
              },
              {
                key: "sourceTables",
                label: t("cacheManager.materialized.sourceTables"),
                sortValue: (mv) => mv.sourceTables.join(", "),
              },
              {
                key: "target",
                label: t("cacheManager.materialized.target"),
                sortValue: (mv) => mv.targetTable,
              },
              {
                key: "status",
                label: t("cacheManager.materialized.status"),
                sortValue: (mv) => mv.status,
                groupValue: (mv) => mv.status,
              },
              {
                key: "rows",
                label: t("cacheManager.materialized.rows"),
                sortValue: (mv) => mv.rowCount ?? 0,
              },
              {
                key: "lastRefresh",
                label: t("cacheManager.materialized.lastRefresh"),
                sortValue: (mv) => mv.lastRefreshAt ?? 0,
              },
              {
                key: "interval",
                label: t("cacheManager.materialized.interval"),
                sortValue: (mv) => mv.refreshInterval,
              },
            ]}
            headers={[
              { col: "view" },
              { col: "sourceTables" },
              { col: "target" },
              { col: "status" },
              { col: "rows" },
              { col: "lastRefresh" },
              { col: "interval" },
              t("cacheManager.materialized.error"),
            ]}
            colSpan={8}
            rowKey={(mv) => mv.id}
            page={{ index: mvPage, size: PAGE_SIZE }}
            onGroupedChange={setMvGrouped}
            render={(mv) => (
              <ListRow key={mv.id}>
                <Table.Td>
                  {/* Show the user's alias; mv.id stays the action key below. */}
                  <code>{displayMvName(mv.id)}</code>
                </Table.Td>
                <Table.Td>{mv.sourceTables.join(", ")}</Table.Td>
                <Table.Td>
                  <code>{mv.targetTable}</code>
                </Table.Td>
                <Table.Td>
                  <Badge color={MV_STATUS_COLOR[mv.status] ?? "gray"} variant="light">
                    {mv.status}
                  </Badge>
                </Table.Td>
                <Table.Td>{mv.rowCount ?? unknown}</Table.Td>
                <Table.Td>
                  {mv.lastRefreshAt
                    ? new Date(mv.lastRefreshAt * 1000).toLocaleTimeString()
                    : t("cacheManager.materialized.never")}
                </Table.Td>
                <Table.Td>{mv.refreshInterval}s</Table.Td>
                <Table.Td maw={200} c="red">
                  {mv.lastError || ""}
                </Table.Td>
                <Table.Td>
                  <Group gap="xs" wrap="nowrap">
                    <Button
                      size="xs"
                      variant="subtle"
                      onClick={() => handleRefresh(mv.id)}
                      disabled={refreshing === mv.id}
                      data-testid={`mv-refresh-btn-${mv.id}`}
                    >
                      {refreshing === mv.id
                        ? t("cacheManager.materialized.refreshing")
                        : t("cacheManager.materialized.refresh")}
                    </Button>
                    <Button
                      size="xs"
                      variant="subtle"
                      onClick={() => toggleMV(mv.id, !mv.enabled)}
                      data-testid={`mv-toggle-btn-${mv.id}`}
                    >
                      {mv.enabled
                        ? t("cacheManager.materialized.disable")
                        : t("cacheManager.materialized.enable")}
                    </Button>
                  </Group>
                </Table.Td>
              </ListRow>
            )}
          />
          {totalPages > 1 && !mvGrouped && (
            <Group justify="flex-end">
              <Pagination
                total={totalPages}
                value={mvPage + 1}
                onChange={(p) => setMvPage(p - 1)}
                size="sm"
              />
            </Group>
          )}
        </>
      )}
      {platform && <MaterializedSettingsPanel />}
    </Stack>
  );
}
