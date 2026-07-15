// Copyright (c) 2026 Kenneth Stott
// Canary: a8e46a16-e119-41fb-96ab-bdbd3e691995
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
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
import {
  useCacheStats,
  useCacheTableStats,
  useHotTables,
  useMaterializeStoreInfo,
  useMVList,
  usePurgeCache,
  usePurgeCacheByTable,
  useRefreshMV,
  useTables,
  useToggleMV,
} from "../../hooks/useAdminQueries";
import { ChevronDown, ChevronsUpDown, ChevronUp } from "lucide-react";
import { CacheStorageTab } from "./CacheStorageTab";
import { FilterInput } from "./FilterInput";
import { displayMvName } from "./mvDisplay";

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

type TabKey = "response" | "hot" | "materialized" | "setup";

const TAB_KEYS: TabKey[] = ["response", "hot", "materialized", "setup"];

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

type SortDir = "asc" | "desc";

function SortableTh({
  label,
  active,
  dir,
  onSort,
}: {
  label: string;
  active: boolean;
  dir: SortDir;
  onSort: () => void;
}) {
  const Icon = !active ? ChevronsUpDown : dir === "asc" ? ChevronUp : ChevronDown;
  return (
    <Table.Th
      onClick={onSort}
      style={{ cursor: "pointer", userSelect: "none" }}
    >
      <Group gap={4} wrap="nowrap">
        {label}
        <Icon size={14} opacity={active ? 1 : 0.4} />
      </Group>
    </Table.Th>
  );
}

export function CacheManager() {
  const { t } = useTranslation();
  const [tab, setTab] = useState<TabKey>("response");
  return (
    <div>
      <Tabs
        value={tab}
        onChange={(v) => setTab((v as TabKey) ?? "response")}
        mb="md"
      >
        <Tabs.List>
          {TAB_KEYS.map((k) => (
            <Tabs.Tab key={k} value={k} data-testid={`cache-tab-${k}`}>
              {t(`cacheManager.tabs.${k}`)}
            </Tabs.Tab>
          ))}
        </Tabs.List>
      </Tabs>
      {tab === "response" && <ResponseCacheTab />}
      {tab === "hot" && <HotTablesTab />}
      {tab === "materialized" && <MaterializedStoreTab />}
      {tab === "setup" && <CacheStorageTab />}
    </div>
  );
}

function ResponseCacheTab() {
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
  const [sortKey, setSortKey] = useState<"table" | "domain" | "entries">("table");
  const [sortDir, setSortDir] = useState<SortDir>("asc");

  const toggleSort = (key: "table" | "domain" | "entries") => {
    if (sortKey === key) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir("asc");
    }
    setTablePage(0);
  };

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

  const hitRate = stats.hitCount + stats.missCount > 0
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
  const memPct = stats.usedMemoryBytes != null && stats.maxMemoryBytes
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
  const sorted = [...filtered].sort((a, b) => {
    let cmp: number;
    if (sortKey === "entries") {
      cmp = (entriesByTable.get(a.id) ?? 0) - (entriesByTable.get(b.id) ?? 0);
    } else if (sortKey === "domain") {
      cmp = (a.domainId ?? "").localeCompare(b.domainId ?? "");
    } else {
      cmp = (a.alias || a.tableName).localeCompare(b.alias || b.tableName);
    }
    return sortDir === "asc" ? cmp : -cmp;
  });
  const totalPages = Math.max(1, Math.ceil(sorted.length / PAGE_SIZE));
  const safePage = Math.min(tablePage, totalPages - 1);
  const paged = sorted.slice(safePage * PAGE_SIZE, (safePage + 1) * PAGE_SIZE);

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
            <StatCard value={stats.evictedKeys ?? unknown} label={t("cacheManager.response.evicted")} />
            <StatCard value={stats.expiredKeys ?? unknown} label={t("cacheManager.response.expired")} />
            <StatCard value={stats.connectedClients ?? unknown} label={t("cacheManager.response.clients")} />
            <StatCard value={stats.opsPerSec ?? unknown} label={t("cacheManager.response.opsPerSec")} />
          </>
        )}
      </SimpleGrid>

      {tables.length > 0 && (
        <Stack gap="sm">
          <Group gap="sm" align="center">
            <FilterInput
              value={tableSearch}
              onChange={(v) => { setTableSearch(v); setTablePage(0); }}
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
          <Table striped highlightOnHover withTableBorder>
            <Table.Thead>
              <Table.Tr>
                <SortableTh
                  label={t("cacheManager.response.table")}
                  active={sortKey === "table"}
                  dir={sortDir}
                  onSort={() => toggleSort("table")}
                />
                <SortableTh
                  label={t("cacheManager.response.domain")}
                  active={sortKey === "domain"}
                  dir={sortDir}
                  onSort={() => toggleSort("domain")}
                />
                <SortableTh
                  label={t("cacheManager.response.cachedEntries")}
                  active={sortKey === "entries"}
                  dir={sortDir}
                  onSort={() => toggleSort("entries")}
                />
                <Table.Th />
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {paged.map((tbl) => (
                <Table.Tr key={tbl.id}>
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
                </Table.Tr>
              ))}
            </Table.Tbody>
          </Table>
          {totalPages > 1 && (
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

function HotTablesTab() {
  const { t } = useTranslation();
  const unknown = t("cacheManager.hot.unknown");
  const { hotTables } = useHotTables();
  const loaded = hotTables.filter((h) => h.loaded);
  const totalRows = loaded.reduce((n, h) => n + h.rowCount, 0);
  return (
    <Stack gap="md">
      <Text size="sm" c="dimmed">
        {t("cacheManager.hot.description")}
      </Text>
      <SimpleGrid cols={{ base: 2, sm: 3 }}>
        <StatCard value={loaded.length} label={t("cacheManager.hot.loadedTables")} />
        <StatCard value={hotTables.length - loaded.length} label={t("cacheManager.hot.candidates")} />
        <StatCard value={totalRows} label={t("cacheManager.hot.cachedRows")} />
      </SimpleGrid>
      {hotTables.length === 0 ? (
        <Text c="dimmed">{t("cacheManager.hot.empty")}</Text>
      ) : (
        <Table striped highlightOnHover withTableBorder>
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("cacheManager.hot.table")}</Table.Th>
              <Table.Th>{t("cacheManager.hot.catalog")}</Table.Th>
              <Table.Th>{t("cacheManager.hot.schema")}</Table.Th>
              <Table.Th>{t("cacheManager.hot.rows")}</Table.Th>
              <Table.Th>{t("cacheManager.hot.kind")}</Table.Th>
              <Table.Th>{t("cacheManager.hot.state")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {hotTables.map((h) => (
              <Table.Tr key={`${h.catalog}.${h.schemaName}.${h.tableName}`}>
                <Table.Td>{h.tableName}</Table.Td>
                <Table.Td>{h.catalog}</Table.Td>
                <Table.Td>{h.schemaName}</Table.Td>
                <Table.Td>{h.loaded ? h.rowCount : unknown}</Table.Td>
                <Table.Td>{h.isApi ? t("cacheManager.hot.kindApi") : t("cacheManager.hot.kindEngine")}</Table.Td>
                <Table.Td>{h.loaded ? t("cacheManager.hot.stateLoaded") : t("cacheManager.hot.stateCandidate")}</Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      )}
    </Stack>
  );
}

function MaterializedStoreTab() {
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

  const totalPages = Math.max(1, Math.ceil(mvList.length / PAGE_SIZE));
  const paged = mvList.slice(mvPage * PAGE_SIZE, (mvPage + 1) * PAGE_SIZE);

  return (
    <Stack gap="md">
      <Text size="sm" c="dimmed">
        {t("cacheManager.materialized.description")}
      </Text>
      <SimpleGrid cols={{ base: 2, sm: 2 }}>
        <StatCard value={info?.engineName ?? unknown} label={t("cacheManager.materialized.federationEngine")} />
        <StatCard value={info?.mvCount ?? unknown} label={t("cacheManager.materialized.materializedViews")} />
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
          <Table striped highlightOnHover withTableBorder>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>{t("cacheManager.materialized.view")}</Table.Th>
                <Table.Th>{t("cacheManager.materialized.sourceTables")}</Table.Th>
                <Table.Th>{t("cacheManager.materialized.target")}</Table.Th>
                <Table.Th>{t("cacheManager.materialized.status")}</Table.Th>
                <Table.Th>{t("cacheManager.materialized.rows")}</Table.Th>
                <Table.Th>{t("cacheManager.materialized.lastRefresh")}</Table.Th>
                <Table.Th>{t("cacheManager.materialized.interval")}</Table.Th>
                <Table.Th>{t("cacheManager.materialized.error")}</Table.Th>
                <Table.Th />
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {paged.map((mv) => (
                <Table.Tr key={mv.id}>
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
                </Table.Tr>
              ))}
            </Table.Tbody>
          </Table>
          {totalPages > 1 && (
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
    </Stack>
  );
}
