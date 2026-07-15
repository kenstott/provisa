// Copyright (c) 2026 Kenneth Stott
// Canary: 38295f0d-fd9a-40c3-aba0-27d3a27ce193
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect } from "react";
import { useTranslation } from "react-i18next";
import { Box, Group, Table, Text } from "@mantine/core";
import { useSystemHealth } from "../../hooks/useAdminQueries";

// null = neutral/disabled (grey), true = ok (green), false = down (red)
function StatusDot({ ok }: { ok: boolean | null }) {
  const color = ok === null ? "var(--text-muted, #6b7280)" : ok ? "var(--success, #22c55e)" : "var(--error, #ef4444)";
  return (
    <Box
      component="span"
      aria-hidden="true"
      style={{
        display: "inline-block",
        width: 10,
        height: 10,
        borderRadius: "50%",
        background: color,
        marginRight: "0.5rem",
      }}
    />
  );
}

export function SystemHealth() {
  const { t } = useTranslation();
  const { systemHealth: health, refetch } = useSystemHealth();

  useEffect(() => {
    const interval = setInterval(() => refetch(), 10000);
    return () => clearInterval(interval);
  }, [refetch]);

  if (!health) return <Text>{t("systemHealth.loading")}</Text>;

  return (
    <Table.ScrollContainer minWidth={640}>
      <Table striped highlightOnHover withTableBorder verticalSpacing="xs">
        <Table.Thead>
          <Table.Tr>
            <Table.Th>{t("systemHealth.colComponent")}</Table.Th>
            <Table.Th>{t("systemHealth.colStatus")}</Table.Th>
            <Table.Th>{t("systemHealth.colDetails")}</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          <Table.Tr>
            <Table.Td>{t("systemHealth.federationEngine")}</Table.Td>
            <Table.Td>
              <Group gap={0} wrap="nowrap">
                <StatusDot ok={health.engineConnected} />
                {health.engineConnected ? t("systemHealth.connected") : t("systemHealth.disconnected")}
              </Group>
            </Table.Td>
            <Table.Td>
              {health.engineConnected
                ? t("systemHealth.workerCount", {
                    count: health.engineWorkerCount,
                    active: health.engineActiveWorkers,
                  })
                : ""}
            </Table.Td>
          </Table.Tr>
          <Table.Tr>
            <Table.Td>
              {t("systemHealth.metadataDb")}
              {health.metadataDialect ? ` (${health.metadataDialect})` : ""}
            </Table.Td>
            <Table.Td>
              <Group gap={0} wrap="nowrap">
                <StatusDot ok={health.metadataPoolSize < 0 ? null : health.metadataPoolSize > 0} />
                {health.metadataPoolSize < 0
                  ? t("systemHealth.unpooled")
                  : health.metadataPoolSize > 0
                    ? t("systemHealth.active")
                    : t("systemHealth.noPool")}
              </Group>
            </Table.Td>
            <Table.Td>
              {health.metadataPoolSize < 0
                ? t("systemHealth.poolNotTracked")
                : t("systemHealth.poolConnections", {
                    count: health.metadataPoolSize,
                    idle: health.metadataPoolFree,
                  })}
            </Table.Td>
          </Table.Tr>
          <Table.Tr>
            <Table.Td>{t("systemHealth.cache")}</Table.Td>
            <Table.Td>
              <Group gap={0} wrap="nowrap">
                <StatusDot ok={health.cacheMode === "disabled" ? null : health.cacheConnected} />
                {health.cacheMode === "disabled"
                  ? t("systemHealth.disabled")
                  : health.cacheMode === "embedded"
                    ? t("systemHealth.embeddedMemory")
                    : health.cacheConnected
                      ? t("systemHealth.serverConnected")
                      : t("systemHealth.serverUnreachable")}
              </Group>
            </Table.Td>
            <Table.Td>
              {health.cacheMode === "server"
                ? t("systemHealth.redisServer")
                : health.cacheMode === "embedded"
                  ? t("systemHealth.fakeredis")
                  : ""}
            </Table.Td>
          </Table.Tr>
          {health.protocols.map((p) => (
            <Table.Tr key={p.name}>
              <Table.Td>{p.name}</Table.Td>
              <Table.Td>
                <Group gap={0} wrap="nowrap">
                  <StatusDot ok={p.status === "disabled" ? null : p.status === "running"} />
                  {p.status === "disabled"
                    ? t("systemHealth.disabled")
                    : p.status === "running"
                      ? t("systemHealth.running")
                      : t("systemHealth.unreachable")}
                </Group>
              </Table.Td>
              <Table.Td>{p.port != null ? t("systemHealth.port", { port: p.port }) : ""}</Table.Td>
            </Table.Tr>
          ))}
          <Table.Tr>
            <Table.Td>{t("systemHealth.mvRefreshLoop")}</Table.Td>
            <Table.Td>
              <Group gap={0} wrap="nowrap">
                <StatusDot ok={health.mvRefreshLoopRunning} />
                {health.mvRefreshLoopRunning ? t("systemHealth.running") : t("systemHealth.stopped")}
              </Group>
            </Table.Td>
            <Table.Td></Table.Td>
          </Table.Tr>
        </Table.Tbody>
      </Table>
    </Table.ScrollContainer>
  );
}
