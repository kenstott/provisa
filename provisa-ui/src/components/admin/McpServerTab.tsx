// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { useTranslation, Trans } from "react-i18next";
import { Alert, Badge, Group, Stack, Table, Text, Title } from "@mantine/core";
import { fetchMcpServer, type McpServerStatus } from "../../api/admin";

// REQ-1008: read-only status of the in-process MCP server. It is enabled purely via the
// PROVISA_MCP_PORT env var at boot, so this tab reports current state + how to enable it rather
// than offering a control that could not actually toggle the running server.
export function McpServerTab() {
  const { t } = useTranslation();
  const [status, setStatus] = useState<McpServerStatus | null>(null);
  const [error, setError] = useState("");

  useEffect(() => {
    fetchMcpServer()
      .then(setStatus)
      .catch((e) => setError(String(e)));
  }, []);

  if (error) return <Alert color="red">{error}</Alert>;
  if (!status) return <Text>{t("mcpServerTab.loading")}</Text>;

  return (
    <Stack gap="md" maw={720}>
      <Text c="dimmed">{t("mcpServerTab.intro")}</Text>

      <Stack gap="xs" p="sm" style={{ border: "1px solid var(--text-muted)", borderRadius: 4 }}>
        <Group gap="xs">
          <Text span>{t("mcpServerTab.statusLabel")}:</Text>
          <Badge
            data-testid="mcp-status"
            color={status.enabled ? "green" : "gray"}
            variant="light"
          >
            {status.enabled ? t("mcpServerTab.statusEnabled") : t("mcpServerTab.statusDisabled")}
          </Badge>
        </Group>

        {status.enabled ? (
          <>
            <Group gap="xs">
              <Text span>{t("mcpServerTab.endpointLabel")}:</Text>
              <Text span data-testid="mcp-endpoint" ff="monospace">
                http://0.0.0.0:{status.port}
              </Text>
            </Group>
            <Group gap="xs">
              <Text span>{t("mcpServerTab.transportLabel")}:</Text>
              <Text span ff="monospace">
                {status.transport}
              </Text>
            </Group>
            <Group gap="xs">
              <Text span>{t("mcpServerTab.stdioRoleLabel")}:</Text>
              {status.stdio_role ? (
                <Text span data-testid="mcp-role" ff="monospace">
                  {status.stdio_role}
                </Text>
              ) : (
                <Text span c="dimmed">
                  <Trans
                    i18nKey="mcpServerTab.stdioRoleNone"
                    values={{ envVar: status.role_env_var }}
                    components={{ code: <Text span ff="monospace" /> }}
                  />
                </Text>
              )}
            </Group>
            <Group gap="xs">
              <Text span>{t("mcpServerTab.maxRowsLabel")}:</Text>
              <Text span ff="monospace">
                {status.max_rows}
              </Text>
            </Group>
          </>
        ) : (
          <Alert data-testid="mcp-enable-hint" color="yellow">
            <Trans
              i18nKey="mcpServerTab.enableHint"
              values={{ enableEnvVar: status.enable_env_var, roleEnvVar: status.role_env_var }}
              components={{ code: <Text span ff="monospace" /> }}
            />
          </Alert>
        )}
      </Stack>

      <Title order={4}>{t("mcpServerTab.toolsHeading")}</Title>
      <Table.ScrollContainer minWidth={480}>
        <Table data-testid="mcp-tools" striped highlightOnHover withTableBorder verticalSpacing="xs">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("mcpServerTab.colTool")}</Table.Th>
              <Table.Th>{t("mcpServerTab.colDescription")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {status.tools.map((tool) => (
              <Table.Tr key={tool.name}>
                <Table.Td ff="monospace">{tool.name}</Table.Td>
                <Table.Td>{tool.description}</Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>
    </Stack>
  );
}
