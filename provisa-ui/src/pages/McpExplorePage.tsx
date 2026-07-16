// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Accordion,
  Alert,
  Badge,
  Button,
  Group,
  NumberInput,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { Search } from "lucide-react";
import { searchCatalog, type CatalogSearchHit } from "../api/admin";
import { useAuth } from "../context/AuthContext";

// REQ-1008: the MCP "explore" surface as an interactive page — natural-language semantic
// catalog search, governed by the active role, resolving hits to authoritative table branches.
export function McpExplorePage() {
  const { t } = useTranslation();
  const { role } = useAuth();
  const roleId = role?.id ?? "";
  const [query, setQuery] = useState("");
  const [k, setK] = useState(5);
  const [hits, setHits] = useState<CatalogSearchHit[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const run = async () => {
    if (!query.trim()) return;
    setLoading(true);
    setError("");
    try {
      setHits(await searchCatalog(query.trim(), roleId, k));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setHits(null);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="page">
      <Title order={2} mb="xs">
        {t("mcpExplore.title")}
      </Title>
      <Text c="dimmed" size="sm" mb="md">
        {t("mcpExplore.intro")}
      </Text>

      <Group align="flex-end" gap="sm" mb="md">
        <TextInput
          style={{ flex: 1 }}
          label={t("mcpExplore.queryLabel")}
          placeholder={t("mcpExplore.queryPlaceholder")}
          value={query}
          onChange={(e) => setQuery(e.currentTarget.value)}
          onKeyDown={(e) => e.key === "Enter" && run()}
          data-testid="mcp-explore-query"
        />
        <NumberInput
          label={t("mcpExplore.resultsLabel")}
          min={1}
          max={25}
          w={110}
          value={k}
          onChange={(v) => setK(typeof v === "number" ? v : 5)}
        />
        <Button
          onClick={run}
          loading={loading}
          disabled={!query.trim()}
          leftSection={<Search size={14} />}
          data-testid="mcp-explore-search"
        >
          {t("mcpExplore.search")}
        </Button>
      </Group>

      <Text size="xs" c="dimmed" mb="md">
        {t("mcpExplore.roleNote", { role: roleId || t("mcpExplore.noRole") })}
      </Text>

      {error && (
        <Alert color="red" mb="md">
          {error}
        </Alert>
      )}

      {hits !== null && hits.length === 0 && !error && (
        <Text c="dimmed">{t("mcpExplore.noResults")}</Text>
      )}

      {hits && hits.length > 0 && (
        <Accordion variant="separated" multiple>
          {hits.map((h) => (
            <Accordion.Item key={`${h.schema}.${h.table}`} value={`${h.schema}.${h.table}`}>
              <Accordion.Control>
                <Group gap="sm" wrap="nowrap">
                  <Text fw={600} ff="monospace">
                    {h.breadcrumb}
                  </Text>
                  <Badge variant="light" color="teal">
                    {t("mcpExplore.scoreBadge", { score: h.score.toFixed(3) })}
                  </Badge>
                  {h.matched_on.column && (
                    <Badge variant="light" color="grape">
                      {t("mcpExplore.matchedColumn", { column: h.matched_on.column })}
                    </Badge>
                  )}
                </Group>
              </Accordion.Control>
              <Accordion.Panel>
                <Stack gap="sm">
                  {h.branch.description && (
                    <Text size="sm" c="dimmed">
                      {h.branch.description}
                    </Text>
                  )}
                  <Table striped withTableBorder verticalSpacing="xs">
                    <Table.Thead>
                      <Table.Tr>
                        <Table.Th>{t("mcpExplore.colColumn")}</Table.Th>
                        <Table.Th>{t("mcpExplore.colType")}</Table.Th>
                        <Table.Th>{t("mcpExplore.colDescription")}</Table.Th>
                      </Table.Tr>
                    </Table.Thead>
                    <Table.Tbody>
                      {h.branch.columns.map((c) => (
                        <Table.Tr key={c.name}>
                          <Table.Td ff="monospace">{c.name}</Table.Td>
                          <Table.Td c="dimmed">{c.type}</Table.Td>
                          <Table.Td>{c.description || "—"}</Table.Td>
                        </Table.Tr>
                      ))}
                    </Table.Tbody>
                  </Table>
                  {h.branch.foreign_keys.length > 0 && (
                    <Stack gap={2}>
                      <Text size="xs" fw={600}>
                        {t("mcpExplore.foreignKeys")}
                      </Text>
                      {h.branch.foreign_keys.map((fk, i) => (
                        <Text key={i} size="xs" ff="monospace" c="dimmed">
                          {fk.column} → {fk.references_schema}.{fk.references_table}.
                          {fk.references_column}
                        </Text>
                      ))}
                    </Stack>
                  )}
                </Stack>
              </Accordion.Panel>
            </Accordion.Item>
          ))}
        </Accordion>
      )}
    </div>
  );
}
