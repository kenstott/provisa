// Copyright (c) 2026 Kenneth Stott
// Canary: 70400fde-cdd6-45e7-913f-ad8d63a0ad44
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-464: in a source with hundreds of tables, the steward describes what they are looking for
// ("customer invoicing and payment tables") and the source's schema is searched for it: a text
// filter, then a model's ranking with a confidence for each. It surfaces candidates and claims
// none — choosing one only fills in the table the register form is about to register.

import { useEffect, useRef, useState } from "react";
import { Badge, Button, Group, Stack, Table, Text, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { searchSourceTables } from "../../api/admin";
import type { TableSearchAnswer } from "../../api/admin";

// While a schema's column names are still loading, the same search is asked again until they
// are in: every few seconds, a bounded number of times.
export const COLUMNS_REASK_MS = 3000;
export const COLUMNS_REASK_LIMIT = 40;

interface Props {
  sourceId: string;
  schemaName: string;
  isRegistered: (table: { name: string }) => boolean;
  onPick: (tableName: string) => void;
}

export function NlTableSearch({ sourceId, schemaName, isRegistered, onPick }: Props) {
  const { t } = useTranslation();
  const [query, setQuery] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [answer, setAnswer] = useState<TableSearchAnswer | null>(null);
  // The query the current answer is for, and how often it has been re-asked.
  const asked = useRef<{ query: string; times: number } | null>(null);
  const found = answer ? answer.candidates : null;

  const search = async () => {
    setBusy(true);
    setError(null);
    asked.current = { query: query.trim(), times: 0 };
    try {
      setAnswer(await searchSourceTables(sourceId, query.trim(), schemaName));
    } catch (e) {
      setAnswer(null);
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  // REQ-464: column names still loading — a column searched for now may not be found yet. The
  // same search is asked again until they are loaded, so an early answer is not the last word.
  useEffect(() => {
    const current = asked.current;
    if (!answer || answer.column_names !== "loading" || !current) return;
    if (current.times >= COLUMNS_REASK_LIMIT) return;
    const timer = setTimeout(() => {
      current.times += 1;
      searchSourceTables(sourceId, current.query, schemaName)
        .then((next) => {
          if (asked.current === current) setAnswer(next);
        })
        .catch((e) => {
          if (asked.current === current) setError(e instanceof Error ? e.message : String(e));
        });
    }, COLUMNS_REASK_MS);
    return () => clearTimeout(timer);
  }, [answer, sourceId, schemaName]);

  return (
    <Stack gap="xs" style={{ gridColumn: "1 / -1" }} data-testid="register-table-nl-search">
      <Group gap="xs" align="flex-end" wrap="nowrap">
        <TextInput
          style={{ flex: 1 }}
          label={t("registerTableForm.nlSearchLabel")}
          placeholder={t("registerTableForm.nlSearchPlaceholder")}
          value={query}
          onChange={(e) => setQuery(e.currentTarget.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && query.trim()) {
              e.preventDefault();
              void search();
            }
          }}
          data-testid="register-table-nl-query"
        />
        <Button
          variant="default"
          onClick={() => void search()}
          loading={busy}
          disabled={!query.trim()}
          data-testid="register-table-nl-run"
        >
          {t("registerTableForm.nlSearchButton")}
        </Button>
      </Group>
      {error && (
        <Text size="xs" c="red">
          {error}
        </Text>
      )}
      {answer?.column_names === "loading" && (
        <Text size="xs" c="dimmed" data-testid="register-table-nl-columns-loading">
          {t("registerTableForm.nlSearchColumnsLoading")}
        </Text>
      )}
      {answer?.column_names === "unavailable" && (
        <Text size="xs" c="dimmed" data-testid="register-table-nl-columns-unavailable">
          {t("registerTableForm.nlSearchColumnsUnavailable")}
        </Text>
      )}
      {found !== null && found.length === 0 && (
        <Text size="xs" c="dimmed">
          {t("registerTableForm.nlSearchNone")}
        </Text>
      )}
      {found !== null && found.length > 0 && (
        <Table withTableBorder verticalSpacing={4} fz="xs" data-testid="register-table-nl-results">
          <Table.Tbody>
            {found.map((c) => {
              const registered = isRegistered({ name: c.table_name });
              return (
                <Table.Tr key={`${c.schema_name}.${c.table_name}`}>
                  <Table.Td>
                    <Text fz="xs" fw={600}>
                      {c.table_name}
                    </Text>
                    {(c.reasoning || c.comment) && (
                      <Text fz="xs" c="dimmed">
                        {c.reasoning || c.comment}
                      </Text>
                    )}
                  </Table.Td>
                  <Table.Td w={90}>
                    <Badge variant="light" size="sm">
                      {t("registerTableForm.nlSearchConfidence", {
                        percent: Math.round(c.confidence * 100),
                      })}
                    </Badge>
                  </Table.Td>
                  <Table.Td w={110}>
                    {registered ? (
                      <Text fz="xs" c="dimmed">
                        {t("registerTableForm.nlSearchRegistered")}
                      </Text>
                    ) : (
                      <Button size="compact-xs" variant="light" onClick={() => onPick(c.table_name)}>
                        {t("registerTableForm.nlSearchUse")}
                      </Button>
                    )}
                  </Table.Td>
                </Table.Tr>
              );
            })}
          </Table.Tbody>
        </Table>
      )}
    </Stack>
  );
}
