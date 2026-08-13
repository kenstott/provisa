// Copyright (c) 2026 Kenneth Stott
// Canary: 4872ad47-31f1-494c-aa82-3771ffaac51f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Divider, Group, Loader, Modal, Stack, Table, Text } from "@mantine/core";
import { runSql } from "../../api/admin";
import { formatTelemetryValue, traceDetailSql, type TelemetryIdKind } from "./traceDetails";

interface TraceDetailsModalProps {
  kind: TelemetryIdKind;
  id: string;
  onClose: () => void;
}

/** Every column of every matching span, attribute maps included. */
export function TraceDetailsModal({ kind, id, onClose }: TraceDetailsModalProps) {
  const { t } = useTranslation();
  // The result carries the id it was fetched for, so a switch to another id
  // retires the previous rows by comparison at render time instead of by an
  // extra render pass that clears state at the top of the effect.
  const [loaded, setLoaded] = useState<{
    key: string;
    rows: Record<string, unknown>[];
    error: string | null;
  } | null>(null);
  const key = `${kind}:${id}`;
  useEffect(() => {
    let cancelled = false;
    runSql(traceDetailSql(kind, id))
      .then((r) => {
        if (!cancelled) setLoaded({ key: `${kind}:${id}`, rows: r.rows, error: r.error ?? null });
      })
      .catch((err: Error) => {
        if (!cancelled) setLoaded({ key: `${kind}:${id}`, rows: [], error: err.message });
      });
    return () => {
      cancelled = true;
    };
  }, [kind, id]);
  const current = loaded?.key === key ? loaded : null;

  return (
    <Modal
      opened
      onClose={onClose}
      size="xl"
      title={t(kind === "trace" ? "traceDetails.traceTitle" : "traceDetails.spanTitle", { id })}
    >
      {current === null && (
        <Group gap="xs">
          <Loader size="sm" />
          <Text size="sm">{t("traceDetails.loading")}</Text>
        </Group>
      )}
      {current?.error != null && (
        <Text size="sm" c="red">
          {current.error}
        </Text>
      )}
      {current !== null && current.error === null && current.rows.length === 0 && (
        <Text size="sm">{t("traceDetails.notFound")}</Text>
      )}
      <Stack gap="md">
        {current?.error == null &&
          current?.rows.map((row, i) => (
            <div key={i}>
              {current.rows.length > 1 && (
                <Divider
                  my="xs"
                  label={t("traceDetails.spanIndex", { index: i + 1, total: current.rows.length })}
                  labelPosition="left"
                />
              )}
              <Table
                withTableBorder
                withRowBorders
                striped
                horizontalSpacing="xs"
                verticalSpacing={2}
                data-testid={`trace-detail-span-${i}`}
              >
                <Table.Tbody>
                  {Object.entries(row).map(([col, value]) => (
                    <Table.Tr key={col}>
                      <Table.Td style={{ width: "22%", verticalAlign: "top", fontWeight: 600 }}>
                        {col}
                      </Table.Td>
                      <Table.Td>
                        <pre
                          style={{
                            margin: 0,
                            fontSize: "0.75rem",
                            whiteSpace: "pre-wrap",
                            wordBreak: "break-word",
                          }}
                        >
                          {formatTelemetryValue(value)}
                        </pre>
                      </Table.Td>
                    </Table.Tr>
                  ))}
                </Table.Tbody>
              </Table>
            </div>
          ))}
      </Stack>
    </Modal>
  );
}
