// Copyright (c) 2026 Kenneth Stott
// Canary: 8b3e1c57-0f42-4d96-a7e8-2c9d5b6f1a03
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2122: a catalog's Import. For every table of the catalog's domains linked to it, what the
// linked asset holds differently from Provisa's values, beside them; the steward accepts all or
// table by table. A value edited in Provisa since it was last imported is marked so. A table not
// linked is listed with the two ways to link it, and nothing is guessed for it.

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Anchor,
  Badge,
  Button,
  Group,
  Loader,
  Modal,
  Paper,
  Stack,
  Table,
  Text,
} from "@mantine/core";
import { Check, ExternalLink, TriangleAlert } from "lucide-react";
import { acceptImport, fetchImportReview, type ImportReview } from "../../api/catalogs";

export function CatalogImportModal({
  catalog,
  onClose,
}: {
  catalog: string | null;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  return (
    <Modal
      opened={catalog !== null}
      onClose={onClose}
      size="xl"
      title={t("catalogImport.title", { catalog })}
      data-testid="catalog-import"
    >
      {/* Remounted for each catalog, so a review always starts from that catalog's state. */}
      {catalog !== null && <ImportReviewBody key={catalog} catalog={catalog} />}
    </Modal>
  );
}

function ImportReviewBody({ catalog }: { catalog: string }) {
  const { t } = useTranslation();
  const [review, setReview] = useState<ImportReview | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState<string>("");
  const [said, setSaid] = useState("");

  const load = useCallback(
    () =>
      fetchImportReview(catalog)
        .then((found) => {
          setError("");
          setReview(found);
        })
        .catch((e) => setError(String(e))),
    [catalog],
  );
  useEffect(() => {
    void load();
  }, [load]);

  const accept = async (tableIds: number[], key: string) => {
    setBusy(key);
    setError("");
    try {
      await acceptImport(catalog, tableIds);
      setSaid(t("catalogImport.accepted", { count: tableIds.length }));
      await load();
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy("");
    }
  };

  const changed = (review?.linked ?? []).filter((x) => (x.changes ?? []).length > 0);
  const fieldLabel = (field: string, column: string | null) =>
    column
      ? t(`catalogImport.field_${field}_column`, { column })
      : t(`catalogImport.field_${field}`);

  return (
    <Stack gap="md">
      {error && (
        <Alert color="red" icon={<TriangleAlert size={16} />} data-testid="catalog-import-error">
          {error}
        </Alert>
      )}
      {said && (
        <Alert color="green" icon={<Check size={16} />} data-testid="catalog-import-said">
          {said}
        </Alert>
      )}
      {!review && !error && (
        <Group gap="xs">
          <Loader size="sm" />
          <Text>{t("catalogImport.reading")}</Text>
        </Group>
      )}
      {review && (
        <>
          <Group justify="space-between">
            <Text size="sm">
              {changed.length
                ? t("catalogImport.changedCount", { count: changed.length })
                : t("catalogImport.nothingChanged")}
            </Text>
            {changed.length > 0 && (
              <Button
                onClick={() =>
                  accept(
                    changed.map((x) => x.table_id),
                    "all",
                  )
                }
                loading={busy === "all"}
                data-testid="catalog-import-accept-all"
              >
                {t("catalogImport.acceptAll")}
              </Button>
            )}
          </Group>
          {review.linked.map((x) => (
            <Paper
              key={x.table_id}
              withBorder
              p="sm"
              data-testid={`catalog-import-table-${x.table_id}`}
            >
              <Group justify="space-between" mb="xs" wrap="nowrap">
                <Group gap="xs">
                  <Text size="sm" fw={600}>
                    {x.table}
                  </Text>
                  <Text size="xs" c="dimmed">
                    {x.asset}
                  </Text>
                  {x.locked && (
                    <Badge size="xs" variant="light">
                      {t("catalogImport.locked")}
                    </Badge>
                  )}
                  {x.url && (
                    <Anchor href={x.url} target="_blank" rel="noreferrer" size="xs">
                      <ExternalLink size={12} />
                    </Anchor>
                  )}
                </Group>
                {(x.changes ?? []).length > 0 && (
                  <Button
                    variant="default"
                    size="compact-sm"
                    onClick={() => accept([x.table_id], String(x.table_id))}
                    loading={busy === String(x.table_id)}
                    data-testid={`catalog-import-accept-${x.table_id}`}
                  >
                    {t("catalogImport.accept")}
                  </Button>
                )}
              </Group>
              {x.error ? (
                <Text size="sm" c="red">
                  {x.error}
                </Text>
              ) : (x.changes ?? []).length === 0 ? (
                <Text size="xs" c="dimmed">
                  {t("catalogImport.inStep")}
                </Text>
              ) : (
                <Table withTableBorder layout="fixed">
                  <Table.Thead>
                    <Table.Tr>
                      <Table.Th w="25%">{t("catalogImport.field")}</Table.Th>
                      <Table.Th>{t("catalogImport.inProvisa")}</Table.Th>
                      <Table.Th>{t("catalogImport.inCatalog", { catalog })}</Table.Th>
                    </Table.Tr>
                  </Table.Thead>
                  <Table.Tbody>
                    {(x.changes ?? []).map((c) => (
                      <Table.Tr key={`${c.field}|${c.column ?? ""}|${c.catalog}`}>
                        <Table.Td>
                          <Stack gap={2}>
                            <Text size="sm">{fieldLabel(c.field, c.column)}</Text>
                            {c.local_edit && (
                              <Text size="xs" c="orange">
                                {t("catalogImport.localEdit")}
                              </Text>
                            )}
                          </Stack>
                        </Table.Td>
                        <Table.Td style={{ whiteSpace: "pre-wrap" }}>{c.current}</Table.Td>
                        <Table.Td style={{ whiteSpace: "pre-wrap" }}>{c.catalog}</Table.Td>
                      </Table.Tr>
                    ))}
                  </Table.Tbody>
                </Table>
              )}
            </Paper>
          ))}
          {review.not_linked.length > 0 && (
            <Stack gap={4} data-testid="catalog-import-not-linked">
              <Text size="sm" fw={600}>
                {t("catalogImport.notLinkedTitle")}
              </Text>
              <Text size="xs" c="dimmed">
                {t("catalogImport.notLinkedHelp", { catalog })}
              </Text>
              <Text size="sm">{review.not_linked.map((x) => x.table).join(", ")}</Text>
            </Stack>
          )}
        </>
      )}
    </Stack>
  );
}
