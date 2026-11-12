// Copyright (c) 2026 Kenneth Stott
// Canary: 31c7982c-74a4-41a6-b79d-aa6f125313ca
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2240: the bar shown once rows of a list are selected (`useListSelection`).

import { useState } from "react";
import { Button, Group, List, Text } from "@mantine/core";
import { Trash2, X } from "lucide-react";
import { useTranslation } from "react-i18next";
import { ConfirmDialog } from "../ConfirmDialog";
import { DependentsDialog } from "../DependentsDialog";
import type { BulkDeleteRefusal, BulkDeleteResult, ListSelection } from "./listSelection";

export interface ListBulkBarProps {
  selection: ListSelection;
  /** A selected row's name, as the list shows it. */
  nameOf: (key: string) => string;
  /** The page's one bulk call: it resolves once the list is read again without what went. */
  onDelete: (keys: string[]) => Promise<BulkDeleteResult>;
  testId?: string;
}

const reasonOf = (error: unknown) =>
  error instanceof Error ? error.message : typeof error === "string" ? error : String(error);

/**
 * The bar shown once rows are selected: the count, Delete and Clear. Delete asks once, naming the
 * rows; afterwards each row refused is named with its reason, and the rows still referred to are
 * shown with what refers to them, one at a time.
 */
export function ListBulkBar({
  selection,
  nameOf,
  onDelete,
  testId = "list-bulk",
}: ListBulkBarProps) {
  const { t } = useTranslation();
  const [refused, setRefused] = useState<BulkDeleteRefusal[]>([]);
  const [failed, setFailed] = useState<string | null>(null);
  // The refusals over dependents, shown in turn: the first is open.
  const [toShow, setToShow] = useState<BulkDeleteRefusal[]>([]);
  const count = selection.selected.size;
  const keys = selection.shown.filter((k) => selection.selected.has(k));
  const names = keys.map(nameOf);

  const remove = async () => {
    setRefused([]);
    setFailed(null);
    selection.markDeleting(new Set(keys));
    try {
      const result = await onDelete(keys);
      selection.clear();
      setRefused(result.refused);
      setToShow(result.refused.filter((r) => r.dependents.length > 0));
    } catch (error) {
      setFailed(t("common.deleteFailed", { reason: reasonOf(error) }));
    } finally {
      selection.markDeleting(new Set());
    }
  };

  const shownRefusal = toShow[0];
  return (
    <>
      {count > 0 && (
        <Group gap="sm" className="list-bulk-bar" data-testid={testId} role="toolbar">
          <Text fz="sm" fw={600} data-testid={`${testId}-count`}>
            {t("list.selectedCount", { count })}
          </Text>
          <ConfirmDialog
            title={t("list.bulkDeleteTitle", { count })}
            consequence={t("list.bulkDeleteConsequence", { names: names.join(", ") })}
            onConfirm={remove}
            confirmLabel={t("list.bulkDelete")}
            testId={`${testId}-confirm-dialog`}
          >
            {(open) => (
              <Button
                size="xs"
                color="red"
                variant="light"
                leftSection={<Trash2 size={14} />}
                onClick={open}
                disabled={selection.deleting.size > 0}
                data-testid={`${testId}-delete`}
                // REQ-2115: what Polly presses here changes something; she asks the user first.
                data-changes="true"
              >
                {t("list.bulkDelete")}
              </Button>
            )}
          </ConfirmDialog>
          <Button
            size="xs"
            variant="subtle"
            leftSection={<X size={14} />}
            onClick={selection.clear}
            disabled={selection.deleting.size > 0}
            data-testid={`${testId}-clear`}
          >
            {t("list.clearSelection")}
          </Button>
        </Group>
      )}
      {failed !== null && (
        <div className="form-problem" role="alert" data-testid={`${testId}-problem`}>
          {failed}
        </div>
      )}
      {refused.length > 0 && (
        <div className="form-problem" role="alert" data-testid={`${testId}-refused`}>
          <Group justify="space-between" wrap="nowrap" align="flex-start">
            <div>
              <Text fz="sm" fw={600}>
                {t("list.bulkRefused", { count: refused.length })}
              </Text>
              <List size="sm">
                {refused.map((r) => (
                  <List.Item key={r.name} data-testid={`${testId}-refused-row`}>
                    <strong>{r.name}</strong>: {r.reason}
                  </List.Item>
                ))}
              </List>
            </div>
            <Button
              size="xs"
              variant="subtle"
              onClick={() => setRefused([])}
              data-testid={`${testId}-refused-dismiss`}
            >
              {t("list.dismiss")}
            </Button>
          </Group>
        </div>
      )}
      {shownRefusal && (
        <DependentsDialog
          subject={shownRefusal.name}
          dependents={shownRefusal.dependents}
          onClose={() => setToShow((rest) => rest.slice(1))}
        />
      )}
    </>
  );
}
