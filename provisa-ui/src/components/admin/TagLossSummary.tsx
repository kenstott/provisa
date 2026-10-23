// Copyright (c) 2026 Kenneth Stott
// Canary: 0e7b3c56-9a14-4d28-b6f1-c85e2d47a913
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A move a person makes that takes tags off columns (REQ-2145, 409 tag.move_loses): one summary of
// what each column loses and gains, the losses first and marked. The reader moves anyway (each lost
// tag confirmed), moves and marks the term the columns move to with the lost tags, or goes back.
// Retire, Claim columns and Move to term, and a relationship's save or delete all ask it.

import { useTranslation } from "react-i18next";
import { Button, Stack, Text } from "@mantine/core";
import type { ColumnTagChange } from "../../api/glossary";
import { FormFooter } from "../form/FormLayout";
import { changed, lostTags } from "./tagLoss";

export function TagLossSummary({
  columns,
  into,
  saving,
  onMove,
  onCarry,
  onCancel,
}: {
  columns: ColumnTagChange[];
  /** The term the columns move to, where one term takes them all. */
  into: string | null;
  saving: boolean;
  onMove: (confirmTagLoss: string[]) => void;
  onCarry: () => void;
  onCancel: () => void;
}) {
  const { t } = useTranslation();
  const lost = lostTags(columns);
  return (
    <Stack gap="sm" data-testid="glossary-tag-loss">
      {changed(columns).map((c) => (
        <Stack gap={0} key={`${c.table}.${c.column}`}>
          <Text size="sm" fw={600}>
            {c.table}.{c.column}
          </Text>
          <Text size="sm">
            {/* What it loses first, marked: that is what the reader is asked to accept. */}
            {c.loses.length > 0 && (
              <Text span c="red" fw={600}>
                {t("glossaryTab.loses", { tags: c.loses.join(", ") })}
              </Text>
            )}
            {c.loses.length > 0 && c.gains.length > 0 && " · "}
            {c.gains.length > 0 && t("glossaryTab.gains", { tags: c.gains.join(", ") })}
          </Text>
        </Stack>
      ))}
      {/* Back: nothing is moved. */}
      <FormFooter
        onCancel={onCancel}
        cancelProps={{ "data-testid": "glossary-tag-loss-cancel" }}
        action={
          <>
            <Button
              color="orange"
              onClick={() => onMove(lost)}
              loading={saving}
              data-testid="glossary-tag-loss-move"
            >
              {t("glossaryTab.moveConfirm")}
            </Button>
            <Button
              variant="default"
              onClick={onCarry}
              loading={saving}
              data-testid="glossary-tag-loss-carry"
            >
              {into === null
                ? t("glossaryTab.moveCarryUnnamed", { tags: lost.join(", ") })
                : t("glossaryTab.moveCarry", { term: into, tags: lost.join(", ") })}
            </Button>
          </>
        }
      />
    </Stack>
  );
}
