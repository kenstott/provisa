// Copyright (c) 2026 Kenneth Stott
// Canary: fac40a71-baf2-42dc-9b90-a9d9b4e90732
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A detail panel's Duplicate, beside its Edit and Delete in the panel's head: the one way to
// make an object from another. "+ Object" opens an empty create form; this opens the CREATE
// form prefilled from the object shown, under a new identity, and nothing that depends on the
// object goes with it (the rule: panelDuplicateGuard.test.ts). The page shows it to a caller who
// may create that kind of object and says what the prefilled form holds (`onDuplicate`).
//
// An icon, named "Duplicate", its tip naming the object. It takes the head's one style
// (PanelHeadStyle): no variant or size of its own.

import { ActionIcon } from "@mantine/core";
import { Copy } from "lucide-react";
import { useTranslation } from "react-i18next";
import { Tip } from "../Tip";

export interface PanelDuplicateProps {
  /** The object's name, as its panel's head shows it: the tip names it. */
  name: string;
  /** Opens the create form prefilled from the object. */
  onDuplicate: () => void;
  testId?: string;
  disabled?: boolean;
}

export function PanelDuplicate({ name, onDuplicate, testId, disabled }: PanelDuplicateProps) {
  const { t } = useTranslation();
  return (
    <Tip text={t("list.duplicateNamed", { name })}>
      <ActionIcon
        // Borderless, as the panel head's one style (PanelHeadStyle) draws it, and the same where
        // that style is not applied (a detail beneath its row).
        variant="subtle"
        aria-label={t("list.duplicate")}
        data-testid={testId}
        disabled={disabled}
        onClick={(e) => {
          e.stopPropagation();
          onDuplicate();
        }}
      >
        <Copy size={14} />
      </ActionIcon>
    </Tip>
  );
}
