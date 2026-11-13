// Copyright (c) 2026 Kenneth Stott
// Canary: 7f0a3c52-6d1e-4b88-a9d4-2e51c0b7a8f3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useTranslation } from "react-i18next";
import { Badge } from "@mantine/core";
import { Tip } from "../Tip";

/**
 * "From FK", beside the name of a relationship created from a foreign key its source declares.
 * The caller draws it only when that is so (`Relationship.autoSuggested`, a derived fact the
 * server returns), never as a Yes/No.
 */
export function FkBadge() {
  const { t } = useTranslation();
  return (
    <Tip text={t("relationshipRow.fkBadgeTitle")}>
      <Badge size="xs" color="gray" variant="filled" style={{ flexShrink: 0 }}>
        {t("relationshipRow.fkBadge")}
      </Badge>
    </Tip>
  );
}
