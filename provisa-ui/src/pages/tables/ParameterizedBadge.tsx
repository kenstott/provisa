// Copyright (c) 2026 Kenneth Stott
// Canary: 5b72b0d7-ebe9-46fd-8971-2a43a15386cb
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1986: a parameterized table is marked in the Tables list after its name: PARAMETERIZED,
// or NEEDS PARAMS in the warning colour when a read must give any of its parameters.

import { Badge } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { isRequiredParam } from "../../components/nativeParams";
import { parameterName } from "./parameters";

interface ParameterColumn {
  columnName: string;
  nativeFilterType: string | null;
  nativeFilterRequired?: boolean | null;
  requiredBySteward?: boolean;
}

export function ParameterizedBadge({
  tableName,
  columns,
}: {
  tableName: string;
  columns: ParameterColumn[];
}) {
  const { t } = useTranslation();
  const parameters = columns.filter((c) => c.nativeFilterType);
  if (parameters.length === 0) return null;
  const required = parameters.filter(isRequiredParam).map((c) => parameterName(c.columnName));
  const names = parameters.map((c) => parameterName(c.columnName)).join(", ");
  return required.length > 0 ? (
    <Badge
      size="xs"
      variant="light"
      color="yellow"
      title={t("parametersPanel.needsBadgeTitle", { parameters: required.join(", ") })}
      data-testid={`tables-needs-params-${tableName}`}
    >
      {t("parametersPanel.needsBadge")}
    </Badge>
  ) : (
    <Badge
      size="xs"
      variant="light"
      color="blue"
      title={t("parametersPanel.badgeTitle", { parameters: names })}
      data-testid={`tables-parameterized-${tableName}`}
    >
      {t("parametersPanel.badge")}
    </Badge>
  );
}
