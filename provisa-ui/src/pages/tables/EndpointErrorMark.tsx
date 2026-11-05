// Copyright (c) 2026 Kenneth Stott
// Canary: fd6b7a0d-7f81-46db-bda7-9f9024976182
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A table of an OpenAPI source whose document could not be read says so in the table list's row:
// a warning mark, the reason on hover. The listing lists the table all the same.

import { TriangleAlert } from "lucide-react";
import { useTranslation } from "react-i18next";
import { Tip } from "../../components/Tip";

export function EndpointErrorMark({ error, testId }: { error: string | null; testId: string }) {
  const { t } = useTranslation();
  if (!error) return null;
  return (
    <Tip text={error}>
      <span
        role="img"
        aria-label={t("tablesPage.endpointUnread")}
        data-testid={testId}
        style={{ display: "inline-flex", color: "var(--warning, #d19a00)" }}
      >
        <TriangleAlert size={14} />
      </span>
    </Tip>
  );
}
