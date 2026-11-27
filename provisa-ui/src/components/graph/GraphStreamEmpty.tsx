// Copyright (c) 2026 Kenneth Stott
// Canary: 6c3e8a14-2d7f-4b91-a05e-9f1b4c7d2e38
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// What the graph page's results show before its first query: how to start.

import { useTranslation } from "react-i18next";
import { Text } from "@mantine/core";

export function GraphStreamEmpty({ sparql }: { sparql: boolean }) {
  const { t } = useTranslation();
  return (
    <div className="graph-stream-empty" role="status" data-testid="graph-empty-state">
      <div className="graph-stream-empty-icon" aria-hidden="true">
        ⬡
      </div>
      <Text component="div">
        {t(sparql ? "graphPage.emptyStateHintSparql" : "graphPage.emptyStateHint")}
      </Text>
      <Text component="div" className="graph-stream-hint">
        {t("graphPage.emptyStateShortcut")}
      </Text>
    </div>
  );
}
