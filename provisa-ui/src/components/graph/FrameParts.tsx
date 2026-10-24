// Copyright (c) 2026 Kenneth Stott
// Canary: cd8fab61-dfcc-418b-8893-8d7796844e95
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Parts of a Cypher page frame: the run in progress, and the tabs of a query holding several
// statements, one per statement, the one chosen shown.

import { useTranslation } from "react-i18next";
import { Loader } from "@mantine/core";
import { StatementTabs } from "../StatementTabs";
import { CopySymbolButton } from "../CopyButton";
import { statementName } from "./frameStatements";
import type { FrameData } from "./graph-model";

export function FrameRunning() {
  const { t } = useTranslation();
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        padding: "2rem",
        gap: "0.5rem",
        color: "var(--text-muted)",
        fontSize: "0.85rem",
      }}
    >
      <Loader size="xs" />
      {t("graphFrame.running")}
    </div>
  );
}

export function FrameStatementTabs({
  frame,
  onStatement,
}: {
  frame: FrameData;
  onStatement: (id: string, index: number) => void;
}) {
  if (!frame.statements) return null;
  return (
    <StatementTabs
      tabs={frame.statements.map((st) => ({
        name: statementName(st.query),
        rows: st.redirect ? null : st.rows.length,
        failed: st.status === "error",
      }))}
      index={frame.statementIndex ?? 0}
      onChange={(i) => onStatement(frame.id, i)}
      testId="graph-statement-tab"
    />
  );
}

/** A refused statement: its reason, each access violation listed under it, copyable whole. */
export function FrameError({ frame }: { frame: FrameData }) {
  const { t } = useTranslation();
  return (
    <div className="gf-error gf-error--copyable">
      <div className="gf-error-text">
        <span data-testid="graph-frame-error">{frame.error}</span>
        {frame.violations && (
          <ul className="gf-error-violations" data-testid="graph-frame-violations">
            {frame.violations.map((v, i) => (
              <li key={i}>{v}</li>
            ))}
          </ul>
        )}
      </div>
      <CopySymbolButton
        text={[frame.error ?? "", ...(frame.violations ?? [])].join("\n")}
        className="gf-error-copy-btn"
        title={t("graphFrame.copyErrorTitle")}
      />
    </div>
  );
}
