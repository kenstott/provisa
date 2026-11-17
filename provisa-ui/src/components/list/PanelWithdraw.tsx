// Copyright (c) 2026 Kenneth Stott
// Canary: 6b1e9d42-7c3a-4f85-b0d6-2e8a5c71f934
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1921: a detail panel's Withdraw, and once withdrawn its Put back. Withdraw takes the object
// out of production, so the reader confirms it, told what stops; Put back returns it as it was and
// asks nothing. While either is pending the control shows a spinner; one that failed says why,
// where the panel's Delete says it (under the panel's head).

import { useContext, useState } from "react";
import { createPortal } from "react-dom";
import { ActionIcon, Loader } from "@mantine/core";
import { Archive, ArchiveRestore } from "lucide-react";
import { useTranslation } from "react-i18next";
import { ConfirmDialog } from "../ConfirmDialog";
import { Tip } from "../Tip";
import { PanelNoticeContext } from "./panelActions";

export interface PanelWithdrawProps {
  /** The object's name, as its panel's head shows it. */
  name: string;
  /** Whether it is withdrawn now: the control is then Put back. */
  withdrawn: boolean;
  /** What withdrawing stops, said in the confirmation. */
  consequence: string;
  /** Each resolves once done and the lists read again; each rejects with the refusal. */
  onWithdraw: () => Promise<void>;
  onPutBack: () => Promise<void>;
  testId: string;
}

export function PanelWithdraw({
  name,
  withdrawn,
  consequence,
  onWithdraw,
  onPutBack,
  testId,
}: PanelWithdrawProps) {
  const { t } = useTranslation();
  const notice = useContext(PanelNoticeContext);
  const [busy, setBusy] = useState(false);
  const [problem, setProblem] = useState<string | null>(null);

  const run = async (action: () => Promise<void>) => {
    setProblem(null);
    setBusy(true);
    try {
      await action();
    } catch (error) {
      setProblem(error instanceof Error ? error.message : String(error));
    } finally {
      setBusy(false);
    }
  };

  const said = problem ? (
    <div className="form-problem" role="alert" data-testid={`${testId}-problem`}>
      {problem}
    </div>
  ) : null;
  const spinner = <Loader size={14} color="currentColor" />;

  return (
    <>
      {withdrawn ? (
        <Tip text={t("list.putBackNamed", { name })}>
          <ActionIcon
            variant="subtle"
            aria-label={t("list.putBack")}
            data-testid={`${testId}-put-back`}
            disabled={busy}
            aria-busy={busy || undefined}
            // REQ-2115: what Polly presses here changes something; she asks the user first.
            data-changes="true"
            onClick={(e) => {
              e.stopPropagation();
              void run(onPutBack);
            }}
          >
            {busy ? spinner : <ArchiveRestore size={14} />}
          </ActionIcon>
        </Tip>
      ) : (
        <ConfirmDialog
          title={t("list.withdrawTitle", { name })}
          consequence={consequence}
          onConfirm={() => run(onWithdraw)}
          confirmLabel={t("list.withdraw")}
          testId={`${testId}-confirm-dialog`}
        >
          {(open) => (
            <Tip text={t("list.withdrawNamed", { name })}>
              <ActionIcon
                variant="subtle"
                aria-label={t("list.withdraw")}
                data-testid={`${testId}-withdraw`}
                disabled={busy}
                aria-busy={busy || undefined}
                data-changes="true"
                onClick={(e) => {
                  e.stopPropagation();
                  open();
                }}
              >
                {busy ? spinner : <Archive size={14} />}
              </ActionIcon>
            </Tip>
          )}
        </ConfirmDialog>
      )}
      {said !== null && (notice ? createPortal(said, notice) : notice === undefined ? said : null)}
    </>
  );
}
