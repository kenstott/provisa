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
// out of production, so the reader confirms it, told what stops. Put back names the region the
// object returns to, among those the reader's data_residency grant covers (the second step of a
// move between regions); where the platform declares no regions it asks nothing. While either is
// pending the control shows a spinner; one that failed says why, where the panel's Delete says it
// (under the panel's head).

import { useContext, useState } from "react";
import { createPortal } from "react-dom";
import { ActionIcon, Button, Loader, Modal, Text } from "@mantine/core";
import { Archive, ArchiveRestore } from "lucide-react";
import { useTranslation } from "react-i18next";
import { ConfirmDialog } from "../ConfirmDialog";
import { Tip } from "../Tip";
import { PanelNoticeContext } from "./panelActions";
import { RegionSelect } from "../admin/RegionSelect";
import { FormField, FormFooter, FormLayout } from "../form/FormLayout";
import type { PutBackRegions } from "../../hooks/useWithdrawal";

export interface PanelWithdrawProps {
  /** The object's name, as its panel's head shows it. */
  name: string;
  /** Whether it is withdrawn now: the control is then Put back. */
  withdrawn: boolean;
  /** What withdrawing stops, said in the confirmation. */
  consequence: string;
  /** The region it is in now: where Put back offers to return it, when the grant covers it. */
  region: string | null;
  /** Where it may be put back; null where the platform declares no regions. */
  putBackRegions: PutBackRegions | null;
  /** Which object this is, for the region field's help. */
  scope: "table" | "view" | "source";
  /** Each resolves once done and the lists read again; each rejects with the refusal. */
  onWithdraw: () => Promise<void>;
  onPutBack: (region: string | null) => Promise<void>;
  testId: string;
}

/** The region a put back starts on: where the object is, when the grant covers it, else the
 * first region the grant covers, else no region when that is covered; undefined when none is. */
function startRegion(region: string | null, can: PutBackRegions): string | null | undefined {
  if (region !== null ? can.regions.includes(region) : can.noRegion) return region;
  if (can.regions.length > 0) return can.regions[0];
  return can.noRegion ? null : undefined;
}

export function PanelWithdraw({
  name,
  withdrawn,
  consequence,
  region,
  putBackRegions,
  scope,
  onWithdraw,
  onPutBack,
  testId,
}: PanelWithdrawProps) {
  const { t } = useTranslation();
  const notice = useContext(PanelNoticeContext);
  const [busy, setBusy] = useState(false);
  const [problem, setProblem] = useState<string | null>(null);
  // The put back's region dialog: open, and the region chosen in it.
  const [choosing, setChoosing] = useState(false);
  const [chosen, setChosen] = useState<string | null | undefined>(undefined);

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
              if (putBackRegions === null) {
                void run(() => onPutBack(null));
                return;
              }
              setChosen(startRegion(region, putBackRegions));
              setChoosing(true);
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
      {putBackRegions !== null && (
        <Modal
          opened={choosing}
          onClose={() => setChoosing(false)}
          title={t("list.putBackTitle", { name })}
          centered
          data-testid={`${testId}-put-back-dialog`}
        >
          <FormLayout>
            {chosen === undefined ? (
              <FormField size="full">
                <Text size="sm" data-testid={`${testId}-put-back-no-region`}>
                  {t("list.putBackNoRegion")}
                </Text>
              </FormField>
            ) : (
              <FormField size="medium" hint={t("list.putBackRegionHint")}>
                <RegionSelect
                  value={chosen}
                  onChange={setChosen}
                  regions={putBackRegions.regions}
                  noRegion={putBackRegions.noRegion}
                  scope={scope}
                  testId={`${testId}-put-back-region`}
                />
              </FormField>
            )}
            <FormFooter
              onCancel={() => setChoosing(false)}
              action={
                <Button
                  type="button"
                  disabled={chosen === undefined}
                  data-testid={`${testId}-put-back-confirm`}
                  data-changes="true"
                  onClick={() => {
                    if (chosen === undefined) return;
                    const into = chosen;
                    setChoosing(false);
                    void run(() => onPutBack(into));
                  }}
                >
                  {t("list.putBack")}
                </Button>
              }
            />
          </FormLayout>
        </Modal>
      )}
      {said !== null && (notice ? createPortal(said, notice) : notice === undefined ? said : null)}
    </>
  );
}
