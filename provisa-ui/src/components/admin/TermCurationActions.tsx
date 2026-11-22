// Copyright (c) 2026 Kenneth Stott
// Canary: 3d9e41b7-5a0c-4f2e-8b6d-c17a2e94f05d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A term's status changes and its delete (REQ-1387, REQ-2144, REQ-2198). Shown only where the
// server would accept them (REQ-1327): the term page renders this for a caller whose detail says
// the term is curatable.

import { Button } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { Archive, ArchiveRestore, BadgeCheck, Hourglass } from "lucide-react";
import { restoreGlossaryTerm, type GlossaryTermDetail } from "../../api/glossary";
import { Tip } from "../Tip";
import { PanelDelete } from "../list/PanelDelete";
import { PanelNoticeContext } from "../list/panelActions";

export function TermCurationActions({
  detail,
  act,
  onApprove,
  onDeprecate,
  onRetire,
  deleteNotice,
  onDelete,
}: {
  detail: GlossaryTermDetail;
  act: (fn: () => Promise<void>, successMessage?: string) => Promise<void>;
  onApprove: () => void;
  onDeprecate: () => void;
  onRetire: () => void;
  deleteNotice: HTMLDivElement | null;
  onDelete: () => Promise<void>;
}) {
  const { t } = useTranslation();
  const linked =
    detail.refs.length > 0 || detail.edges_out.length > 0 || detail.edges_in.length > 0;
  return (
    <>
      {/* REQ-1387, REQ-2144: each status change is a person's act. A defined draft is
        approved, its impact seen first; an approved term deprecated, with a
        reason; any term in service retired; a deprecation or a retirement undone. */}
      {detail.status === "draft" && (
        <Tip
          text={
            (detail.definition ?? "").trim()
              ? t("glossaryTab.approveHint")
              : t("glossaryTab.approveNeedsDefinition")
          }
        >
          <span>
            <Button
              variant="light"
              color="teal"
              leftSection={<BadgeCheck size={14} />}
              disabled={!(detail.definition ?? "").trim()}
              onClick={onApprove}
              data-testid="glossary-approve-btn"
            >
              {t("glossaryTab.approve")}
            </Button>
          </span>
        </Tip>
      )}
      {detail.status === "approved" && (
        <Tip text={t("glossaryTab.deprecateHint")}>
          <Button
            variant="light"
            color="gray"
            leftSection={<Hourglass size={14} />}
            onClick={onDeprecate}
            data-testid="glossary-deprecate-btn"
          >
            {t("glossaryTab.deprecate")}
          </Button>
        </Tip>
      )}
      {detail.status !== "retired" && (
        <Tip text={t("glossaryTab.retireHint")}>
          <Button
            variant="light"
            color="orange"
            leftSection={<Archive size={14} />}
            onClick={onRetire}
            data-testid="glossary-retire-btn"
          >
            {t("glossaryTab.retire")}
          </Button>
        </Tip>
      )}
      {(detail.status === "deprecated" || detail.status === "retired") && (
        <Tip text={t("glossaryTab.unretireHint")}>
          <Button
            variant="light"
            color="teal"
            leftSection={<ArchiveRestore size={14} />}
            onClick={() =>
              void act(() => restoreGlossaryTerm(detail.id), t("glossaryTab.unretireDone"))
            }
            data-testid="glossary-restore-btn"
          >
            {t("glossaryTab.unretire")}
          </Button>
        </Tip>
      )}
      <PanelNoticeContext.Provider value={deleteNotice}>
        <PanelDelete
          itemKey={detail.id}
          text={t("glossaryTab.delete")}
          ariaLabel={t("glossaryTab.delete")}
          title={t("glossaryTab.deleteTitle")}
          consequence={t("glossaryTab.deleteConfirm", { name: detail.name })}
          disabled={linked}
          tip={linked ? t("glossaryTab.deleteDisabledHint") : null}
          testId="glossary-delete-btn"
          confirmTestId="glossary-delete-confirm"
          onDelete={onDelete}
        />
      </PanelNoticeContext.Provider>
    </>
  );
}
