// Copyright (c) 2026 Kenneth Stott
// Canary: 3d9e41b7-5a0c-4f2e-8b6d-c17a2e94f05d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A term panel's body (PanelBody, REQ-2277 rule 7) and, while the term is not being edited, its
// actions in the panel's head (TermPanel), icons only, as the Tables panel's head has
// them (maintainer 2026-11-26): Edit, then the term's toggles -- its approval, pii and exclusion
// from metadata export -- then Duplicate, its status changes and Delete (REQ-1387, REQ-2144,
// REQ-2145, REQ-2198). Shown only where the server would accept them (REQ-1327): a caller who may
// write the glossary may duplicate a term; one whose detail says the term is curatable does the
// rest.

import type { ReactNode } from "react";
import { ActionIcon } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { Archive, ArchiveRestore, BadgeCheck, FileX, Hourglass, Pencil } from "lucide-react";
import {
  restoreGlossaryTerm,
  updateGlossaryTerm,
  type GlossaryTermDetail,
} from "../../api/glossary";
import { Tip } from "../Tip";
import { PanelActions } from "../list/SplitPane";
import { PanelBody } from "../list/PanelBody";
import { PanelDelete } from "../list/PanelDelete";
import { PanelDuplicate } from "../list/PanelDuplicate";
import { PanelToggle } from "../list/PanelToggle";
import { TermPiiToggle } from "./TermPiiToggle";

export function TermHeadActions({
  detail,
  canEdit,
  canCurate,
  act,
  onEdit,
  onDuplicate,
  onApprove,
  onDeprecate,
  onRetire,
  onDelete,
  onTagsChanged,
  editing,
  children,
}: {
  detail: GlossaryTermDetail;
  /** The caller may write the glossary: Duplicate. */
  canEdit: boolean;
  /** The caller may curate this term: Edit, the toggles, the status changes and Delete. */
  canCurate: boolean;
  act: (fn: () => Promise<void>, successMessage?: string) => Promise<void>;
  onEdit: () => void;
  onDuplicate: () => void;
  onApprove: () => void;
  onDeprecate: () => void;
  onRetire: () => void;
  onDelete: () => Promise<void>;
  /** A tag was put on the term or taken off; the term is read again. */
  onTagsChanged: () => void;
  /** The term is being edited: its form puts Save and Cancel in the head, not these. */
  editing: boolean;
  /** The panel's body: its parts, set apart by the standard rhythm. */
  children: ReactNode;
}) {
  const { t } = useTranslation();
  const defined = (detail.definition ?? "").trim() !== "";
  const approved = detail.status === "approved";
  const linked =
    detail.refs.length > 0 || detail.edges_out.length > 0 || detail.edges_in.length > 0;
  const actions = !editing && (canEdit || canCurate);
  return (
    <>
      {actions && (
        <PanelActions>
          {canCurate && (
            <Tip text={t("glossaryTab.editTip", { name: detail.name })} named>
              <ActionIcon
                aria-label={t("glossaryTab.editTip", { name: detail.name })}
                onClick={onEdit}
                data-testid="glossary-edit-btn"
              >
                <Pencil size={14} />
              </ActionIcon>
            </Tip>
          )}
          {/* REQ-1387, REQ-2144: approval is a person's act, its impact seen first. A draft is
          approved here; an approved term leaves service by Deprecate or Retire, never by
          turning approval off. */}
          {canCurate && (detail.status === "draft" || approved) && (
            <PanelToggle
              label={t("glossaryTab.approve")}
              on={approved}
              fixed={approved}
              disabled={!approved && !defined}
              tip={
                approved
                  ? t("glossaryTab.approvedTip")
                  : defined
                    ? t("glossaryTab.approveHint")
                    : t("glossaryTab.approveNeedsDefinition")
              }
              icon={<BadgeCheck size={14} />}
              color="teal"
              onToggle={onApprove}
              testId="glossary-approve-btn"
            />
          )}
          {canCurate && <TermPiiToggle term={detail} onChanged={onTagsChanged} />}
          {canCurate && (
            <PanelToggle
              label={t("glossaryTab.excludeFromExportLabel")}
              on={detail.export_excluded}
              tip={
                detail.export_excluded
                  ? t("glossaryTab.exportExcludedOnTip")
                  : t("glossaryTab.exportExcludedOffTip")
              }
              icon={<FileX size={14} />}
              onToggle={() =>
                void act(() =>
                  updateGlossaryTerm(detail.id, { export_excluded: !detail.export_excluded }),
                )
              }
              testId="glossary-export-excluded-toggle"
            />
          )}
          {canEdit && (
            <PanelDuplicate
              name={detail.name}
              onDuplicate={onDuplicate}
              testId="glossary-duplicate-btn"
            />
          )}
          {canCurate && approved && (
            <Tip text={t("glossaryTab.deprecateHint")}>
              <ActionIcon
                aria-label={t("glossaryTab.deprecate")}
                onClick={onDeprecate}
                data-testid="glossary-deprecate-btn"
              >
                <Hourglass size={14} />
              </ActionIcon>
            </Tip>
          )}
          {canCurate && detail.status !== "retired" && (
            <Tip text={t("glossaryTab.retireHint")}>
              <ActionIcon
                color="orange"
                aria-label={t("glossaryTab.retire")}
                onClick={onRetire}
                data-testid="glossary-retire-btn"
              >
                <Archive size={14} />
              </ActionIcon>
            </Tip>
          )}
          {canCurate && (detail.status === "deprecated" || detail.status === "retired") && (
            <Tip text={t("glossaryTab.unretireHint")}>
              <ActionIcon
                color="teal"
                aria-label={t("glossaryTab.unretire")}
                onClick={() =>
                  void act(() => restoreGlossaryTerm(detail.id), t("glossaryTab.unretireDone"))
                }
                data-testid="glossary-restore-btn"
              >
                <ArchiveRestore size={14} />
              </ActionIcon>
            </Tip>
          )}
          {canCurate && (
            <PanelDelete
              itemKey={detail.id}
              ariaLabel={t("glossaryTab.delete")}
              title={t("glossaryTab.deleteTitle")}
              consequence={t("glossaryTab.deleteConfirm", { name: detail.name })}
              disabled={linked}
              tip={linked ? t("glossaryTab.deleteDisabledHint") : null}
              testId="glossary-delete-btn"
              confirmTestId="glossary-delete-confirm"
              onDelete={onDelete}
            />
          )}
        </PanelActions>
      )}
      <PanelBody>{children}</PanelBody>
    </>
  );
}
