// Copyright (c) 2026 Kenneth Stott
// Canary: 9548cece-f729-4253-8280-33e9f2851d32
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2198: a detail panel's Delete, which works like its Save. The reader confirms; from then
// until the delete settles the control shows a spinner in place of its icon (Save's busy state,
// usePendingPress), the panel's fields and actions are disabled, and the item's row in the list
// is marked as being deleted and offers it for nothing. A delete that went through closes the
// panel, the list already read again without the item; one that failed unlocks the panel and
// says why, where a refused save says it (under the panel's head).

import {
  useContext,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
  type MouseEvent,
  type ReactNode,
} from "react";
import { createPortal } from "react-dom";
import { ActionIcon, Button, Loader } from "@mantine/core";
import { Trash2 } from "lucide-react";
import { useTranslation } from "react-i18next";
import { ConfirmDialog } from "../ConfirmDialog";
import { Tip } from "../Tip";
import { usePendingPress } from "../form/usePendingPress";
import {
  DeletingKeysContext,
  PanelDeletingContext,
  PanelNoticeContext,
  type usePanelDeleting,
} from "./panelActions";

export interface PanelDeleteProps {
  /** The item's key, as its row in the list knows it (`ListRow` `itemKey`, `ListItems` `rowKey`). */
  itemKey: string | number;
  /** The confirmation's title and what deleting does. */
  title: string;
  consequence: string;
  /** The control's accessible name and tip. */
  ariaLabel: string;
  testId?: string;
  /**
   * The delete. It resolves once the item is deleted and the list read again without it; it
   * resolves `false` where nothing was deleted and the page has said why itself (a delete refused
   * over the item's dependents, REQ-1918); it rejects, with the reason, where the delete failed.
   */
  onDelete: () => Promise<boolean | void>;
  /** The control's icon at rest, where a trash can does not say it (discarding a proposal). */
  icon?: ReactNode;
  /** The confirmation's own word and test id, where "Confirm" and the default do not suit. */
  confirmLabel?: string;
  confirmTestId?: string;
  /**
   * The control's word, where it stands among the detail's other worded actions (a toolbar of
   * Edit, Retire, ...) rather than as an icon in the panel's head: it is then a text button, its
   * icon (or spinner) before the word.
   */
  text?: string;
  /** Where the item cannot be deleted now (it has dependents); `tip` then says why. */
  disabled?: boolean;
  /** The control's tip, where it is not its accessible name; `null` for none. */
  tip?: string | null;
}

const reasonOf = (error: unknown) =>
  error instanceof Error ? error.message : typeof error === "string" ? error : String(error);

export function PanelDelete({
  itemKey,
  title,
  consequence,
  ariaLabel,
  testId,
  onDelete,
  icon = <Trash2 size={14} />,
  confirmLabel,
  confirmTestId,
  text,
  disabled = false,
  tip,
}: PanelDeleteProps) {
  const { t } = useTranslation();
  const pane = useContext(PanelDeletingContext);
  const notice = useContext(PanelNoticeContext);
  // What was said of a failed delete, and of which item: another item in the same panel is not
  // told it.
  const [failed, setFailed] = useState<{ key: string; problem: string } | null>(null);
  const problem = failed !== null && failed.key === String(itemKey) ? failed.problem : null;
  // The item the panel shows now: a delete that finishes after the reader closed the panel or
  // moved to another item does not close what is open then.
  const shown = useRef<string | null>(String(itemKey));
  useLayoutEffect(() => {
    shown.current = String(itemKey);
  });
  useEffect(() => {
    shown.current = String(itemKey);
    return () => {
      shown.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- mounted or not; the key is kept above
  }, []);

  const release = useRef<(() => void) | null>(null);
  const unlock = useRef<(() => void) | null>(null);
  const deleting = useRef<string | null>(null);
  const { busy, press } = usePendingPress<boolean | void>(
    () => {
      const key = String(itemKey);
      setFailed(null);
      deleting.current = key;
      release.current = pane?.markDeleting(key) ?? null;
      unlock.current = pane?.lock() ?? null;
      return onDelete();
    },
    (outcome) => {
      const key = deleting.current;
      release.current?.();
      release.current = null;
      // Released with the settle, in the commit that shows the problem, not in a later effect pass.
      unlock.current?.();
      unlock.current = null;
      deleting.current = null;
      if (!outcome.ok) {
        if (key !== null)
          setFailed({
            key,
            problem: t("common.deleteFailed", { reason: reasonOf(outcome.error) }),
          });
        return;
      }
      if (outcome.value !== false && key === shown.current) pane?.close();
    },
  );
  // The panel is locked while this delete is pending, and no longer than this control is shown.
  useEffect(() => () => unlock.current?.(), []);

  const said = problem ? (
    <div
      className="form-problem"
      role="alert"
      data-testid={testId ? `${testId}-problem` : "form-problem"}
    >
      {problem}
    </div>
  ) : null;

  return (
    <>
      <ConfirmDialog
        title={title}
        consequence={consequence}
        // The dialog closes at once: the delete's progress is the panel's, not the dialog's.
        onConfirm={press}
        confirmLabel={confirmLabel ?? t("common.delete")}
        testId={confirmTestId ?? (testId ? `${testId}-confirm-dialog` : undefined)}
      >
        {(open) => {
          const shared = {
            color: "red",
            "aria-label": ariaLabel,
            "data-testid": testId,
            disabled: busy || disabled,
            "data-deleting": busy || undefined,
            "aria-busy": busy || undefined,
            // REQ-2115: what Polly presses here changes something; she asks the user first.
            "data-changes": "true",
            onClick: (e: MouseEvent) => {
              e.stopPropagation();
              open();
            },
          } as const;
          const spinner = <Loader size={14} color="currentColor" />;
          return text !== undefined ? (
            <Tip text={tip === undefined ? ariaLabel : tip} named={tip === undefined}>
              {/* A disabled button takes no pointer: the tip is held by its wrapper. */}
              <span>
                <Button variant="light" leftSection={busy ? spinner : icon} {...shared}>
                  {text}
                </Button>
              </span>
            </Tip>
          ) : (
            <Tip text={tip === undefined ? ariaLabel : tip} named={tip === undefined}>
              <ActionIcon
                // Borderless, as the panel head's one style (PanelHeadStyle) draws it, and the
                // same beneath a row, where that style is not applied.
                variant="subtle"
                {...shared}
              >
                {busy ? spinner : icon}
              </ActionIcon>
            </Tip>
          );
        }}
      </ConfirmDialog>
      {said !== null && (notice ? createPortal(said, notice) : notice === undefined ? said : null)}
    </>
  );
}

/**
 * REQ-2198: for a list whose rows open their detail beneath them, in place of a SplitPane: the
 * detail's content, locked while its Delete is in progress (fields and actions disabled), with
 * the rows of the items being deleted marked (`DeletingRows`). `host` is the list's
 * `usePanelDeleting`.
 */
export function PanelDeleteLock({
  host,
  children,
}: {
  host: ReturnType<typeof usePanelDeleting>;
  children: ReactNode;
}) {
  return (
    <fieldset
      className="panel-lock"
      disabled={host.locked}
      inert={host.locked}
      aria-busy={host.locked || undefined}
      data-testid="panel-delete-lock"
    >
      <PanelDeletingContext.Provider value={host.api}>{children}</PanelDeletingContext.Provider>
    </fieldset>
  );
}

/** The list beside a `PanelDeleteLock`: its rows of items being deleted are marked so. */
export function DeletingRows({
  host,
  children,
}: {
  host: ReturnType<typeof usePanelDeleting>;
  children: ReactNode;
}) {
  return (
    <DeletingKeysContext.Provider value={host.deleting}>{children}</DeletingKeysContext.Provider>
  );
}
