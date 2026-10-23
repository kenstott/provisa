// Copyright (c) 2026 Kenneth Stott
// Canary: aa8acffe-0520-4265-a4ca-40e5109d562d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A list with a detail panel on its right. The panel pushes the list narrower rather than covering
// it; its left edge resizes it (the mechanism Polly's panel uses, useResizableWidth) and the width
// is kept per browser; it expands to the full width and back. Where the row is too narrow for a
// list and a panel side by side, the list gives way: the panel takes its place, with a way back.
//
// The panel's head is pinned: the title, then whatever the content puts in the actions slot
// (PanelActions), then expand and close. The content scrolls beneath it.

import {
  useContext,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
  type KeyboardEvent,
  type ReactNode,
} from "react";
import { createPortal } from "react-dom";
import { ActionIcon, Button, Title } from "@mantine/core";
import { ArrowLeft, Maximize2, Minimize2, X } from "lucide-react";
import { useResizableWidth } from "../../hooks/useResizableWidth";
import { useTranslation } from "react-i18next";
import { ConfirmDialog } from "../ConfirmDialog";
import { PanelActionsContext, PanelNoticeContext } from "./panelActions";
import { PanelHeadStyle } from "./PanelHeadStyle";
import { PanelLeaveContext, usePanelLeave, type PanelLeave } from "./panelLeave";

/** The narrowest the panel is dragged, and the least room the list keeps beside it. */
const PANEL_MIN = 360;
const LIST_MIN = 280;
/** Below this width of the whole row the list and the panel no longer fit side by side. */
const SIDE_BY_SIDE_MIN = 900;

export interface SplitPaneLabels {
  /** The panel's accessible name. */
  panel: string;
  resize: string;
  expand: string;
  restore: string;
  close: string;
  backToList: string;
}

/**
 * Puts its children in the pinned head of the panel it is rendered in. Outside a panel it renders
 * them where it stands, so a view that works under a list row works in a panel unchanged.
 */
export function PanelActions({ children }: { children: ReactNode }) {
  const slot = useContext(PanelActionsContext);
  // The head's one style goes with the actions wherever they are rendered.
  const styled = <PanelHeadStyle>{children}</PanelHeadStyle>;
  if (slot === undefined) return styled;
  // The head is not mounted on the first pass; the actions appear with it on the next.
  return slot === null ? null : createPortal(styled, slot);
}

export function SplitPane({
  list,
  title,
  children,
  onClose,
  closeOnEscape = true,
  storageKey,
  labels,
  testId,
  leave,
  onDiscard,
}: {
  list: ReactNode;
  /** The panel's heading. */
  title: ReactNode;
  /** The panel's content; `null` closes the panel and gives the list the row. */
  children: ReactNode | null;
  onClose: () => void;
  /** False while leaving would drop what is being typed: Escape then does nothing. */
  closeOnEscape?: boolean;
  storageKey: string;
  labels: SplitPaneLabels;
  testId: string;
  /** The page's own `usePanelLeave()`, where the page guards openers of its own with it. */
  leave?: PanelLeave;
  /** Drops the open form's edit: called when the reader chooses Discard, before they leave. */
  onDiscard?: () => void;
}) {
  const { t } = useTranslation();
  const ownLeave = usePanelLeave();
  const leaving = leave ?? ownLeave;
  // The panel's own ways out ask first when its form has changes. Escape keeps its own rule.
  const close = () => leaving.guard(onClose);
  const open = children !== null;
  const rowRef = useRef<HTMLDivElement>(null);
  const [rowWidth, setRowWidth] = useState<number | null>(null);
  const [expanded, setExpanded] = useState(false);
  const [actionsSlot, setActionsSlot] = useState<HTMLDivElement | null>(null);
  const [noticeSlot, setNoticeSlot] = useState<HTMLDivElement | null>(null);

  const { ref, width, startResize, onHandleKeyDown } = useResizableWidth<HTMLElement>(storageKey, {
    // Not moved yet: the panel takes its share of the row (see `shown`).
    initial: 0,
    min: PANEL_MIN,
    max: () => (rowRef.current ? rowRef.current.clientWidth : window.innerWidth) - LIST_MIN,
  });

  // The row's own width decides whether the two fit side by side: Polly open beside the page
  // narrows the row without narrowing the window.
  useLayoutEffect(() => {
    const row = rowRef.current;
    if (!row) return;
    setRowWidth(row.clientWidth);
    const observer = new ResizeObserver(() => setRowWidth(row.clientWidth));
    observer.observe(row);
    return () => observer.disconnect();
  }, []);
  // A row that reports no width has not been laid out (a test's document): side by side.
  const narrow = rowWidth !== null && rowWidth > 0 && rowWidth < SIDE_BY_SIDE_MIN;
  // About 60% of the row until the handle is moved: Polly open beside the page leaves the list
  // its share too. A row that reports no width has not been laid out: the window's stands in.
  const row = rowWidth !== null && rowWidth > 0 ? rowWidth : window.innerWidth;
  const shown = Math.max(
    PANEL_MIN,
    width > 0 ? Math.min(width, row - LIST_MIN) : Math.round(row * 0.6),
  );
  const alone = open && (narrow || expanded);

  // Focus goes into the panel when it opens and back to where it was when it closes.
  const returnFocus = useRef<Element | null>(null);
  useEffect(() => {
    if (open) {
      returnFocus.current = document.activeElement;
      // A modal dialog over the page keeps the focus: a panel opening under it (a linked item whose
      // data arrives after a first visit's welcome is up) would otherwise take the reader's Escape
      // for the dialog as its own close.
      if (!document.activeElement?.closest('[aria-modal="true"]')) ref.current?.focus();
      return;
    }
    const back = returnFocus.current;
    returnFocus.current = null;
    if (back instanceof HTMLElement && back.isConnected) back.focus();
  }, [open, ref]);

  const onPanelKeyDown = (e: KeyboardEvent) => {
    // A menu or a picker inside the panel that used Escape to close itself has claimed it.
    if (e.key !== "Escape" || e.defaultPrevented || !closeOnEscape) return;
    e.stopPropagation();
    onClose();
  };
  const provided = (node: ReactNode) => (
    <PanelLeaveContext.Provider value={leaving}>{node}</PanelLeaveContext.Provider>
  );

  return (
    <div className="split-pane" ref={rowRef} data-testid={testId}>
      <div className="split-pane__list" hidden={alone}>
        {provided(list)}
      </div>
      {/* In the page only while it is asking: a pane that asks nothing holds no dialog. It
          goes with the answer, at once -- there is no closing dialog left behind it. */}
      {leaving.asking !== null && (
        <ConfirmDialog
          opened
          title={t("common.discardTitle")}
          consequence={t("common.discardBody", { what: leaving.asking })}
          confirmLabel={t("common.discard")}
          onCancel={() => leaving.answer(false)}
          onConfirm={() => {
            onDiscard?.();
            leaving.answer(true);
          }}
          testId="discard-changes"
        />
      )}
      {open && (
        <>
          {!alone && (
            <div
              role="separator"
              aria-orientation="vertical"
              aria-label={labels.resize}
              tabIndex={0}
              className="split-pane__handle"
              onPointerDown={startResize}
              onKeyDown={onHandleKeyDown}
              data-testid={`${testId}-handle`}
            >
              <div />
            </div>
          )}
          <aside
            ref={ref}
            tabIndex={-1}
            className="split-pane__panel"
            aria-label={labels.panel}
            data-alone={alone || undefined}
            style={alone ? undefined : { width: shown }}
            onKeyDown={onPanelKeyDown}
            data-testid={`${testId}-panel`}
          >
            <div className="split-pane__head">
              <PanelHeadStyle>
                {narrow && (
                  <Button
                    leftSection={<ArrowLeft size={14} />}
                    onClick={close}
                    data-testid={`${testId}-back`}
                  >
                    {labels.backToList}
                  </Button>
                )}
                {/* The name, then a thin line, then the actions. The line is the name's own
                  trailing edge (App.css), so it never starts a row when the actions wrap, adds
                  nothing to the reading order, and is not drawn when the head has no actions. */}
                <Title order={4} className="split-pane__title" data-testid={`${testId}-title`}>
                  {title}
                </Title>
                <div className="split-pane__actions" ref={setActionsSlot} />
                {!narrow && (
                  <ActionIcon
                    aria-label={expanded ? labels.restore : labels.expand}
                    aria-pressed={expanded}
                    onClick={() => setExpanded((v) => !v)}
                    data-testid={`${testId}-expand`}
                  >
                    {expanded ? <Minimize2 size={16} /> : <Maximize2 size={16} />}
                  </ActionIcon>
                )}
                <ActionIcon
                  aria-label={labels.close}
                  onClick={close}
                  data-testid={`${testId}-close`}
                >
                  <X size={16} />
                </ActionIcon>
              </PanelHeadStyle>
            </div>
            {/* What a form in the panel has to say of itself (a refused save): pinned too. */}
            <div className="split-pane__notice" ref={setNoticeSlot} />
            <div className="split-pane__body">
              <PanelActionsContext.Provider value={actionsSlot}>
                <PanelNoticeContext.Provider value={noticeSlot}>
                  {provided(children)}
                </PanelNoticeContext.Provider>
              </PanelActionsContext.Provider>
            </div>
          </aside>
        </>
      )}
    </div>
  );
}
