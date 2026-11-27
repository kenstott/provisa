// Copyright (c) 2026 Kenneth Stott
// Canary: 0d7c9a35-81e4-4f2b-b6a3-e94f50c2d718
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277 rule 10: the one modal dialog layout. Every dialog is a ModalLayout: its title at the
// top, its content in a body that scrolls, and its buttons in a fixed bar at its bottom, outside
// the scrolling content, so scrolling the content never hides them (as the relationship discovery
// dialog first did). modalGuard.test.ts fails a file that renders Mantine's Modal itself, or a
// dialog button outside the bar.

import { useState, type ReactNode } from "react";
import { Modal, type ModalProps } from "@mantine/core";
import { PanelActionsContext, PanelNoticeContext } from "../list/panelActions";
import { ModalFooterContext } from "./modalFooter";

export interface ModalLayoutProps extends ModalProps {
  /** The dialog's content. A `FormFooter` anywhere inside it (in this file or a component it
   *  renders) is drawn in the dialog's fixed button bar, not where it stands in the content: the
   *  dialog's actions go in a `FormFooter` (`onSave`/`submit`, `onCancel`, `action`, and further
   *  buttons as its children). A `FormFooter submit` names its `<form>` with `form`, since the bar
   *  is outside the form's element. */
  children?: ReactNode;
  /**
   * The content fills the dialog's height and scrolls parts of itself (a table under a pinned
   * header, an editor): the body is a flex column that does not scroll as a whole, and a child
   * that takes `flex: 1` with `minHeight: 0` gets the height left. Left out, the body scrolls as
   * a whole, which is what every other dialog wants.
   */
  fill?: boolean;
  /** The content meets the dialog's edges: the body has no inset of its own (a canvas, a graph,
   *  a workbench that draws its own). The title and the bar keep theirs. */
  flush?: boolean;
}

function join(...names: (string | undefined | false)[]): string {
  return names.filter(Boolean).join(" ");
}

/**
 * A modal dialog: Mantine's Modal (focus trap, `role="dialog"`, Escape and overlay dismissal,
 * focus restored on close), with a scrolling body and a fixed button bar. It takes every prop
 * Mantine's Modal takes; `padding` sets the inset of both the body and the bar. The bar is not
 * drawn while no button is in it.
 */
export function ModalLayout({
  children,
  fill = false,
  flush = false,
  classNames,
  ...rest
}: ModalLayoutProps) {
  const [bar, setBar] = useState<HTMLDivElement | null>(null);
  const own = typeof classNames === "object" && classNames !== null ? classNames : {};
  return (
    <Modal
      {...rest}
      classNames={{
        ...own,
        content: join("modal-layout__content", own.content),
        body: join("modal-layout__body", own.body),
      }}
    >
      {/* A dialog opened from a detail panel is not part of that panel: its forms' buttons go in
          the dialog's bar, never in the panel's head behind it. */}
      <PanelActionsContext.Provider value={undefined}>
        <PanelNoticeContext.Provider value={undefined}>
          <ModalFooterContext.Provider value={bar}>
            <div
              className={join(
                "modal-layout__scroll",
                fill && "modal-layout__scroll--fill",
                flush && "modal-layout__scroll--flush",
              )}
              data-modal-scroll
            >
              {children}
            </div>
          </ModalFooterContext.Provider>
        </PanelNoticeContext.Provider>
      </PanelActionsContext.Provider>
      <div ref={setBar} className="modal-layout__bar" data-testid="modal-footer-bar" />
    </Modal>
  );
}
