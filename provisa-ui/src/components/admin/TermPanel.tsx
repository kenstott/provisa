// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A term's detail on the Ontology page, as the standard detail panel draws one (SplitPane): a
// pinned head with the term's name and, beside it, what the content puts in the actions slot
// (PanelActions: the term's icon actions and toggles; the edit form's Save and Cancel while it is
// open); under the head, what a form or a delete has to say; then the content, scrolling beneath.
// The term list stays beside it as the page draws it, so there is no expand, close or resize.
// A delete in progress (PanelDelete) disables the head's actions and the content's fields.

import { useState, type ReactNode } from "react";
import { Title } from "@mantine/core";
import {
  PanelActionsContext,
  PanelDeletingContext,
  PanelNoticeContext,
  type usePanelDeleting,
} from "../list/panelActions";
import { PanelHeadStyle } from "../list/PanelHeadStyle";

export function TermPanel({
  name,
  host,
  children,
}: {
  /** The term's name: the panel's heading. */
  name: string;
  /** The page's `usePanelDeleting`: a delete in progress locks the panel. */
  host: ReturnType<typeof usePanelDeleting>;
  children: ReactNode;
}) {
  const [actionsSlot, setActionsSlot] = useState<HTMLFieldSetElement | null>(null);
  const [noticeSlot, setNoticeSlot] = useState<HTMLDivElement | null>(null);
  return (
    <div
      style={{ display: "flex", flexDirection: "column", flex: 1, minHeight: 0, minWidth: 0 }}
      data-testid="glossary-panel"
    >
      <div className="split-pane__head">
        <PanelHeadStyle>
          <Title order={4} className="split-pane__title" data-testid="glossary-panel-title">
            {name}
          </Title>
          {/* A fieldset, so a delete in progress disables every action put here. */}
          <fieldset
            className="split-pane__actions"
            ref={setActionsSlot}
            disabled={host.locked}
            data-testid="glossary-panel-actions"
          />
        </PanelHeadStyle>
      </div>
      <div className="split-pane__notice" ref={setNoticeSlot} data-testid="glossary-panel-notice" />
      <fieldset
        className="split-pane__body"
        disabled={host.locked}
        inert={host.locked}
        aria-busy={host.locked || undefined}
        data-testid="panel-delete-lock"
      >
        <PanelActionsContext.Provider value={actionsSlot}>
          <PanelNoticeContext.Provider value={noticeSlot}>
            <PanelDeletingContext.Provider value={host.api}>
              {children}
            </PanelDeletingContext.Provider>
          </PanelNoticeContext.Provider>
        </PanelActionsContext.Provider>
      </fieldset>
    </div>
  );
}
