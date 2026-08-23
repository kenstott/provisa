// Copyright (c) 2026 Kenneth Stott
// Canary: 88e6cb14-c3e1-4af2-9976-9c628251ff2f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useRef } from "react";
import { MergeView } from "@codemirror/merge";
import { EditorState } from "@codemirror/state";
import { EditorView, lineNumbers } from "@codemirror/view";

interface ConfigDiffViewProps {
  /** Original config (on-disk file) — the left, read-only side. */
  original: string;
  /** Current config (live state) — the right, editable side. */
  current: string;
  /** Fires with the edited right-side doc as chunks are reverted or edited. */
  onCurrentChange?: (doc: string) => void;
  /**
   * REQ-1524: compare two refs of the repository without offering to change either.
   *
   * Both sides of a repository diff are history — the right one is a commit that has already been
   * made, not a draft — so the editable side and the revert arrows would offer an edit that has
   * nowhere to go. This is the same view with those two affordances withheld.
   */
  readOnly?: boolean;
}

/**
 * Standard side-by-side diff of the original (file) vs current (live-state) config, using CodeMirror
 * MergeView. Revert arrows in the center gutter push an original chunk into the current side — that
 * is the per-line/per-chunk undo. The right side is editable; onCurrentChange reports the revised doc
 * (what an "apply" would upload).
 */
export function ConfigDiffView({
  original,
  current,
  onCurrentChange,
  readOnly = false,
}: ConfigDiffViewProps) {
  const host = useRef<HTMLDivElement>(null);
  // Kept in a ref so the MergeView effect below can stay mounted across renders: writing it during
  // render mutates a ref while React is rendering, so the write belongs in a commit-phase effect.
  const changeRef = useRef(onCurrentChange);
  useEffect(() => {
    changeRef.current = onCurrentChange;
  });

  useEffect(() => {
    if (!host.current) return;
    const view = new MergeView({
      parent: host.current,
      // a = original (left, read-only), b = current (right, editable).
      a: {
        doc: original,
        extensions: [lineNumbers(), EditorState.readOnly.of(true), EditorView.editable.of(false)],
      },
      b: {
        doc: current,
        extensions: readOnly
          ? [lineNumbers(), EditorState.readOnly.of(true), EditorView.editable.of(false)]
          : [
              lineNumbers(),
              EditorView.updateListener.of((u) => {
                if (u.docChanged) changeRef.current?.(u.state.doc.toString());
              }),
            ],
      },
      // Center revert arrows apply an original chunk onto the current side (undo a change).
      revertControls: readOnly ? undefined : "a-to-b",
      highlightChanges: true,
      gutter: true,
      collapseUnchanged: { margin: 3, minSize: 4 },
    });

    // MergeView scrolls its two editors independently — keep them (and their line-number gutters) in
    // lockstep. A guard flag prevents the mirrored scroll from echoing back into a feedback loop.
    let syncing = false;
    const link = (from: EditorView, to: EditorView) => {
      const onScroll = () => {
        if (syncing) return;
        syncing = true;
        to.scrollDOM.scrollTop = from.scrollDOM.scrollTop;
        to.scrollDOM.scrollLeft = from.scrollDOM.scrollLeft;
        syncing = false;
      };
      from.scrollDOM.addEventListener("scroll", onScroll);
      return () => from.scrollDOM.removeEventListener("scroll", onScroll);
    };
    const unlink = [link(view.a, view.b), link(view.b, view.a)];

    return () => {
      unlink.forEach((fn) => fn());
      view.destroy();
    };
  }, [original, current, readOnly]);

  return <div ref={host} className="config-diff" data-testid="config-diff" />;
}
