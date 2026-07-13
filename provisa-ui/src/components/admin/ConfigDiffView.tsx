// Copyright (c) 2026 Kenneth Stott
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
}

/**
 * Standard side-by-side diff of the original (file) vs current (live-state) config, using CodeMirror
 * MergeView. Revert arrows in the center gutter push an original chunk into the current side — that
 * is the per-line/per-chunk undo. The right side is editable; onCurrentChange reports the revised doc
 * (what an "apply" would upload).
 */
export function ConfigDiffView({ original, current, onCurrentChange }: ConfigDiffViewProps) {
  const host = useRef<HTMLDivElement>(null);
  const changeRef = useRef(onCurrentChange);
  changeRef.current = onCurrentChange;

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
        extensions: [
          lineNumbers(),
          EditorView.updateListener.of((u) => {
            if (u.docChanged) changeRef.current?.(u.state.doc.toString());
          }),
        ],
      },
      // Center revert arrows apply an original chunk onto the current side (undo a change).
      revertControls: "a-to-b",
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
  }, [original, current]);

  return <div ref={host} className="config-diff" data-testid="config-diff" />;
}
