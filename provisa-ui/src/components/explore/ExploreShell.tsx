// Copyright (c) 2026 Kenneth Stott
// Canary: 7e2b9d41-5c3a-4f86-b1d7-0a9e6c2f5b18
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: the one frame every Explore surface stands in. The page gutter the management pages
// keep (--page-gutter), the surface's toolbar as a strip at the top of it, and its panes edge to
// edge below, divided by single lines -- no inner frame, band or card. A side panel and an editor
// above its results are resized by dragging the divider between them (SplitHandle), and the size
// is kept per viewer (useResizableSize).

import { useRef, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { useResizableSize } from "../../hooks/useResizableSize";
import { SplitHandle } from "../list/SplitHandle";
import "./explore.css";

export function ExploreShell({
  testId,
  toolbar,
  children,
  className,
  answered,
}: {
  /** The surface's own test id; the shell also carries `data-explore-shell`. */
  testId: string;
  /** The surface's toolbar: a strip at the top of the gutter. */
  toolbar?: ReactNode;
  children: ReactNode;
  /** The surface's own class, for its own rules inside the shell. */
  className?: string;
  /** REQ-2010: marked once the surface has answered, which a demo tour step waits for. */
  answered?: boolean;
}) {
  return (
    <div
      className={className ? `explore-shell ${className}` : "explore-shell"}
      data-explore-shell=""
      data-testid={testId}
      data-answered={answered ? "" : undefined}
    >
      {toolbar && (
        <div className="explore-shell__toolbar" data-testid={`${testId}-toolbar`}>
          {toolbar}
        </div>
      )}
      <div className="explore-shell__body">{children}</div>
    </div>
  );
}

/** The panes of a surface side by side: a side panel at the left, the work beside it. */
export function ExploreSide({
  storageKey,
  initial,
  min,
  max,
  testId,
  children,
  className,
}: {
  /** Where the viewer's width is kept. */
  storageKey: string;
  initial: number;
  min: number;
  max: number;
  testId: string;
  children: ReactNode;
  className?: string;
}) {
  const { t } = useTranslation();
  const { ref, size, startResize, onHandleKeyDown } = useResizableSize<HTMLElement>(storageKey, {
    initial,
    min,
    max,
    edge: "right",
  });
  return (
    <>
      <aside
        ref={ref}
        className={className ? `explore-side ${className}` : "explore-side"}
        style={{ width: size }}
        data-testid={testId}
      >
        {children}
      </aside>
      <SplitHandle
        orientation="vertical"
        label={t("explore.resizeSide")}
        onPointerDown={startResize}
        onKeyDown={onHandleKeyDown}
        testId={`${testId}-handle`}
      />
    </>
  );
}

/** The least room the results keep below the editor: their strip, a grid's filter row and chips,
 * its header and a few rows. It holds on screen as well as on a drag: in a stack shorter than the
 * editor's kept height and this, the editor gives way, down to its own least. */
const RESULTS_MIN = 240;

/** An editor above its results, the divider between them dragged to share the height. */
export function ExploreStack({
  storageKey,
  initial,
  min,
  testId,
  editor,
  results,
}: {
  /** Where the viewer's editor height is kept. */
  storageKey: string;
  initial: number;
  min: number;
  testId: string;
  editor: ReactNode;
  results: ReactNode;
}) {
  const { t } = useTranslation();
  const stackRef = useRef<HTMLDivElement>(null);
  const { ref, size, startResize, onHandleKeyDown } = useResizableSize<HTMLDivElement>(storageKey, {
    initial,
    min,
    max: () =>
      (stackRef.current ? stackRef.current.clientHeight : window.innerHeight) - RESULTS_MIN,
    edge: "bottom",
  });
  return (
    <div className="explore-stack" ref={stackRef} data-testid={testId}>
      <div ref={ref} className="explore-stack__editor" style={{ height: size, minHeight: min }}>
        {editor}
      </div>
      <SplitHandle
        orientation="horizontal"
        label={t("explore.resizeEditor")}
        onPointerDown={startResize}
        onKeyDown={onHandleKeyDown}
        testId={`${testId}-handle`}
      />
      <div className="explore-stack__results" style={{ minHeight: RESULTS_MIN }}>
        {results}
      </div>
    </div>
  );
}

/** The work beside a side panel: the rest of the row. */
export function ExploreMain({
  children,
  className,
  answered,
}: {
  children: ReactNode;
  className?: string;
  /** REQ-2010: marked once the surface has answered, which a demo tour step waits for. */
  answered?: boolean;
}) {
  return (
    <div
      className={className ? `explore-main ${className}` : "explore-main"}
      data-answered={answered ? "" : undefined}
    >
      {children}
    </div>
  );
}
