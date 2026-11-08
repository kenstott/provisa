// Copyright (c) 2026 Kenneth Stott
// Canary: 8d2c6e15-a94f-4b37-9e02-3f7b1d6c4a58
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A tour step that explains a page with a graph on it must not lay its text over the graph. driver.js
// places a popover against the step's anchor, and an anchor as large as a detail panel leaves it
// nowhere but on top of what the panel holds; so once it is placed, it is moved clear of the graph.

/** A place on the page. */
export interface Rect {
  left: number;
  top: number;
  width: number;
  height: number;
}

export interface ClearPlace {
  left: number;
  top: number;
  /** Set where the popover has to narrow to fit beside the graph. */
  width?: number;
}

const MARGIN = 16;
/** The narrowest a popover is made to fit beside a graph; below it, it goes above or below. */
const MIN_WIDTH = 280;

/** Where a popover of ``size`` sits clear of ``graph`` in a window of ``view``: beside the graph
 * first, left then right, above and below after; null if there is no room anywhere. */
export function clearPlace(
  size: { width: number; height: number },
  graph: Rect,
  view: { width: number; height: number },
): ClearPlace | null {
  const top = Math.max(MARGIN, Math.min(graph.top, view.height - size.height - MARGIN));
  const left = Math.max(MARGIN, Math.min(graph.left, view.width - size.width - MARGIN));
  const roomLeft = graph.left - 2 * MARGIN;
  if (roomLeft >= MIN_WIDTH) {
    const width = Math.min(size.width, roomLeft);
    return { left: graph.left - MARGIN - width, top, ...(width < size.width ? { width } : {}) };
  }
  const right = graph.left + graph.width;
  const roomRight = view.width - right - 2 * MARGIN;
  if (roomRight >= MIN_WIDTH) {
    const width = Math.min(size.width, roomRight);
    return { left: right + MARGIN, top, ...(width < size.width ? { width } : {}) };
  }
  if (graph.top - 2 * MARGIN >= size.height)
    return { left, top: graph.top - MARGIN - size.height };
  const bottom = graph.top + graph.height;
  if (view.height - bottom - 2 * MARGIN >= size.height) return { left, top: bottom + MARGIN };
  return null;
}

/** A step's graph: the element, and whether the step brings it into view (a step about the graph)
 * or only keeps its text off it when it is on screen. */
export interface ClearOf {
  selector: string;
  reveal: boolean;
}

/** Moves the popover on screen clear of the step's graph, and again whenever driver.js would have
 * put it back (it places the popover anew on each scroll and resize), until ``signal`` aborts. The
 * returned function places it now; a step that lights its anchor again calls it. */
export function keepClearOf(clearOf: ClearOf, signal: AbortSignal): () => void {
  let revealed = false;
  const place = () => {
    const popover = document.querySelector<HTMLElement>(".driver-popover");
    const graph = document.querySelector<HTMLElement>(clearOf.selector);
    if (!popover || !graph) return;
    if (clearOf.reveal && !revealed) {
      revealed = true;
      graph.scrollIntoView({ block: "nearest" });
    }
    const view = { width: window.innerWidth, height: window.innerHeight };
    const box = graph.getBoundingClientRect();
    popover.style.width = "";
    const size = () => ({ width: popover.offsetWidth, height: popover.offsetHeight });
    const at = clearPlace(size(), box, view);
    if (at === null) return;
    if (at.width !== undefined) popover.style.width = `${at.width}px`;
    // Narrowed, it is taller: placed again at the height it now has.
    const final = at.width !== undefined ? (clearPlace(size(), box, view) ?? at) : at;
    popover.style.left = `${final.left}px`;
    popover.style.top = `${final.top}px`;
    popover.style.right = "auto";
    popover.style.bottom = "auto";
    popover.classList.add("tour-popover-clear");
  };
  let frame = 0;
  const after = () => {
    cancelAnimationFrame(frame);
    frame = requestAnimationFrame(place);
  };
  window.addEventListener("resize", after);
  window.addEventListener("scroll", after, true);
  signal.addEventListener(
    "abort",
    () => {
      cancelAnimationFrame(frame);
      window.removeEventListener("resize", after);
      window.removeEventListener("scroll", after, true);
      document.querySelector(".driver-popover")?.classList.remove("tour-popover-clear");
    },
    { once: true },
  );
  after();
  return after;
}
