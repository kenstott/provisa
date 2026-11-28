// Copyright (c) 2026 Kenneth Stott
// Canary: 0874ac56-7ca9-4875-8268-77306128581d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1795: the size of a pane resized by dragging one of its edges and kept per browser. One
// mechanism for every such pane: Polly, a page's detail panel, and (REQ-2277) an Explore surface's
// side panel and its editor above the results.

import { useRef, type KeyboardEvent, type PointerEvent as ReactPointerEvent } from "react";
import { useLocalStorage } from "../components/graph/graph-persistence";

/** One press of an arrow key on the resize handle. */
const KEY_STEP = 32;

/** The edge of the pane its handle stands on: a panel docked at the right of a row is dragged by
 * its left edge, a side panel at the left by its right edge, an editor above results by its bottom. */
export type ResizeEdge = "left" | "right" | "bottom";

export interface ResizableSize<E extends HTMLElement> {
  /** Goes on the pane: the drag measures from the pane's edge opposite the handle. */
  ref: React.RefObject<E | null>;
  /** The kept width (or, for a bottom edge, height). */
  size: number;
  /** `onPointerDown` of the handle. */
  startResize: (e: ReactPointerEvent) => void;
  /** `onKeyDown` of the handle: the arrow keys move the edge. */
  onHandleKeyDown: (e: KeyboardEvent) => void;
}

/**
 * `max` may depend on what else shares the row, so it is asked for at the moment of the drag.
 * The size starts at `initial` and, once changed, is kept under `storageKey`. A pane whose
 * starting size depends on its surroundings passes 0 and sizes itself until it is first moved.
 */
export function useResizableSize<E extends HTMLElement>(
  storageKey: string,
  {
    initial,
    min,
    max,
    edge = "left",
  }: { initial: number; min: number; max: number | (() => number); edge?: ResizeEdge },
): ResizableSize<E> {
  const [size, setSize] = useLocalStorage(storageKey, initial);
  const ref = useRef<E | null>(null);
  const resizingRef = useRef(false);
  // The least size wins over the most: a pane in a row too small for both keeps its least.
  const clamp = (w: number) =>
    Math.max(min, Math.min(typeof max === "function" ? max() : max, Math.round(w)));

  const startResize = (e: ReactPointerEvent) => {
    const pane = ref.current;
    if (!pane) return;
    e.preventDefault();
    resizingRef.current = true;
    // The edge opposite the handle stays where it is; the handle's edge follows the pointer.
    const box = pane.getBoundingClientRect();
    const sizeAt = (ev: PointerEvent) =>
      edge === "left"
        ? box.right - ev.clientX
        : edge === "right"
          ? ev.clientX - box.left
          : ev.clientY - box.top;
    const onMove = (ev: PointerEvent) => {
      if (!resizingRef.current) return;
      setSize(clamp(sizeAt(ev)));
    };
    const onUp = () => {
      resizingRef.current = false;
      window.removeEventListener("pointermove", onMove);
      window.removeEventListener("pointerup", onUp);
    };
    window.addEventListener("pointermove", onMove);
    window.addEventListener("pointerup", onUp);
  };

  const onHandleKeyDown = (e: KeyboardEvent) => {
    // The key that moves the handle's edge outward grows the pane.
    const grow = edge === "left" ? "ArrowLeft" : edge === "right" ? "ArrowRight" : "ArrowDown";
    const shrink = edge === "left" ? "ArrowRight" : edge === "right" ? "ArrowLeft" : "ArrowUp";
    if (e.key !== grow && e.key !== shrink) return;
    const pane = ref.current;
    if (!pane) return;
    e.preventDefault();
    // From the size the pane has on screen, which is not the kept one until it is first moved.
    const shown = edge === "bottom" ? pane.offsetHeight : pane.offsetWidth;
    setSize(clamp(shown + (e.key === grow ? KEY_STEP : -KEY_STEP)));
  };

  return { ref, size, startResize, onHandleKeyDown };
}
