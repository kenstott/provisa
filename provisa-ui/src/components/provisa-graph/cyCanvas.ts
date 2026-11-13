// Copyright (c) 2026 Kenneth Stott
// Canary: 0aa9c98b-a975-47e9-9cab-a31a1d55ddbb
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A cytoscape core's canvas, and its end while ELK is still laying it out. ELK answers
// asynchronously and places the nodes when it does: a core destroyed before then throws there
// (its renderer is gone: "Cannot read properties of null (reading 'notify') at Core.endBatch"),
// and a core destroyed later empties its container -- which, were it the host, would hold the next
// core's canvas by then, so that diagram goes blank. So each core draws into a div of its own
// inside the host; ended while a layout is pending, its div leaves the page at once, the core is
// unmounted (a renderer that draws nothing) and destroyed when the last layout ends.

/** What of a cytoscape core its end needs; cytoscape's typings leave out its ``unmount``. */
interface EndingCore {
  destroy(): void;
  unmount(): void;
}

/** What of a cytoscape layout running it needs. */
export interface RunningLayout {
  one(event: "layoutstop", fn: () => void): unknown;
  /** ``layoutfail`` (elkLayout's ``LAYOUT_FAIL``): a layout that failed after it started. */
  one(event: "layoutfail", fn: (evt: unknown, error: Error) => void): unknown;
  run(): unknown;
}

export interface CyCanvas {
  /** The div the core is made in. */
  container: HTMLDivElement;
  /** Runs ``layout``; ``then`` once it has placed the nodes, or ``failed`` with the error of
   * one that fails after it started -- either unless the core has ended. */
  run(layout: RunningLayout, then?: () => void, failed?: (error: Error) => void): void;
  /** The core's end: at once, or -- a layout pending -- when the last one ends. */
  end(cy: { destroy(): void }): void;
}

export function cyCanvas(host: HTMLElement): CyCanvas {
  const container = document.createElement("div");
  container.style.width = "100%";
  container.style.height = "100%";
  host.appendChild(container);
  let pending = 0;
  let ended: { destroy(): void } | null = null;
  return {
    container,
    run(layout, then, failed) {
      pending += 1;
      /** The layout's end, placed or failed: ``answer`` unless the core has ended. */
      const done = (answer?: () => void) => {
        pending -= 1;
        if (ended === null) answer?.();
        else if (pending === 0) ended.destroy();
      };
      layout.one("layoutstop", () => done(then));
      layout.one("layoutfail", (_evt, error) => done(failed && (() => failed(error))));
      layout.run();
    },
    end(cy) {
      container.remove();
      if (pending === 0) cy.destroy();
      else {
        ended = cy;
        (cy as EndingCore).unmount();
      }
    },
  };
}
