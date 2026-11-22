// Copyright (c) 2026 Kenneth Stott
// Canary: ea22566a-2831-45a8-b6f6-f1425ce324a6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A stand-in for Mermaid's render in the plan diagram's tests: the chart's own comment lines say
// how big its image is and where its nodes are, and the SVG it returns is shaped as Mermaid's is
// (a viewBox, and each node a <g class="node"> with Mermaid's element id and its translate).
//
//   %% size 4000x3000
//   %% node p1 2000 40

export function mockSvg(id: string, chart: string): string {
  const size = /%% size (\d+)x(\d+)/.exec(chart);
  const [w, h] = size ? [Number(size[1]), Number(size[2])] : [200, 100];
  const nodes = [...chart.matchAll(/%% node (\S+) (\d+) (\d+)/g)]
    .map(
      ([, key, x, y], i) =>
        `<g class="node default" id="${id}-flowchart-${key}-${i}" transform="translate(${x}, ${y})">` +
        `<rect x="-40" y="-15" width="80" height="30"></rect><text>${key}</text></g>`,
    )
    .join("");
  return (
    `<svg id="${id}" width="100%" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${w} ${h}" ` +
    `style="max-width: ${w}px;"><g class="root"><g class="nodes">${nodes}</g></g></svg>`
  );
}

/** Gives every plan diagram's stage a size, as the page would (jsdom lays nothing out). */
export function sizeStages(width: number, height: number): () => void {
  const proto = HTMLElement.prototype;
  const w = Object.getOwnPropertyDescriptor(proto, "clientWidth");
  const h = Object.getOwnPropertyDescriptor(proto, "clientHeight");
  Object.defineProperty(proto, "clientWidth", {
    configurable: true,
    get(this: HTMLElement) {
      return this.classList.contains("plan-stage") ? width : 0;
    },
  });
  Object.defineProperty(proto, "clientHeight", {
    configurable: true,
    get(this: HTMLElement) {
      return this.classList.contains("plan-stage") ? height : 0;
    },
  });
  return () => {
    if (w) Object.defineProperty(proto, "clientWidth", w);
    if (h) Object.defineProperty(proto, "clientHeight", h);
  };
}

/** The view's zoom and pan, as the diagram reports them on its drawing. */
export function viewOf(el: Element): { zoom: number; x: number; y: number } {
  const view = el.querySelector('[data-testid="plan-diagram-view"]') as HTMLElement;
  return {
    zoom: Number(view.dataset.zoom),
    x: Number(view.dataset.panX),
    y: Number(view.dataset.panY),
  };
}
