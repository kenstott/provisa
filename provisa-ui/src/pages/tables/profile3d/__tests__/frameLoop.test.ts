// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A 3D stage draws a frame only when something changed or moves, and nothing while it cannot be
// seen: an idle page costs no CPU (a headless browser on CI ran a stage's loop at a full core).

import { describe, expect, it, vi } from "vitest";
import { FrameLoop } from "../frameLoop";

function harness(moving: () => boolean = () => false) {
  const queued: ((now: number) => void)[] = [];
  const raf = vi.fn((cb: (now: number) => void) => {
    queued.push(cb);
    return queued.length;
  });
  const caf = vi.fn();
  const draws: boolean[] = [];
  const loop = new FrameLoop(
    (_now, changed) => {
      draws.push(changed);
      return moving();
    },
    raf,
    caf,
  );
  // Runs the frames the browser has been asked for, as many ticks as given.
  const tick = (n = 1) => {
    for (let i = 0; i < n; i++) {
      const due = queued.splice(0);
      due.forEach((cb) => cb(i * 16));
    }
  };
  return { loop, raf, caf, draws, tick, queued };
}

describe("a 3D stage's frame loop", () => {
  it("draws its first frame, then asks for none while nothing changes", () => {
    const h = harness();
    h.tick(10);
    expect(h.draws).toEqual([true]);
    expect(h.queued).toHaveLength(0);
  });

  it("draws one frame for a change, however many changes come before it", () => {
    const h = harness();
    h.tick();
    h.loop.invalidate();
    h.loop.invalidate();
    h.tick(5);
    expect(h.draws).toEqual([true, true]);
  });

  it("keeps drawing while something moves, and stops when it settles", () => {
    let left = 3;
    const h = harness(() => left-- > 0);
    h.tick(10);
    expect(h.draws).toHaveLength(4);
    expect(h.queued).toHaveLength(0);
  });

  it("draws nothing while it cannot be seen, and catches up once it can", () => {
    const h = harness(() => true); // a turning stage
    h.tick();
    h.loop.setShown(false);
    const before = h.draws.length;
    h.tick(10);
    h.loop.invalidate();
    h.tick(10);
    expect(h.draws.length).toBe(before);
    h.loop.setShown(true);
    h.tick();
    expect(h.draws.length).toBe(before + 1);
  });

  it("asks for nothing after it is disposed", () => {
    const h = harness(() => true);
    h.tick();
    h.loop.dispose();
    h.tick(5);
    h.loop.invalidate();
    expect(h.queued).toHaveLength(0);
    expect(h.caf).toHaveBeenCalled();
  });
});
