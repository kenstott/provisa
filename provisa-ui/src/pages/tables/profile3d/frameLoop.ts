// Copyright (c) 2026 Kenneth Stott
// Canary: d2ecbb5b-4d34-4e72-9563-3e4acd8c2bc6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A 3D stage's frames, drawn on demand: one for each change (invalidate), and one per animation
// frame only while something moves -- a camera tween, the orbit settling, the stage turning, a
// scene's own animation. Nothing is asked of the browser while the stage cannot be seen, or once
// it is gone. An idle page costs no CPU: a stage that drew every frame forever ran a headless
// browser at a full core on CI, and a laptop's fan on an idle page.

export class FrameLoop {
  private id = 0;
  private changed = true;
  private shown = true;
  private gone = false;

  /**
   * ``draw(now, changed)`` advances what moves and draws when ``changed`` or anything moved; it
   * answers whether anything still moves, which asks for the next frame.
   */
  private readonly draw: (now: number, changed: boolean) => boolean;
  private readonly raf: (cb: (now: number) => void) => number;
  private readonly caf: (id: number) => void;

  constructor(
    draw: (now: number, changed: boolean) => boolean,
    raf: (cb: (now: number) => void) => number = (cb) => requestAnimationFrame(cb),
    caf: (id: number) => void = (id) => cancelAnimationFrame(id),
  ) {
    this.draw = draw;
    this.raf = raf;
    this.caf = caf;
    this.wake();
  }

  /** What is drawn changed: draw it on the next frame. */
  invalidate() {
    this.changed = true;
    this.wake();
  }

  /** Whether the stage can be seen (on screen, the page shown): nothing is drawn while not. */
  setShown(shown: boolean) {
    this.shown = shown;
    if (shown) this.invalidate();
    else this.cancel();
  }

  dispose() {
    this.gone = true;
    this.cancel();
  }

  private wake() {
    if (!this.id && this.shown && !this.gone) this.id = this.raf(this.tick);
  }

  private cancel() {
    if (this.id) this.caf(this.id);
    this.id = 0;
  }

  private tick = (now: number) => {
    this.id = 0;
    if (this.gone || !this.shown) return;
    const changed = this.changed;
    this.changed = false;
    if (this.draw(now, changed) || this.changed) this.wake();
  };
}
