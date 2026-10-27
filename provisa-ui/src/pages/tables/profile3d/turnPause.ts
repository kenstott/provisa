// Copyright (c) 2026 Kenneth Stott
// Canary: efbcdbce-ee74-41bb-8e84-84c1bab50c4f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A turning stage stops turning once nobody is using the page: after TURN_IDLE_MS with no pointer
// or keyboard input anywhere on it, and turns again on the next input. A stopped stage draws
// nothing (frameLoop.ts), so a page left open with a turning stage on it costs no CPU.

/** How long a stage keeps turning with no pointer or keyboard input on the page. */
export const TURN_IDLE_MS = 10_000;

const INPUT = ["pointerdown", "pointermove", "keydown", "wheel"] as const;

export class TurnPause {
  private timer: ReturnType<typeof setTimeout> | undefined;
  private idle = false;
  private readonly onChange: () => void;

  /** ``onChange`` is called each time the page goes idle or is used again. */
  constructor(onChange: () => void) {
    this.onChange = onChange;
    for (const e of INPUT)
      document.addEventListener(e, this.used, { capture: true, passive: true });
    this.arm();
  }

  /** Whether no input has reached the page for TURN_IDLE_MS. */
  get paused(): boolean {
    return this.idle;
  }

  dispose() {
    clearTimeout(this.timer);
    for (const e of INPUT) document.removeEventListener(e, this.used, { capture: true });
  }

  private used = () => {
    this.arm();
    if (!this.idle) return;
    this.idle = false;
    this.onChange();
  };

  private arm() {
    clearTimeout(this.timer);
    this.timer = setTimeout(() => {
      this.idle = true;
      this.onChange();
    }, TURN_IDLE_MS);
  }
}
