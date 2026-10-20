// Copyright (c) 2026 Kenneth Stott
// Canary: f7a88b1a-4128-4bbd-90a2-28569dbebce2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The tour's status card (waiting, stuck) is drawn while driver.js runs, and driver.js's own sheet
// turns pointer events off for every element but its popover and the step's element. The card's
// buttons took no click in a browser while a test's fireEvent, which ignores CSS, clicked them.
// Both sheets are applied here as the app applies them, and the card's buttons must take a click.

import { describe, it, expect, afterEach } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const sheet = (path: string) => readFileSync(resolve(__dirname, path), "utf8");

afterEach(() => {
  document.head.innerHTML = "";
  document.body.innerHTML = "";
  document.body.className = "";
});

describe("the tour's status card while driver.js is active", () => {
  it("takes clicks on its buttons", () => {
    for (const css of [
      sheet("../../node_modules/driver.js/dist/driver.css"),
      sheet("../tour/tour.css"),
    ]) {
      const style = document.createElement("style");
      style.textContent = css;
      document.head.appendChild(style);
    }
    document.body.className = "driver-active";
    document.body.innerHTML = `
      <main><button id="page">page</button></main>
      <div class="tour-status-overlay"><div class="tour-status-card">
        <div class="tour-status-actions"><button id="skip" class="tour-status-btn">Skip</button></div>
      </div></div>`;
    // The page itself stays inert, as driver.js means it to.
    expect(getComputedStyle(document.getElementById("page")!).pointerEvents).toBe("none");
    expect(getComputedStyle(document.getElementById("skip")!).pointerEvents).toBe("auto");
  });
});
