// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { describe, it, expect } from "vitest";
import { presetHolidays } from "../holidayPresets";

describe("presetHolidays (REQ-962)", () => {
  it("computes the 11 US Federal holidays for 2026 with weekend-observed shifts", () => {
    const h = presetHolidays("us_federal", 2026, 2026);
    expect(h).toEqual([
      "2026-01-01", // New Year (Thu)
      "2026-01-19", // MLK — 3rd Mon Jan
      "2026-02-16", // Washington — 3rd Mon Feb
      "2026-05-25", // Memorial — last Mon May
      "2026-06-19", // Juneteenth (Fri)
      "2026-07-03", // Independence Day — Jul 4 is a Sat → observed Fri
      "2026-09-07", // Labor — 1st Mon Sep
      "2026-10-12", // Columbus — 2nd Mon Oct
      "2026-11-11", // Veterans (Wed)
      "2026-11-26", // Thanksgiving — 4th Thu Nov
      "2026-12-25", // Christmas (Fri)
    ]);
  });

  it("uses the NYSE list: adds Good Friday, drops Columbus + Veterans", () => {
    const h = presetHolidays("us_nyse", 2026, 2026);
    expect(h).toContain("2026-04-03"); // Good Friday (Easter 2026 = Apr 5)
    expect(h).not.toContain("2026-10-12"); // no Columbus Day
    expect(h).not.toContain("2026-11-11"); // no Veterans Day
    expect(h).toContain("2026-11-26"); // still Thanksgiving
  });

  it("covers an inclusive multi-year range, sorted and de-duplicated", () => {
    const h = presetHolidays("us_federal", 2026, 2028);
    expect(h[0]).toBe("2026-01-01");
    // each year in the range contributes holidays (New Year 2028 observes to 2027-12-31 — a Saturday)
    expect(h.some((d) => d.startsWith("2026"))).toBe(true);
    expect(h.some((d) => d.startsWith("2027"))).toBe(true);
    expect(h.some((d) => d.startsWith("2028"))).toBe(true);
    expect(new Set(h).size).toBe(h.length); // no duplicates
    expect([...h]).toEqual([...h].sort()); // sorted
  });
});
