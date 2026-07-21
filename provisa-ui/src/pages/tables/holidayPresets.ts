// Copyright (c) 2026 Kenneth Stott
// Canary: 76f70a8b-fab5-4b65-a6b5-1b8c4f2672f7
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-962: preset public-holiday sets for a snapshot calendar. Holidays are CONCRETE dates (not
// rules — the calendar stores them versioned/immutable), so a preset expands to the actual dates
// across a year range. US Federal/Bank observe the Federal Reserve list; NYSE is the stock-market
// list (adds Good Friday, drops Columbus + Veterans). Weekend-falling fixed holidays shift to the
// observed weekday, since that's the day business is actually closed.

export type HolidayPreset = "us_federal" | "us_nyse";

function iso(y: number, m1: number, d: number): string {
  return `${y}-${String(m1).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
}

// The n-th `weekday` (0=Sun..6=Sat) of `month0` (0-based). n>=1; n=-1 → the last one.
function nthWeekday(year: number, month0: number, weekday: number, n: number): Date {
  if (n === -1) {
    const last = new Date(year, month0 + 1, 0); // last day of the month
    const back = (last.getDay() - weekday + 7) % 7;
    return new Date(year, month0, last.getDate() - back);
  }
  const first = new Date(year, month0, 1);
  const offset = (weekday - first.getDay() + 7) % 7;
  return new Date(year, month0, 1 + offset + (n - 1) * 7);
}

// Federal observed rule: a fixed holiday on Saturday is observed the Friday before; on Sunday the
// Monday after. Floating (Monday/Thursday) holidays never hit a weekend, so this only moves fixed ones.
function observed(year: number, month0: number, day: number): Date {
  const d = new Date(year, month0, day);
  if (d.getDay() === 6) return new Date(year, month0, day - 1); // Sat → Fri
  if (d.getDay() === 0) return new Date(year, month0, day + 1); // Sun → Mon
  return d;
}

// Anonymous Gregorian computus → Easter Sunday for `year` (NYSE Good Friday = Easter − 2 days).
function easterSunday(year: number): Date {
  const a = year % 19;
  const b = Math.floor(year / 100);
  const c = year % 100;
  const d = Math.floor(b / 4);
  const e = b % 4;
  const f = Math.floor((b + 8) / 25);
  const g = Math.floor((b - f + 1) / 3);
  const h = (19 * a + b - d - g + 15) % 30;
  const i = Math.floor(c / 4);
  const k = c % 4;
  const l = (32 + 2 * e + 2 * i - h - k) % 7;
  const mm = Math.floor((a + 11 * h + 22 * l) / 451);
  const month = Math.floor((h + l - 7 * mm + 114) / 31); // 3=March, 4=April
  const day = ((h + l - 7 * mm + 114) % 31) + 1;
  return new Date(year, month - 1, day);
}

function d2iso(d: Date): string {
  return iso(d.getFullYear(), d.getMonth() + 1, d.getDate());
}

function holidaysForYear(preset: HolidayPreset, y: number): string[] {
  const out: string[] = [];
  const federalFloating = [
    d2iso(nthWeekday(y, 0, 1, 3)), // MLK — 3rd Monday of January
    d2iso(nthWeekday(y, 1, 1, 3)), // Washington's Birthday — 3rd Monday of February
    d2iso(nthWeekday(y, 4, 1, -1)), // Memorial Day — last Monday of May
    d2iso(nthWeekday(y, 8, 1, 1)), // Labor Day — 1st Monday of September
    d2iso(nthWeekday(y, 10, 4, 4)), // Thanksgiving — 4th Thursday of November
  ];
  const fixed = [
    d2iso(observed(y, 0, 1)), // New Year's Day
    d2iso(observed(y, 5, 19)), // Juneteenth
    d2iso(observed(y, 6, 4)), // Independence Day
    d2iso(observed(y, 11, 25)), // Christmas
  ];
  out.push(...federalFloating, ...fixed);
  if (preset === "us_federal") {
    out.push(d2iso(nthWeekday(y, 9, 1, 2))); // Columbus Day — 2nd Monday of October
    out.push(d2iso(observed(y, 10, 11))); // Veterans Day
  } else {
    // NYSE: add Good Friday, omit Columbus + Veterans
    const easter = easterSunday(y);
    out.push(d2iso(new Date(y, easter.getMonth(), easter.getDate() - 2)));
  }
  return out;
}

/** All preset holiday dates (ISO YYYY-MM-DD) for the inclusive year range, sorted + de-duplicated. */
export function presetHolidays(preset: HolidayPreset, fromYear: number, toYear: number): string[] {
  const set = new Set<string>();
  const lo = Math.min(fromYear, toYear);
  const hi = Math.max(fromYear, toYear);
  for (let y = lo; y <= hi; y++) for (const d of holidaysForYear(preset, y)) set.add(d);
  return [...set].sort();
}
