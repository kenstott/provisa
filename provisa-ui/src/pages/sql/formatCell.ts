// Copyright (c) 2026 Kenneth Stott
// Canary: 8c2b41ad-79f5-4e0e-9a3d-6f0c2b17e9d4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1437: instant columns are rendered for a reader, not for a parser. The wire form is the
// engine's — `2026-08-12T12:01:59.174658Z` — whose T separator, fractional seconds and zone suffix
// are all noise in a grid cell. A value carrying a zone is shown in the viewer's own zone, because
// that is the clock the reader is comparing it against; a value carrying none is already the
// reader's wall time and is shown unchanged rather than being shifted by a zone it never had.

const TIMESTAMP_RE =
  /^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2}))?(?:\.\d+)?(Z|[+-]\d{2}:?\d{2})?$/;

function pad(n: number): string {
  return String(n).padStart(2, "0");
}

/** `YYYY-MM-DD HH:mm:ss` in the viewer's zone, or null when the text is not a timestamp. */
export function formatTimestamp(text: string): string | null {
  const m = TIMESTAMP_RE.exec(text);
  if (m === null) return null;
  const [, year, month, day, hour, minute, second, zone] = m;
  if (zone === undefined) return `${year}-${month}-${day} ${hour}:${minute}:${second ?? "00"}`;
  const at = new Date(text);
  if (Number.isNaN(at.getTime())) return null;
  return (
    `${at.getFullYear()}-${pad(at.getMonth() + 1)}-${pad(at.getDate())} ` +
    `${pad(at.getHours())}:${pad(at.getMinutes())}:${pad(at.getSeconds())}`
  );
}

const DATE_ONLY_RE = /^(\d{4})-(\d{2})-(\d{2})$/;

/** An instant recovered from its wire text: the clock to write, and whether it carries a time. */
export interface Instant {
  /** The wall clock `formatTimestamp` displays, carried in the Date's UTC fields — a spreadsheet
      serial is a plain day count with no zone, so the UTC fields are what lands in the cell. */
  at: Date;
  hasTime: boolean;
}

/** The instant a cell carries, or null when the text is not one. */
export function parseInstant(text: string): Instant | null {
  const dateOnly = DATE_ONLY_RE.exec(text);
  if (dateOnly !== null) {
    const [, year, month, day] = dateOnly;
    return { at: new Date(Date.UTC(+year, +month - 1, +day)), hasTime: false };
  }
  const shown = formatTimestamp(text);
  if (shown === null) return null;
  const [date, time] = shown.split(" ");
  const [year, month, day] = date.split("-");
  const [hour, minute, second] = time.split(":");
  return {
    at: new Date(Date.UTC(+year, +month - 1, +day, +hour, +minute, +second)),
    hasTime: true,
  };
}

/** Display text for a result cell — the value as-is unless it is an instant. */
export function formatCell(value: unknown): string | null {
  if (value == null) return null;
  const text = String(value);
  return formatTimestamp(text) ?? text;
}
