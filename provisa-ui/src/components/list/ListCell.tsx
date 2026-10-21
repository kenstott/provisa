// Copyright (c) 2026 Kenneth Stott
// Canary: 0fe345b1-e045-4e73-8879-8f5abb05dc4c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useLayoutEffect, useRef, useState, type ReactNode } from "react";
import { Badge } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { Tip } from "../Tip";
import { LIST_CUT_MIN_CHARS, LIST_WRAP_MIN_CHARS } from "./listCellLimits";
import { useCut } from "./useCut";

/**
 * The text of one cell of a page list, held to the column's cap and the row's maximum height:
 *
 * - a short single value: one line, at its whole width;
 * - a long single value (an id, an address, a path: no space in it): one line, cut with an
 *   ellipsis; its column gives up width before the list grows past the window, down to a minimum;
 * - words (anything with a space in it, however short): its column gives up width before the list
 *   grows past the window and the text wraps at its spaces, the row growing up to its maximum of
 *   lines, where the last line ends with an ellipsis. A long text keeps a minimum width so it does
 *   not read one word to a line; a short phrase may wrap down to its longest word.
 *
 * A cell that is cut shows its whole value on hover, and takes keyboard focus so the same can be
 * read without a pointer. A cell that is not cut is not a tab stop.
 */
export function ListCell({
  text,
  oneLine = false,
  children = text,
}: {
  /** The cell's value as text: what decides how it is cut, and what the hint shows whole. */
  text: string;
  /** Cut on one line like a single value, whatever its spaces (a page's one-line column). */
  oneLine?: boolean;
  /** How the page drew that text, where it gave it a look of its own (code, a dimmed line). */
  children?: ReactNode;
}) {
  const { ref, cut } = useCut(text);
  // Text with a space in it wraps; a single value gives up width only when it is longer than what
  // it keeps when squeezed.
  const value = text.trim();
  const words = !oneLine && /\s/.test(value);
  const kind = words ? "words" : oneLine || value.length > LIST_CUT_MIN_CHARS ? "token" : "value";
  // Only text longer than a wrapped column's minimum is held to that minimum.
  const wide = words && value.length > LIST_WRAP_MIN_CHARS;
  return (
    <Tip text={cut ? text : false}>
      <div
        ref={ref}
        className={`list-cell list-cell--${kind}${wide ? " list-cell--wide" : ""}${oneLine ? " list-cell--one-line" : ""}`}
        data-cut={cut || undefined}
        tabIndex={cut ? 0 : undefined}
      >
        {children}
      </div>
    </Tip>
  );
}

/**
 * A cell of several values drawn as pills. They wrap inside the column up to the row's maximum
 * height; the ones that do not fit are counted in a last pill ("+3 more") whose hint lists every
 * value.
 */
export function ListPills({
  values,
  pill = (value) => (
    <Badge variant="light" size="sm">
      {value}
    </Badge>
  ),
}: {
  values: string[];
  pill?: (value: string) => ReactNode;
}) {
  const { t } = useTranslation();
  const ref = useRef<HTMLDivElement>(null);
  // How many pills are drawn; the rest are counted. Starts at all of them and is cut down to
  // what the cell shows whole, leaving room for the count.
  const [shown, setShown] = useState(values.length);
  const all = values.join(", ");
  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    const measure = () => {
      const pills = [...el.querySelectorAll<HTMLElement>("[data-pill]")];
      const foot = el.clientHeight;
      const whole = pills.filter((p) => p.offsetTop + p.offsetHeight <= foot + 1).length;
      // One fewer than fit, so the count has a place on the last line.
      setShown((now) => (whole < pills.length ? Math.max(1, Math.min(now, whole) - 1) : now));
    };
    measure();
    const watch = new ResizeObserver(() => {
      setShown(values.length);
      requestAnimationFrame(measure);
    });
    watch.observe(el);
    return () => watch.disconnect();
    // eslint-disable-next-line react-hooks/exhaustive-deps -- remeasured when the values change
  }, [all]);
  const hidden = values.length - shown;
  return (
    <div ref={ref} className="list-cell list-cell--pills" data-cut={hidden > 0 || undefined}>
      {values.slice(0, shown).map((value) => (
        <span key={value} data-pill="">
          {pill(value)}
        </span>
      ))}
      {hidden > 0 && (
        <Tip text={all}>
          <span className="list-cell__more" tabIndex={0} data-testid="list-pills-more">
            {t("list.moreValues", { count: hidden })}
          </span>
        </Tip>
      )}
    </div>
  );
}
