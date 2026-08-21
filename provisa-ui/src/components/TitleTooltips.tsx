// Copyright (c) 2026 Kenneth Stott
// Canary: 222d51d3-02f8-4b27-85b0-3b6be978a2c9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import "./TitleTooltips.css";

const DELAY_MS = 350;
const GAP_PX = 8;

type Shown = { text: string; top: number; left: number; above: boolean };

/**
 * REQ-1520: every hover hint in the app rendered in the app's own styling.
 *
 * Hundreds of controls carry a native `title`, which the browser paints in its own chrome —
 * a different font, a different colour, and no relation to the theme. Mounting this once
 * upgrades all of them: the attribute is lifted off the element on hover (so the browser
 * has nothing to draw) and repainted as a themed bubble, then put back when the pointer
 * leaves, keeping the element's accessible name intact.
 */
export function TitleTooltips() {
  const [shown, setShown] = useState<Shown | null>(null);
  const timer = useRef<number | null>(null);
  const held = useRef<{ el: HTMLElement; text: string } | null>(null);

  useEffect(() => {
    const restore = () => {
      if (held.current) {
        held.current.el.setAttribute("title", held.current.text);
        held.current = null;
      }
    };

    const hide = () => {
      if (timer.current !== null) {
        window.clearTimeout(timer.current);
        timer.current = null;
      }
      restore();
      setShown(null);
    };

    const arm = (target: EventTarget | null) => {
      if (!(target instanceof Element)) return;
      const el = target.closest("[title]");
      if (!(el instanceof HTMLElement)) return;
      const text = el.getAttribute("title") ?? "";
      if (!text.trim()) return;
      if (held.current?.el === el) return;
      hide();
      el.removeAttribute("title");
      held.current = { el, text };
      timer.current = window.setTimeout(() => {
        timer.current = null;
        const rect = el.getBoundingClientRect();
        const above = rect.bottom + 120 > window.innerHeight;
        setShown({
          text,
          top: above ? rect.top - GAP_PX : rect.bottom + GAP_PX,
          left: rect.left + rect.width / 2,
          above,
        });
      }, DELAY_MS);
    };

    const onOver = (e: MouseEvent) => arm(e.target);
    const onOut = (e: MouseEvent) => {
      if (!held.current) return;
      const to = e.relatedTarget;
      if (to instanceof Node && held.current.el.contains(to)) return;
      hide();
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") hide();
    };

    document.addEventListener("mouseover", onOver, true);
    document.addEventListener("mouseout", onOut, true);
    document.addEventListener("keydown", onKey, true);
    window.addEventListener("scroll", hide, true);
    window.addEventListener("blur", hide);
    return () => {
      document.removeEventListener("mouseover", onOver, true);
      document.removeEventListener("mouseout", onOut, true);
      document.removeEventListener("keydown", onKey, true);
      window.removeEventListener("scroll", hide, true);
      window.removeEventListener("blur", hide);
      hide();
    };
  }, []);

  if (!shown) return null;
  return createPortal(
    <div
      className={`app-tooltip${shown.above ? " app-tooltip-above" : ""}`}
      role="tooltip"
      data-testid="app-tooltip"
      style={{ top: shown.top, left: shown.left }}
    >
      {shown.text}
    </div>,
    document.body,
  );
}
