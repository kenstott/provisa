// Copyright (c) 2026 Kenneth Stott
// Canary: 699ec72b-1d43-4703-969c-d2bd3fbc8102
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A list entry's detail held to three lines with an ellipsis, opened in place by clicking it (or
// Enter/Space on it) and closed the same way. Text that fits in three lines is shown as it is, with
// nothing to toggle.

import { useLayoutEffect, useRef, useState, type CSSProperties, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { UnstyledButton } from "@mantine/core";

const LINES = 3;

const CLAMPED: CSSProperties = {
  display: "-webkit-box",
  WebkitBoxOrient: "vertical",
  WebkitLineClamp: LINES,
  overflow: "hidden",
};

export function ClampedDetail({ children, testId }: { children: ReactNode; testId: string }) {
  const { t } = useTranslation();
  const body = useRef<HTMLDivElement>(null);
  const [open, setOpen] = useState(false);
  // Whether the closed detail has more than its three lines: measured while closed, kept while open.
  const [cut, setCut] = useState(false);

  useLayoutEffect(() => {
    const el = body.current;
    if (el === null || open) return;
    const measure = () => setCut(el.scrollHeight > el.clientHeight + 1);
    measure();
    const watch = new ResizeObserver(measure);
    watch.observe(el);
    return () => watch.disconnect();
    // `cut` too: the detail is mounted anew inside the button once it is found cut, and watched there.
  }, [open, cut]);

  const detail = (
    <div ref={body} style={open ? undefined : CLAMPED} data-testid={`${testId}-text`}>
      {children}
    </div>
  );
  if (!cut) return detail;
  return (
    <UnstyledButton
      onClick={() => setOpen((o) => !o)}
      aria-expanded={open}
      aria-label={open ? t("dataProductCore.detailCollapse") : t("dataProductCore.detailExpand")}
      style={{ display: "block", width: "100%", textAlign: "start" }}
      data-testid={testId}
    >
      {detail}
    </UnstyledButton>
  );
}
