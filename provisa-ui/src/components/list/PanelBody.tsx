// Copyright (c) 2026 Kenneth Stott
// Canary: ca432384-98b4-41d4-bcce-31f0847b4080
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277 rule 7: a detail panel body has ONE vertical rhythm, defined once. The spacing between
// the panel head, a strip (a wizard's steps, a panel's section tabs), a section's title, its
// description, the first field and each field group comes from the `--panel-gap-*` tokens in
// App.css, applied by these parts' own classes; no panel sets its own gaps between them, which a
// guard refuses (panelBodyGuard.test.ts). The head is PanelHeadStyle's; the field rows are
// FormLayout's, whose row gap is the same group token.

import type { HTMLAttributes, ReactNode } from "react";

function classes(...names: (string | undefined)[]): string {
  return names.filter(Boolean).join(" ");
}

/**
 * The body of a detail panel, in a read view, a form and a wizard alike: its parts one under the
 * other, each set apart from the one before by the rhythm (App.css `.panel-body`).
 */
export function PanelBody({ className, children, ...rest }: HTMLAttributes<HTMLDivElement>) {
  return (
    <div {...rest} className={classes("panel-body", className)}>
      {children}
    </div>
  );
}

/** A strip at the top of a body (a wizard's step strip, a panel's section tabs). */
export function PanelStrip({ className, children, ...rest }: HTMLAttributes<HTMLDivElement>) {
  return (
    <div {...rest} className={classes("panel-strip", className)}>
      {children}
    </div>
  );
}

/**
 * A titled part of a body: its title, what it is about under it, then its content, each at the
 * rhythm's spacing. A section's content is laid out as a body's own (fields, groups, lists).
 */
export function PanelSection({
  title,
  description,
  className,
  children,
  ...rest
}: Omit<HTMLAttributes<HTMLElement>, "title"> & {
  title: ReactNode;
  description?: ReactNode;
}) {
  return (
    <section {...rest} className={classes("panel-body", "panel-section", className)}>
      <div className="panel-section__head">
        <div className="panel-section__title">{title}</div>
        {description != null && <div className="panel-section__description">{description}</div>}
      </div>
      {children}
    </section>
  );
}
