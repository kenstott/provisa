// Copyright (c) 2026 Kenneth Stott
// Canary: d99051fb-4f30-4ce2-813e-dba3b4fc72dd
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Maintainer 2026-11-28: every link to the documentation opens in a new browser tab, so the page it
// was followed from (a dialog, a form, a card) stays as it was. DocsLink is the one component that
// draws such a link; docsLinkGuard.test.ts fails a docs link drawn without target="_blank".
// The docs page's own navigation between docs pages (pages/DocsPage.tsx) stays in its tab: it is
// inside the docs already.

import type { ComponentPropsWithoutRef, ReactNode } from "react";
import { Anchor, type MantineSize } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { docsHref, isDocsHref, type DocsTarget } from "./docsTarget";

type DocsLinkProps = {
  testId: string;
  /** The link's text; "Read the documentation" when not given. */
  children?: ReactNode;
  size?: MantineSize;
} &
  /** A page (and section) of the docs, opened in the app's Docs page. */
  (
    | { target: DocsTarget; href?: never }
    /** A docs address that is not a DocsTarget: the static docs site, a link in Polly's text. */
    | { href: string; target?: never }
  );

/** A link to the documentation, opened in a new tab. */
export function DocsLink(props: DocsLinkProps) {
  const { t } = useTranslation();
  return (
    <Anchor
      href={props.target === undefined ? props.href : docsHref(props.target)}
      target="_blank"
      rel="noopener noreferrer"
      size={props.size ?? "xs"}
      data-testid={props.testId}
    >
      {props.children ?? t("pageHelp.docsLink")}
    </Anchor>
  );
}

/** The link of rendered Markdown (Polly's answers): a docs address is a DocsLink, opened in a new
 *  tab; any other address is the plain link Markdown draws. */
export function MarkdownLink({
  node: _node,
  href,
  children,
  ...rest
}: ComponentPropsWithoutRef<"a"> & { node?: unknown }) {
  if (href !== undefined && isDocsHref(href)) {
    return (
      <DocsLink href={href} size="sm" testId="markdown-docs-link">
        {children}
      </DocsLink>
    );
  }
  return (
    <a href={href} {...rest}>
      {children}
    </a>
  );
}
