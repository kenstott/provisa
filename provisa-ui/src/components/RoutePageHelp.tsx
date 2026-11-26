// Copyright (c) 2026 Kenneth Stott
// Canary: 6e98a971-8a85-413b-aa92-ce2717734fd8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useTranslation } from "react-i18next";
import { useLocation } from "react-router-dom";
import { PageHelp } from "./PageHelp";
import { PAGE_HELP, pageHelpDocsHref, type PageHelpPlacement } from "./pageHelpRegistry";

/**
 * REQ-2277 rule 8: the current page's (?) help, read from the registry by its route. Each header
 * that can carry it names its placement, and the button is drawn only by the one the registry
 * names for the route, so a page never shows two.
 *
 * A route with no entry draws nothing. That is the rule's own design, not a default: rule 8 makes a
 * page without help content fail a guard test (pageHelpGuard.test.ts), which checks every route
 * of the app's route table against the registry; what is left without an entry at run time is a
 * header drawn off any page route, such as a unit test's bare router.
 */
export function RoutePageHelp({ placement }: { placement: PageHelpPlacement }) {
  const { t } = useTranslation();
  const { pathname } = useLocation();
  const entry = PAGE_HELP[pathname];
  if (entry === undefined || entry.placement !== placement) return null;
  return (
    <PageHelp
      title={t("pageHelp.title")}
      paragraphs={[t(entry.purpose), ...(entry.more ?? []).map((key) => t(key))]}
      ariaLabel={t("pageHelp.title")}
      testId="page-help"
      docs={{ href: pageHelpDocsHref(entry), label: t("pageHelp.docsLink") }}
    />
  );
}
