// Copyright (c) 2026 Kenneth Stott
// Canary: 9b41d6e0-2f7c-4a88-b315-c0e2d97a46f1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A term's To review section: one chip per finding, each opening to what was found, in the reader's
// language (serverErrors, by its code), and the acts that fix it. A finding whose fix is not
// offered here says what was found alone.

import { useTranslation } from "react-i18next";
import type { GlossaryFinding } from "../../api/glossary";
import { ToReview, type FindingFix } from "../lifecycle/ToReview";

export type { FindingFix };

export function TermToReview({
  findings,
  fixesOf,
}: {
  findings: GlossaryFinding[];
  /** The acts that fix a finding, where the caller may take them. */
  fixesOf: (finding: GlossaryFinding) => FindingFix[];
}) {
  const { t } = useTranslation();
  return (
    <ToReview
      title={t("glossaryTab.toReview")}
      findings={findings}
      chipLabel={(f) => t(`glossaryTab.finding_${f.kind}`)}
      fixesOf={fixesOf}
      testId="glossary"
    />
  );
}
