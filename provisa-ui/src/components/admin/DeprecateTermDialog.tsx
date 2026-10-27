// Copyright (c) 2026 Kenneth Stott
// Canary: 4d8b1e57-a2c3-4f69-9e0d-73b5c2a1f846
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Deprecate a live term (REQ-2144): a reason, and an optional planned retirement, today or later.
// The term stays in service until then, when the scheduler retires it.

import { useTranslation } from "react-i18next";
import { deprecateGlossaryTerm, type GlossaryTermDetail } from "../../api/glossary";
import { DeprecateDialog } from "../lifecycle/DeprecateDialog";

/** Drawn while it is open: each opening starts from an empty form. */
export function DeprecateTermDialog({
  term,
  onClose,
  onDeprecated,
}: {
  term: GlossaryTermDetail;
  onClose: () => void;
  onDeprecated: () => void;
}) {
  const { t } = useTranslation();
  return (
    <DeprecateDialog
      text={{
        title: t("glossaryTab.deprecateTitle", { term: term.name }),
        reason: t("glossaryTab.retireReason"),
        retireOn: t("glossaryTab.retireOnLabel"),
        retireOnHint: t("glossaryTab.retireOnHint"),
        submit: t("glossaryTab.deprecate"),
      }}
      deprecate={(reason, retireOn) =>
        deprecateGlossaryTerm(term.id, { reason, retire_on: retireOn })
      }
      onClose={onClose}
      onDeprecated={onDeprecated}
      testId="glossary"
    />
  );
}
