// Copyright (c) 2026 Kenneth Stott
// Canary: 30db3174-2e60-49a5-97be-929e0dd90f9b
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2145: pii on a Property term, a toggle in the term panel's head. The pii tag requires a
// reason (its registry's reason policy), so turning it on opens the reason here and marks the
// term with it; turning it off unmarks the term. A Class or a Concept means no column, and a
// draft without a definition governs nothing yet: the toggle is then off and says why.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Popover, TextInput } from "@mantine/core";
import { FingerprintPattern } from "lucide-react";
import { termRole, type GlossaryTermDetail } from "../../api/glossary";
import { useAssignTag, useUnassignTag } from "../../hooks/useTagQueries";
import { serverMessage } from "../../i18n/serverMessage";
import type { MutationResult } from "../../types/admin";
import { FormField, FormFooter, FormLayout } from "../form/FormLayout";
import { useRequiredFields } from "../form/useRequiredFields";
import { PanelToggle } from "../list/PanelToggle";

const PII_TAG_ID = "pii";

export function TermPiiToggle({
  term,
  onChanged,
}: {
  term: GlossaryTermDetail;
  /** The term was marked or unmarked: it is read again. */
  onChanged: () => void;
}) {
  const { t } = useTranslation();
  const { assignTag } = useAssignTag();
  const { unassignTag } = useUnassignTag();
  const [asking, setAsking] = useState(false);
  const [reason, setReason] = useState("");
  const [refusal, setRefusal] = useState<MutationResult | null>(null);
  const on = term.tags.some((g) => g.tag_id === PII_TAG_ID);
  // The reason the pii tag requires: asked for on Save, never by a disabled Save.
  const required = useRequiredFields(on ? {} : { reason: reason.trim() });
  const property = termRole(term) === "property";
  const defined = (term.definition ?? "").trim() !== "";
  const disabled = !property || (!defined && !on);
  const tip = !property
    ? t("glossaryTab.piiPropertyOnly")
    : !defined && !on
      ? t("serverErrors.tag.term_not_live", { term: term.name })
      : on
        ? t("glossaryTab.piiOnTip", { count: term.ref_count })
        : t("glossaryTab.piiOffTip");
  const target = { tagId: PII_TAG_ID, objectType: "term", termId: term.id } as const;

  const settle = (result: MutationResult) => {
    if (!result.success) {
      setRefusal(result);
      return;
    }
    setRefusal(null);
    setAsking(false);
    setReason("");
    onChanged();
  };
  const close = () => {
    setAsking(false);
    setReason("");
    setRefusal(null);
  };

  return (
    <Popover
      opened={asking || (refusal !== null && on)}
      onChange={(opened) => !opened && close()}
      position="bottom-end"
      withinPortal
      shadow="md"
      width={360}
      trapFocus
    >
      <Popover.Target>
        {/* The popover's anchor: the toggle, as the head draws it. */}
        <span style={{ display: "inline-flex" }}>
          <PanelToggle
            label={t("glossaryTab.piiLabel")}
            on={on}
            tip={tip}
            icon={<FingerprintPattern size={14} />}
            color="red"
            disabled={disabled}
            onToggle={() => {
              setRefusal(null);
              if (on) void unassignTag(target).then(settle);
              else setAsking(!asking);
            }}
            testId="glossary-pii-toggle"
          />
        </span>
      </Popover.Target>
      <Popover.Dropdown onClick={(e) => e.stopPropagation()}>
        <div data-testid="glossary-pii-form">
          <FormLayout>
            {!on && (
              <FormField size="long" help={t("glossaryTab.piiHelp")}>
                <TextInput
                  label={t("tagControl.reasonLabel")}
                  {...required.field("reason")}
                  placeholder={t("tagControl.reasonRequired")}
                  value={reason}
                  onChange={(e) => setReason(e.currentTarget.value)}
                  data-testid="glossary-pii-reason"
                />
              </FormField>
            )}
            <FormFooter
              onSave={
                on
                  ? undefined
                  : async () => {
                      if (!required.validate()) return;
                      settle(await assignTag({ ...target, reason: reason.trim() }));
                    }
              }
              onCancel={close}
              saveProps={{ "data-testid": "glossary-pii-apply" }}
              cancelProps={{ "data-testid": "glossary-pii-cancel" }}
              problem={
                refusal && (
                  <span data-testid="glossary-pii-refusal">
                    {serverMessage(refusal, refusal.message)}
                  </span>
                )
              }
            />
          </FormLayout>
        </div>
      </Popover.Dropdown>
    </Popover>
  );
}
