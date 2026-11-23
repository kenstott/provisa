// Copyright (c) 2026 Kenneth Stott
// Canary: 84b30a98-61de-414d-9263-0ed3bed0b194
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// What names a registered table: its source and, where asked, its domain (REQ-1531, REQ-2234). Both
// are required (REQ-1968, amended 2026-11-22): each carries its marker from the start and is marked
// inline when the form's action finds it empty.

import { Input } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { FormField, RequiredMark } from "../../components/form/FormLayout";
import type { RequiredFields } from "../../components/form/useRequiredFields";
import { DERIVED_SOURCE_ID, type Source } from "../../types/admin";

export function SourceAndDomainFields({
  sources,
  checkedDomains,
  sourceId,
  onSourceChange,
  domainAsked,
  domainId,
  onDomainChange,
  offeredDomains,
  required,
}: {
  sources: Source[];
  /** The domains of the domain filter: a source limited to other domains is not offered. */
  checkedDomains: Set<string>;
  sourceId: string;
  onSourceChange: (sourceId: string) => void;
  /** With domains in use and more than one offered (or an authored table), the domain is asked. */
  domainAsked: boolean;
  domainId: string;
  onDomainChange: (domainId: string) => void;
  offeredDomains: string[];
  required: RequiredFields<"source" | "domain">;
}) {
  const { t } = useTranslation();
  return (
    <>
      <FormField size="medium">
        <label>
          {t("registerTableForm.sourceLabel")}
          <RequiredMark />
          <select
            className="form-select"
            value={sourceId}
            onChange={(e) => onSourceChange(e.target.value)}
            {...required.native("source")}
            data-testid="register-table-source-select"
          >
            <option value="">{t("registerTableForm.sourcePlaceholder")}</option>
            {sources
              .filter(
                (s) =>
                  // Views come from + View, and a system source is Provisa's own.
                  s.id !== DERIVED_SOURCE_ID &&
                  !s.isSystem &&
                  (s.allowedDomains.length === 0 ||
                    s.allowedDomains.some((d) => checkedDomains.has(d))),
              )
              .map((s) => (
                <option key={s.id} value={s.id}>
                  {s.id}
                </option>
              ))}
          </select>
          <Input.Error>{required.errorOf("source")}</Input.Error>
        </label>
      </FormField>
      {domainAsked && (
        <FormField size="medium">
          <label>
            {t("registerTableForm.domainLabel")}
            <RequiredMark />
            <select
              className="form-select"
              value={domainId}
              onChange={(e) => onDomainChange(e.target.value)}
              {...required.native("domain")}
              data-testid="register-table-domain-select"
            >
              <option value="">{t("registerTableForm.domainPlaceholder")}</option>
              {offeredDomains.map((d) => (
                <option key={d} value={d}>
                  {d}
                </option>
              ))}
            </select>
            <Input.Error>{required.errorOf("domain")}</Input.Error>
          </label>
        </FormField>
      )}
    </>
  );
}
