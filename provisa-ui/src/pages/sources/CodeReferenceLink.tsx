// Copyright (c) 2026 Kenneth Stott
// Canary: 82adfcab-5950-42ec-9de9-11e43e7d6e7e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1984 (amended 2026-11-24): a code source's dialog links the reference of the language chosen,
// opened in the in-app Docs page at that language's section. In the read view and the edit form alike.

import { Input } from "@mantine/core";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { FormField } from "../../components/form/FormLayout";
import { SOURCE_TYPES } from "./constants";
import { DocsLink } from "../../components/DocsLink";
import { codeReferenceTarget } from "./codeSource";

type Props = { label: string; type: string };

function languageOf(type: string): string {
  const entry = SOURCE_TYPES.find((s) => s.value === type);
  if (!entry) throw new Error(`${type} is not a source type`);
  return entry.label;
}

function link(p: Props, t: TFunction) {
  return (
    // A new tab, as every docs link: the link sits inside the source dialog, and following it in
    // place would close it.
    <DocsLink target={codeReferenceTarget(p.type)} size="md" testId="code-reference-link">
      {t("codeSourceFields.referenceLink", { language: languageOf(p.type) })}
    </DocsLink>
  );
}

function OpenReference(p: Props) {
  const { t } = useTranslation();
  return (
    <Input.Wrapper label={p.label}>
      <div>{link(p, t)}</div>
    </Input.Wrapper>
  );
}
OpenReference.readValue = (p: Record<string, unknown>, t: TFunction) =>
  link(p as unknown as Props, t);

export function CodeReferenceLink({ type }: { type: string }) {
  const { t } = useTranslation();
  return (
    <FormField size="medium">
      <OpenReference label={t("codeSourceFields.reference")} type={type} />
    </FormField>
  );
}
