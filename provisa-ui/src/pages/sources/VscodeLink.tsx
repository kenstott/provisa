// Copyright (c) 2026 Kenneth Stott
// Canary: b3a5d0e4-1f10-476c-b2d6-977556264a60
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2121: Open in VS Code, on a code source's form: a server directory in the installed VS Code,
// a Git repository at its commit on vscode.dev. In the read view and the edit form alike.

import { Anchor, Input } from "@mantine/core";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { FormField } from "../../components/form/FormLayout";

type Props = { label: string; href: string };

function link(p: Props, t: TFunction) {
  return (
    <Anchor href={p.href} target="_blank" rel="noopener noreferrer" data-testid="code-open-vscode">
      {t("codeSourceFields.openInVscode")}
    </Anchor>
  );
}

function OpenInVscode(p: Props) {
  const { t } = useTranslation();
  return (
    <Input.Wrapper label={p.label}>
      <div>{link(p, t)}</div>
    </Input.Wrapper>
  );
}
OpenInVscode.readValue = (p: Record<string, unknown>, t: TFunction) =>
  link(p as unknown as Props, t);

export function VscodeLink({ href }: { href: string }) {
  const { t } = useTranslation();
  return (
    <FormField size="medium">
      <OpenInVscode label={t("codeSourceFields.editor")} href={href} />
    </FormField>
  );
}
