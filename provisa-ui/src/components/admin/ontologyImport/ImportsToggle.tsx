// Copyright (c) 2026 Kenneth Stott
// Canary: e2a7c5d8-41f9-4b06-8d3e-96b1f0c47a25
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2237: the Ontology page's toolbar toggle that shows the ontology imports in place of the
// term list.

import { useTranslation } from "react-i18next";
import { ActionIcon } from "@mantine/core";
import { FileUp } from "lucide-react";
import { Tip } from "../../Tip";

export function ImportsToggle({
  open,
  onChange,
}: {
  open: boolean;
  onChange: (open: boolean) => void;
}) {
  const { t } = useTranslation();
  return (
    <Tip text={t("ontologyImport.importsToggle")} named>
      <ActionIcon
        size="input-sm"
        variant={open ? "light" : "subtle"}
        aria-label={t("ontologyImport.importsToggle")}
        aria-pressed={open}
        onClick={() => onChange(!open)}
        data-testid="glossary-imports-btn"
      >
        <FileUp size={16} aria-hidden />
      </ActionIcon>
    </Tip>
  );
}
