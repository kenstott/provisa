// Copyright (c) 2026 Kenneth Stott
// Canary: d99051fb-4f30-4ce2-813e-dba3b4fc72dd
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The "Read the documentation" link of a (?) help or a page command (docsTarget.ts).

import { Link } from "react-router-dom";
import { Anchor } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { docsHref, type DocsTarget } from "./docsTarget";

/** The "Read the documentation" link that ends a (?) help or a command's description. */
export function DocsLink({ target, testId }: { target: DocsTarget; testId: string }) {
  const { t } = useTranslation();
  return (
    <Anchor component={Link} to={docsHref(target)} size="xs" data-testid={testId}>
      {t("pageHelp.docsLink")}
    </Anchor>
  );
}
