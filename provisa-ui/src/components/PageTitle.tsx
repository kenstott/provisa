// Copyright (c) 2026 Kenneth Stott
// Canary: 3d1a90c7-6b24-4f52-9a0e-8c5b7d21e4af
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The one page-title pattern: a lucide icon before the title text. Every top-level page heading
// renders through this so the pages read as one set.

import type { ReactNode } from "react";
import type { LucideIcon } from "lucide-react";
import { Group, Title } from "@mantine/core";
import type { TitleOrder } from "@mantine/core";

interface PageTitleProps {
  icon: LucideIcon;
  order?: TitleOrder;
  mb?: string | number;
  children: ReactNode;
}

export function PageTitle({ icon: Icon, order = 2, mb, children }: PageTitleProps) {
  return (
    <Group gap="xs" align="center" wrap="nowrap" data-testid="page-title">
      <Icon size={22} aria-hidden="true" style={{ display: "block", flexShrink: 0 }} />
      {/* mb=0 drops the `.page h2` bottom margin, which would push the text off the icon's centre. */}
      <Title order={order} mb={mb ?? 0} lh={1}>
        {children}
      </Title>
    </Group>
  );
}
