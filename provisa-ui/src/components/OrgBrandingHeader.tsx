// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1486: the org's mark above the sign-in form.
//
// Renders only what the org actually set. An org that set nothing renders nothing at all, so the
// product's own sign-in page is unchanged — branding adds a header, it does not replace one.

import { Group, Image, Stack, Text } from "@mantine/core";
import type { PublicBranding } from "../api/branding";
import { publicLogoUrl } from "../api/branding";

export function OrgBrandingHeader({ branding }: { branding: PublicBranding | null }) {
  if (!branding?.org_id) return null;
  const name = branding.branding.display_name ?? branding.name;
  const welcome = branding.branding.welcome_message;
  if (!branding.logo && !name && !welcome) return null;
  return (
    <Stack
      gap="xs"
      mb="lg"
      data-testid="org-branding-header"
      style={{
        borderLeft: "4px solid var(--org-accent, var(--primary))",
        paddingLeft: "var(--mantine-spacing-md)",
      }}
    >
      <Group gap="sm" align="center">
        {branding.logo && (
          <Image
            src={publicLogoUrl(branding.org_id)}
            alt={name ?? branding.org_id}
            h={40}
            w="auto"
            fit="contain"
            data-testid="org-branding-logo"
          />
        )}
        {name && (
          <Text fw={600} size="lg" data-testid="org-branding-name">
            {name}
          </Text>
        )}
      </Group>
      {welcome && (
        <Text c="dimmed" size="sm" data-testid="org-branding-welcome">
          {welcome}
        </Text>
      )}
    </Stack>
  );
}
