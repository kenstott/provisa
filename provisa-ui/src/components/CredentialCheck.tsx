// Copyright (c) 2026 Kenneth Stott
// Canary: 551f0cb0-428f-4fde-9cee-f91ea9962fdf
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useTranslation } from "react-i18next";
import { Center, Loader, Stack, Text } from "@mantine/core";
import { ShieldCheck } from "lucide-react";

/** REQ-1430: what a page shows while the identity bootstrap is still in flight. */
export function CredentialCheck() {
  const { t } = useTranslation();
  return (
    <Center mih="60vh" data-testid="capability-gate-checking">
      <Stack align="center" gap="sm">
        <div style={{ position: "relative", width: 56, height: 56 }}>
          <Loader size={56} />
          <ShieldCheck
            size={22}
            aria-hidden
            style={{
              position: "absolute",
              inset: 0,
              margin: "auto",
              color: "var(--mantine-color-dimmed)",
            }}
          />
        </div>
        <Text size="sm" c="dimmed">
          {t("capabilityGate.checking")}
        </Text>
      </Stack>
    </Center>
  );
}
