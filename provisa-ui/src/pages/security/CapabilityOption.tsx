// Copyright (c) 2026 Kenneth Stott
// Canary: 5397601b-c80d-4309-8a4b-c3a8c683e829
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useTranslation } from "react-i18next";
import { Checkbox, Group } from "@mantine/core";
import { Tip } from "../../components/Tip";
import type { Capability } from "../../types/auth";

/** One capability in the role form's dropdown: a tick beside its name, with what it allows as the
 *  hint, or why it cannot be chosen when no parent of the role holds it. */
export function CapabilityOption({
  cap,
  checked,
  offered,
}: {
  cap: Capability;
  checked: boolean;
  offered: boolean;
}) {
  const { t } = useTranslation();
  return (
    <Tip text={offered ? t(`capabilityHelp.${cap}`) : t("capabilityHelp.notHeldByParent")}>
      {/* A disabled option takes no pointer events, so the hint is on what holds it. */}
      <span data-testid={`capability-tick-${cap}`}>
        {/* The dropdown option takes the click and carries the state for assistive technology; the
            indicator is the tick drawn beside the name. */}
        <Group gap="xs" wrap="nowrap">
          <Checkbox.Indicator
            data-testid={`capability-box-${cap}`}
            checked={checked}
            disabled={!offered}
            size="sm"
          />
          <span>{cap}</span>
        </Group>
      </span>
    </Tip>
  );
}
