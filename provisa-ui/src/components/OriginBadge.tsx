// Copyright (c) 2026 Kenneth Stott
// Canary: 0eb0aa65-e667-4963-947e-3598bcf074d5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Badge, Tooltip } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { Origin } from "../types/admin";

/** REQ-1919: marks a source, domain, role or table a config file declares. It can be edited and
 *  deleted here like any other; the next load of the config re-applies what the file says, which
 *  the tooltip tells the person about before they change it. Renders nothing for the rest. */
export function OriginBadge({ origin }: { origin: Origin }) {
  const { t } = useTranslation();
  if (origin !== "config") return null;
  return (
    <Tooltip label={t("originBadge.configHint")} multiline w={280} withArrow>
      <Badge size="xs" variant="light" color="gray" data-testid="origin-badge">
        {t("originBadge.config")}
      </Badge>
    </Tooltip>
  );
}
