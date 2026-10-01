// Copyright (c) 2026 Kenneth Stott
// Canary: 8e2a4c71-3f95-4b06-9d18-5a7c0e3b2f64
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1913: the platform endpoints answer 403 to anyone but a platform administrator. A surface
 * that reads one shows this state instead of an error alert or an empty page.
 */

import { Alert } from "@mantine/core";
import { useTranslation } from "react-i18next";

export function PlatformRequired() {
  const { t } = useTranslation();
  return (
    <Alert color="gray" data-testid="platform-required">
      {t("adminPage.setting.forbidden")}
    </Alert>
  );
}
