// Copyright (c) 2026 Kenneth Stott
// Canary: 8c2d9a41-2b7e-4f6d-8a35-5e0f7d9c1b64
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect } from "react";
import { useTranslation } from "react-i18next";
import { notifications } from "@mantine/notifications";
import { listenForStorageNotice } from "../lib/browserStorage";

/** Says once when the browser would not keep something (lib/browserStorage). */
export function StorageNotice() {
  const { t } = useTranslation();
  useEffect(
    () =>
      listenForStorageNotice((kind) =>
        notifications.show({
          color: kind === "session" ? "red" : "yellow",
          message: t(`storageNotice.${kind}`),
        }),
      ),
    [t],
  );
  return null;
}
