// Copyright (c) 2026 Kenneth Stott
// Canary: 55ecea78-4131-4bb4-8a93-71e1bdacea84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { TextInput, PasswordInput } from "@mantine/core";
import { useTranslation } from "react-i18next";

/** Reusable auth fields for username/password */
export function AuthUserPass({
  authFields,
  setAuthFields,
}: {
  authFields: Record<string, string>;
  setAuthFields: (f: Record<string, string>) => void;
}) {
  const { t } = useTranslation();
  return (
    <>
      <TextInput
        label={t("authUserPass.username")}
        required
        value={authFields.username ?? ""}
        onChange={(e) => setAuthFields({ ...authFields, username: e.currentTarget.value })}
      />
      <PasswordInput
        label={t("authUserPass.password")}
        required
        value={authFields.password ?? ""}
        onChange={(e) => setAuthFields({ ...authFields, password: e.currentTarget.value })}
      />
    </>
  );
}
