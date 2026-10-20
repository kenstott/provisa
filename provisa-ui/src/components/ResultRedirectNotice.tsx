// Copyright (c) 2026 Kenneth Stott
// Canary: 3068bc16-81d2-4f47-8f02-f9f54f46a399
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Alert, Anchor, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { ResultRedirect } from "../types/resultRedirect";

/**
 * REQ-1194 / REQ-1224: a result the server landed in the results store, said as that: how many rows
 * it holds and a link to it. Shown where the rows would be; the rows are there, not here.
 */
export function ResultRedirectNotice({
  redirect,
  className,
}: {
  redirect: ResultRedirect;
  className?: string;
}) {
  const { t, i18n } = useTranslation();
  return (
    <Alert color="blue" data-testid="result-redirected" className={className}>
      <Text size="sm">
        {t("resultRedirect.landed", {
          count: redirect.rowCount,
          rows: redirect.rowCount.toLocaleString(i18n.language),
        })}{" "}
        <Anchor href={redirect.url} target="_blank" rel="noreferrer" size="sm">
          {t("resultRedirect.download")}
        </Anchor>
      </Text>
    </Alert>
  );
}
