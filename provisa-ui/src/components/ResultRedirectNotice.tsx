// Copyright (c) 2026 Kenneth Stott
// Canary: 3068bc16-81d2-4f47-8f02-f9f54f46a399
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, type MouseEvent } from "react";
import { Alert, Anchor, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { ResultRedirect } from "../types/resultRedirect";

const API_BASE = import.meta.env.VITE_API_BASE || "";

/**
 * REQ-1194 / REQ-1224: a result the server landed in the results store, said as that: how many rows
 * it holds and a link to it. Shown where the rows would be; the rows are there, not here.
 *
 * A file this server keeps (REQ-1192) is served only to the user who asked for it, and a link the
 * browser follows carries no Authorization header, so it is fetched through the app's fetch and
 * saved. An object store's presigned URL (absolute) carries its own signature: the browser follows it.
 */
export function ResultRedirectNotice({
  redirect,
  className,
}: {
  redirect: ResultRedirect;
  className?: string;
}) {
  const { t, i18n } = useTranslation();
  const [failure, setFailure] = useState<string | null>(null);

  const fetchFile = async (event: MouseEvent<HTMLAnchorElement>) => {
    event.preventDefault();
    setFailure(null);
    try {
      const resp = await fetch(`${API_BASE}${redirect.url}`);
      if (!resp.ok) {
        setFailure(String(resp.status));
        return;
      }
      const objectUrl = URL.createObjectURL(await resp.blob());
      const save = document.createElement("a");
      save.href = objectUrl;
      save.download = redirect.url.split("/").pop() ?? "";
      save.click();
      URL.revokeObjectURL(objectUrl);
    } catch (err) {
      setFailure(err instanceof Error ? err.message : String(err));
    }
  };

  return (
    <Alert color="blue" data-testid="result-redirected" className={className}>
      <Text size="sm">
        {t("resultRedirect.landed", {
          count: redirect.rowCount,
          rows: redirect.rowCount.toLocaleString(i18n.language),
        })}{" "}
        <Anchor
          href={redirect.url}
          target="_blank"
          rel="noreferrer"
          size="sm"
          onClick={redirect.url.startsWith("/") ? fetchFile : undefined}
        >
          {t("resultRedirect.download")}
        </Anchor>
      </Text>
      {failure !== null && (
        <Text size="sm" c="red" data-testid="result-redirect-failed">
          {t("resultRedirect.downloadFailed", { reason: failure })}
        </Text>
      )}
    </Alert>
  );
}
