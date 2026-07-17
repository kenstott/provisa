// Copyright (c) 2026 Kenneth Stott
// Canary: 6130d66d-d4ca-4e10-9dba-dc5e82954d7c
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useRef, useCallback } from "react";
import { useLocation } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { useMantineColorScheme } from "@mantine/core";
import { useAuth } from "../context/AuthContext";
import { useDomainFilter } from "../context/DomainFilterContext";
import "./OpenApiPage.css";

export function OpenApiPage() {
  const { t } = useTranslation();
  const location = useLocation();
  const { colorScheme } = useMantineColorScheme();
  const { role } = useAuth();
  const { checkedDomains } = useDomainFilter();
  const roleId = role?.id ?? "";
  const domainsParam = checkedDomains.size > 0 ? [...checkedDomains].join(",") : "";
  const theme = colorScheme === "light" ? "light" : "dark";
  const params = new URLSearchParams();
  if (roleId) params.set("role", roleId);
  if (domainsParam) params.set("domains", domainsParam);
  params.set("theme", theme);
  const query = params.toString();
  const src = `/data/rest/docs${query ? "?" + query : ""}`;

  const navState = location.state as { openApiUrl?: string; autoRun?: boolean } | null;
  const openApiUrl = navState?.openApiUrl ?? "";
  const autoRun = navState?.autoRun === true;

  const iframeRef = useRef<HTMLIFrameElement>(null);

  const handleIframeLoad = useCallback(() => {
    if (!autoRun || !openApiUrl) return;

    const parts = openApiUrl.trim().split(/\s+/);
    if (parts.length < 2) return;
    const method = parts[0].toLowerCase();
    const fullPath = parts[1];

    // Swagger UI renders asynchronously after the iframe load event
    setTimeout(() => {
      const doc = iframeRef.current?.contentDocument;
      if (!doc) return;

      // data-path is relative to the server base (e.g. /pet-store/inquiries),
      // but NL gives the full path (/data/rest/pet-store/inquiries) — use endsWith
      const blocks = Array.from(doc.querySelectorAll(".opblock"));
      const block = blocks.find((b) => {
        const m = b.querySelector(".opblock-summary-method")?.textContent?.toLowerCase();
        const dataPath = b.querySelector("[data-path]")?.getAttribute("data-path") ?? "";
        return m === method && fullPath.endsWith(dataPath);
      });
      if (!block) return;

      block.scrollIntoView({ behavior: "smooth", block: "start" });

      // Clicking .opblock-summary-control expands AND auto-activates try-it-out
      if (!block.classList.contains("is-open")) {
        (block.querySelector(".opblock-summary-control") as HTMLElement)?.click();
      }

      setTimeout(() => {
        (block.querySelector(".btn.execute.opblock-control__btn") as HTMLElement)?.click();
      }, 500);
    }, 1200);
  }, [autoRun, openApiUrl]);

  return (
    <div className="openapi-page page">
      <iframe
        ref={iframeRef}
        key={`${roleId}:${domainsParam}:${theme}`}
        src={src}
        className="openapi-frame"
        title={t("openApiPage.frameTitle")}
        sandbox="allow-scripts allow-same-origin allow-forms allow-downloads allow-popups"
        onLoad={handleIframeLoad}
      />
    </div>
  );
}
