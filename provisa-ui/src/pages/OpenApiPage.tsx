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

import { useRef, useCallback, useEffect, useState } from "react";
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
  const { role, loading: authLoading } = useAuth();
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

  // Loaded via fetch() + srcDoc rather than <iframe src>: a plain browser navigation to `src`
  // can't carry the bearer token installAuthFetch attaches to fetch() calls, so the request hits
  // the auth middleware with no Authorization header. srcDoc's content still resolves relative
  // URLs (e.g. Swagger UI's own openapi.json fetch) against this page's origin.
  const [html, setHtml] = useState<string | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  useEffect(() => {
    // authLoading gates re-fetch when AuthContext is mid-refresh (e.g. a token
    // renewal in flight) so this doesn't race an about-to-change provisa_token.
    if (authLoading) return;
    let cancelled = false;
    setHtml(null);
    setLoadError(null);
    fetch(src)
      .then((res) => {
        if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
        return res.text();
      })
      .then((text) => {
        if (!cancelled) setHtml(text);
      })
      .catch((err: Error) => {
        if (!cancelled) setLoadError(err.message);
      });
    return () => {
      cancelled = true;
    };
  }, [src, authLoading]);

  // Swagger UI mounts/expands/re-renders each step asynchronously (React tree inside the
  // iframe); polling for the actual DOM state avoids racing fixed setTimeout delays that fire
  // before the previous step's render has landed (execute clicked before param inputs exist).
  const waitFor = useCallback(
    (predicate: () => boolean, timeoutMs = 2000, intervalMs = 50): Promise<boolean> =>
      new Promise((resolve) => {
        const start = Date.now();
        const tick = () => {
          if (predicate()) {
            resolve(true);
            return;
          }
          if (Date.now() - start >= timeoutMs) {
            resolve(false);
            return;
          }
          setTimeout(tick, intervalMs);
        };
        tick();
      }),
    [],
  );

  const handleIframeLoad = useCallback(() => {
    if (!autoRun || !openApiUrl) return;

    const parts = openApiUrl.trim().split(/\s+/);
    if (parts.length < 2) return;
    const method = parts[0].toLowerCase();
    const [fullPath, fullQuery = ""] = parts[1].split("?");
    const navParams = new URLSearchParams(fullQuery);

    void (async () => {
      const doc = iframeRef.current?.contentDocument;
      if (!doc) return;

      // data-path is relative to the server base (e.g. /pet-store/inquiries),
      // but NL gives the full path (/data/rest/pet-store/inquiries) — use endsWith.
      // REQ-1359: fullPath must have its querystring stripped first, or it never matches.
      const findBlock = () =>
        Array.from(doc.querySelectorAll(".opblock")).find((b) => {
          const m = b.querySelector(".opblock-summary-method")?.textContent?.toLowerCase();
          const dataPath = b.querySelector("[data-path]")?.getAttribute("data-path") ?? "";
          return m === method && fullPath.endsWith(dataPath);
        }) as HTMLElement | undefined;

      await waitFor(() => !!findBlock());
      const block = findBlock();
      if (!block) return;

      block.scrollIntoView({ behavior: "smooth", block: "start" });

      // Clicking .opblock-summary-control expands AND auto-activates try-it-out
      if (!block.classList.contains("is-open")) {
        (block.querySelector(".opblock-summary-control") as HTMLElement)?.click();
      }

      const paramKeys = [...navParams.keys()];
      await waitFor(() =>
        paramKeys.every((key) => !!block.querySelector(`tr[data-param-name="${key}"] input, tr[data-param-name="${key}"] select, tr[data-param-name="${key}"] textarea`)),
      );

      // REQ-1359: populate the try-it-out param inputs (e.g. aggregate/groupBy) from the
      // NL-forwarded querystring before executing, so the replicated call matches the NL query.
      navParams.forEach((value, key) => {
        const row = block.querySelector(`tr[data-param-name="${key}"]`);
        const input = row?.querySelector("input, select, textarea") as
          | HTMLInputElement
          | HTMLSelectElement
          | HTMLTextAreaElement
          | null;
        if (!input) return;
        const setter = Object.getOwnPropertyDescriptor(
          input.tagName === "SELECT" ? window.HTMLSelectElement.prototype : window.HTMLInputElement.prototype,
          "value",
        )?.set;
        setter?.call(input, value);
        input.dispatchEvent(new Event("input", { bubbles: true }));
        input.dispatchEvent(new Event("change", { bubbles: true }));
      });

      // The native-setter + dispatchEvent hack updates the DOM input immediately, but the actual
      // request Swagger UI builds on execute is driven by its own Redux store, which only picks
      // up the change a render cycle later. The live curl preview reflects that same store, so
      // wait for it to show every param before executing — otherwise execute can fire against
      // the pre-update state even though the inputs already display the right values.
      await waitFor(() => {
        const curlText = block.querySelector(".curl-command, .opblock-body pre.curl, .curl")?.textContent ?? "";
        return [...navParams.entries()].every(([key, value]) => curlText.includes(`${key}=${encodeURIComponent(value)}`));
      });

      await waitFor(() => !!block.querySelector(".btn.execute.opblock-control__btn"));
      (block.querySelector(".btn.execute.opblock-control__btn") as HTMLElement | null)?.click();

      await waitFor(() => !!block.querySelector(".responses-wrapper .live-responses-table, .responses-wrapper .microlight"));
      (block.querySelector(".responses-wrapper") as HTMLElement | null)?.scrollIntoView({
        behavior: "smooth",
        block: "start",
      });
    })();
  }, [autoRun, openApiUrl, waitFor]);

  return (
    <div className="openapi-page page">
      {loadError && (
        <p className="openapi-load-error">{t("openApiPage.loadError", { error: loadError })}</p>
      )}
      {html !== null && (
        <iframe
          ref={iframeRef}
          key={`${roleId}:${domainsParam}:${theme}`}
          srcDoc={html}
          className="openapi-frame"
          title={t("openApiPage.frameTitle")}
          sandbox="allow-scripts allow-same-origin allow-forms allow-downloads allow-popups"
          onLoad={handleIframeLoad}
        />
      )}
    </div>
  );
}
