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
import { useNavPayload } from "../hooks/useNavPayload";
import { useTranslation } from "react-i18next";
import { useComputedColorScheme } from "@mantine/core";
import { useAuth } from "../context/AuthContext";
import { useDomainFilter } from "../context/DomainFilterContext";
import { useScreenInputsReader } from "../mcp/screenState";
import {
  bodyInput,
  findOperation,
  hasParam,
  parseCall,
  setParam,
  shownOperation,
  typeInto,
} from "./openapi/swaggerDocs";
import { currentDataAddress, pageAtDataAddress } from "../lib/dataAddress";
import "./OpenApiPage.css";

// How long a driven call's response may take: a query on a loaded demo answers in seconds.
const RESPONSE_WAIT_MS = 60000;
// How long the docs may take to show the handed operation. The frame's load fires when its HTML
// is in, before Swagger UI has fetched the role's spec and rendered it: on a demo sandbox that
// took longer than the 2 s the other waits allow, the call was never made, and its tour step
// waited out its time ("taking longer than expected").
const DOCS_WAIT_MS = 60000;

export function OpenApiPage() {
  const { t } = useTranslation();
  // Computed, not the raw setting: the raw value is "auto" when the user follows the system, and
  // "auto" is not "light", so the docs frame rendered dark on a light app.
  const colorScheme = useComputedColorScheme("light");
  const { loading: authLoading, actingRoles } = useAuth();
  const { checkedDomains } = useDomainFilter();
  // REQ-1620: the ACTIVE roles — one, or under "Role: All" the set served as their meta-role.
  const roleId = actingRoles ?? "";
  const domainsParam = checkedDomains.size > 0 ? [...checkedDomains].join(",") : "";
  const theme = colorScheme === "light" ? "light" : "dark";
  const params = new URLSearchParams();
  if (roleId) params.set("role", roleId);
  if (domainsParam) params.set("domains", domainsParam);
  params.set("theme", theme);
  const query = params.toString();
  const src = `/data/rest/docs${query ? "?" + query : ""}`;

  // A call handed to the page (NL "Open in OpenAPI", Polly), whether the page was just opened or
  // already open. Each hand-off has its own sequence number and is driven in the docs once, as soon
  // as the docs frame has loaded: opened and filled in, and executed when autoRun asks.
  const [handoff, setHandoff] = useState<{
    url: string;
    body: unknown;
    autoRun: boolean;
    seq: number;
  } | null>(null);
  // The last hand-off's response is on screen (REQ-2010: a demo tour step waits for it; the
  // response itself is inside the docs frame, out of the page's reach).
  const [answered, setAnswered] = useState(false);
  // Why the last hand-off could not be carried out; null while none has failed.
  const [driveError, setDriveError] = useState<string | null>(null);
  useNavPayload<{ openApiUrl?: string; openApiBody?: unknown; autoRun?: boolean }>((payload) => {
    if (!payload.openApiUrl) return;
    const url = payload.openApiUrl;
    const body = payload.openApiBody;
    const autoRun = payload.autoRun === true;
    setHandoff((prev) => ({ url, body, autoRun, seq: (prev?.seq ?? 0) + 1 }));
    setAnswered(false);
    setDriveError(null);
  });
  // The document the frame has loaded (its key), so a hand-off waits for the current frame.
  const [frameLoaded, setFrameLoaded] = useState<string | null>(null);
  const drivenSeqRef = useRef<number | null>(null);

  const iframeRef = useRef<HTMLIFrameElement>(null);

  // Loaded via fetch() + srcDoc rather than <iframe src>: a plain browser navigation to `src`
  // can't carry the bearer token installAuthFetch attaches to fetch() calls, so the request hits
  // the auth middleware with no Authorization header. srcDoc's content still resolves relative
  // URLs (e.g. Swagger UI's own openapi.json fetch) against this page's origin.
  //
  // The result carries the `src` it came from, so a change of role/domain/theme retires the
  // previous document by comparison at render time. Clearing two state variables at the top of the
  // effect would do the same thing by way of an extra render pass.
  const [loaded, setLoaded] = useState<{
    src: string;
    html: string | null;
    error: string | null;
  } | null>(null);
  useEffect(() => {
    // authLoading gates re-fetch when AuthContext is mid-refresh (e.g. a token
    // renewal in flight) so this doesn't race an about-to-change provisa_token.
    if (authLoading) return;
    let cancelled = false;
    // REQ-1916: on a coordinator-served UI the docs come from, and call, the chosen data node.
    Promise.all([
      fetch(src).then((res) => {
        if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
        return res.text();
      }),
      currentDataAddress(),
    ])
      .then(([text, address]) => {
        if (!cancelled) setLoaded({ src, html: pageAtDataAddress(text, address), error: null });
      })
      .catch((err: Error) => {
        if (!cancelled) setLoaded({ src, html: null, error: err.message });
      });
    return () => {
      cancelled = true;
    };
  }, [src, authLoading]);
  const current = loaded?.src === src ? loaded : null;
  const html = current?.html ?? null;
  const loadError = current?.error ?? null;

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

  const drive = useCallback(
    (openApiUrl: string, body: unknown, autoRun: boolean) => {
      const call = parseCall(openApiUrl);
      if (!call) {
        setDriveError(`"${openApiUrl}" is not a METHOD and a path`);
        return;
      }
      const { method, path, params: navParams } = call;
      const named = `${method.toUpperCase()} ${path}`;

      void (async () => {
        const doc = iframeRef.current?.contentDocument;
        if (!doc) return;

        await waitFor(() => !!findOperation(doc, method, path), DOCS_WAIT_MS);
        const block = findOperation(doc, method, path);
        if (!block) {
          setDriveError(`the docs have no operation ${named}`);
          return;
        }

        block.scrollIntoView({ behavior: "smooth", block: "start" });

        // Clicking .opblock-summary-control expands AND auto-activates try-it-out
        if (!block.classList.contains("is-open")) {
          (block.querySelector(".opblock-summary-control") as HTMLElement)?.click();
        }

        const paramKeys = [...navParams.keys()];
        await waitFor(() => paramKeys.every((key) => hasParam(block, key)));
        // A parameter the operation does not take is refused here: left out, the call would run
        // wider than the one handed over.
        const unknown = paramKeys.filter((key) => !hasParam(block, key));
        if (unknown.length > 0) {
          setDriveError(`${named} has no parameter ${unknown.join(", ")}`);
          return;
        }

        // REQ-1359: populate the try-it-out param inputs (e.g. aggregate/groupBy) from the
        // NL-forwarded querystring before executing, so the replicated call matches the NL query.
        for (const [key, value] of navParams) await setParam(block, key, value, waitFor);

        if (body !== undefined) {
          await waitFor(() => !!bodyInput(block));
          const input = bodyInput(block);
          if (!input) {
            setDriveError(`${named} takes no request body`);
            return;
          }
          typeInto(input, JSON.stringify(body, null, 2));
        }
        if (!autoRun) return;

        // The native-setter + dispatchEvent hack updates the DOM input immediately, but the actual
        // request Swagger UI builds on execute is driven by its own Redux store, which only picks
        // up the change a render cycle later. The live curl preview reflects that same store, so
        // wait for it to show every param before executing — otherwise execute can fire against
        // the pre-update state even though the inputs already display the right values.
        await waitFor(() => {
          const curlText =
            block.querySelector(".curl-command, .opblock-body pre.curl, .curl")?.textContent ?? "";
          return [...navParams.entries()].every(([key, value]) =>
            curlText.includes(`${key}=${encodeURIComponent(value)}`),
          );
        });

        await waitFor(() => !!block.querySelector(".btn.execute.opblock-control__btn"));
        (block.querySelector(".btn.execute.opblock-control__btn") as HTMLElement | null)?.click();

        const responded = await waitFor(
          () =>
            !!block.querySelector(
              ".responses-wrapper .live-responses-table, .responses-wrapper .microlight",
            ),
          RESPONSE_WAIT_MS,
        );
        setAnswered(responded);
        if (!responded) setDriveError(`no response within ${RESPONSE_WAIT_MS / 1000}s`);
        (block.querySelector(".responses-wrapper") as HTMLElement | null)?.scrollIntoView({
          behavior: "smooth",
          block: "start",
        });
      })();
    },
    [waitFor],
  );

  // REQ-2115: Polly's read_screen reads the call on screen from the docs frame, never the rows.
  const readInputs = useCallback(
    () => ({
      ...shownOperation(iframeRef.current?.contentDocument),
      status: driveError ? "failed" : answered ? "answered" : "idle",
      error: driveError ?? loadError,
    }),
    [answered, driveError, loadError],
  );
  useScreenInputsReader("/openapi", readInputs);

  const frameKey = `${roleId}:${domainsParam}:${theme}`;
  useEffect(() => {
    if (!handoff || frameLoaded !== frameKey || drivenSeqRef.current === handoff.seq) return;
    drivenSeqRef.current = handoff.seq;
    drive(handoff.url, handoff.body, handoff.autoRun);
  }, [handoff, frameLoaded, frameKey, drive]);

  return (
    <div className="openapi-page page" data-answered={answered ? "" : undefined}>
      {loadError && (
        <p className="openapi-load-error">{t("openApiPage.loadError", { error: loadError })}</p>
      )}
      {html !== null && (
        <iframe
          ref={iframeRef}
          key={frameKey}
          srcDoc={html}
          className="openapi-frame"
          title={t("openApiPage.frameTitle")}
          sandbox="allow-scripts allow-same-origin allow-forms allow-downloads allow-popups"
          onLoad={() => setFrameLoaded(frameKey)}
        />
      )}
    </div>
  );
}
