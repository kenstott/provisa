// Copyright (c) 2026 Kenneth Stott
// Canary: b7c1ef87-ea60-414e-9f67-8122db7579e4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useRef, useCallback, useState, useMemo, useEffect } from "react";
import { useLocation } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { ActionIcon, Alert, Checkbox, Group, NumberInput, Select, Text, useMantineColorScheme } from "@mantine/core";
import { X } from "lucide-react";
import * as monaco from "monaco-editor";
import { GraphiQL } from "graphiql";
import { createGraphiQLFetcher, type Fetcher } from "@graphiql/toolkit";
import { parse, getOperationAST, buildClientSchema, type GraphQLSchema } from "graphql";
import "@graphiql/react/style.css";
import "@graphiql/plugin-explorer/style.css";
import "@graphiql/plugin-doc-explorer/style.css";
import "graphiql/graphiql.css";
import "./QueryPage.css";
import { useAuth } from "../context/AuthContext";
import { useDomainFilter } from "../context/DomainFilterContext";
import { provisaToolsPlugin } from "../plugins/provisa-tools";
import { ResponseTableOverlay } from "../plugins/table-view";
import { setLastQueryElapsedMs, subscribeQueryTiming } from "../query-timing";
import { HeadersQuickInsert } from "../plugins/headers-quick-insert";
import {
  useGraphiQL,
  useGraphiQLActions,
  useOperationsEditorState,
  useOptimisticState,
} from "@graphiql/react";

// @ts-expect-error -- CJS fork, no type declarations
import { Explorer } from "graphiql-explorer";
import { useDomains } from "../hooks/useAdminQueries";
import { domainGqlAlias } from "../types/admin";

/** Register # @provisa hint completions in the GraphQL Monaco editor. */
monaco.languages.registerCompletionItemProvider("graphql", {
  triggerCharacters: ["#", "@", " ", "="],
  provideCompletionItems(model, position) {
    const lineText = model.getValueInRange({
      startLineNumber: position.lineNumber,
      startColumn: 1,
      endLineNumber: position.lineNumber,
      endColumn: position.column,
    });

    const trimmed = lineText.trimStart();

    // Non-comment lines: suggest Provisa operation-level directives on "@"
    if (!trimmed.startsWith("#")) {
      const atMatch = lineText.match(/@(\w*)$/);
      if (!atMatch) return { suggestions: [] };
      const typed = atMatch[1];
      const atStart = position.column - typed.length - 1; // column of "@"
      const mkRangeAt = (startCol: number) => ({
        startLineNumber: position.lineNumber,
        endLineNumber: position.lineNumber,
        startColumn: startCol,
        endColumn: position.column,
      });
      const directives = [
        {
          label: "@cached",
          insertText: "cached(ttl: ${1:300})",
          detail: "Cache results for N seconds",
          documentation:
            "Cache this query's results server-side. ttl is the time-to-live in seconds (0 = disable).",
        },
        {
          label: "@route",
          insertText: "route(engine: ${1|FEDERATED,DIRECT|})",
          detail: "Force execution engine",
          documentation: "FEDERATED = federated execution, DIRECT = native driver.",
        },
        {
          label: "@join",
          insertText: "join(strategy: ${1|BROADCAST,PARTITIONED|})",
          detail: "Federated join strategy",
          documentation: "BROADCAST: small dimension table. PARTITIONED: large fact-to-fact join.",
        },
        {
          label: "@reorder",
          insertText: "reorder(enabled: ${1|false,true|})",
          detail: "Federated join reordering",
          documentation: "enabled: false disables the federation engine's cost-based join reordering.",
        },
        {
          label: "@broadcastSize",
          insertText: 'broadcastSize(size: "${1:512MB}")',
          detail: "Max broadcast table size",
          documentation: "Sets the federation engine join_max_broadcast_table_size session property.",
        },
        {
          label: "@redirect",
          insertText:
            'redirect(format: "${1|parquet,csv,arrow,ndjson,json|}", threshold: ${2:10000})',
          detail: "Redirect large results to object store",
          documentation: "Streams results to MinIO/S3 when row count exceeds threshold.",
        },
        {
          label: "@sink",
          insertText: 'sink(topic: "${1:topic-name}")',
          detail: "Stream results to Kafka topic",
          documentation: "Publishes query results to the specified Kafka topic.",
        },
        {
          label: "@watermark",
          insertText: "watermark",
          detail: "Mark watermark field (field-level)",
          documentation:
            "Applied to a field to mark it as the watermark/cursor column for incremental queries.",
        },
      ];
      return {
        suggestions: directives
          .filter((d) => d.label.slice(1).startsWith(typed))
          .map((d) => ({
            label: d.label,
            kind: monaco.languages.CompletionItemKind.Keyword,
            detail: d.detail,
            documentation: d.documentation,
            insertText: d.insertText,
            insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
            range: mkRangeAt(atStart),
          })),
      };
    }

    const commentContent = trimmed.slice(1).trimStart();

    const mkRange = (startCol: number) => ({
      startLineNumber: position.lineNumber,
      endLineNumber: position.lineNumber,
      startColumn: startCol,
      endColumn: position.column,
    });

    // After "# @provisa " → suggest commands
    const provisaMatch = commentContent.match(/^@provisa\s+(\S*)$/);
    if (provisaMatch) {
      const typed = provisaMatch[1];
      const cmdStart = position.column - typed.length;
      return {
        suggestions: [
          {
            label: "route=federated",
            kind: monaco.languages.CompletionItemKind.EnumMember,
            detail: "Force federated execution",
            documentation:
              "Route this query through the federation engine even if a direct driver is available.",
            insertText: "route=federated",
            range: mkRange(cmdStart),
          },
          {
            label: "route=direct",
            kind: monaco.languages.CompletionItemKind.EnumMember,
            detail: "Force direct driver",
            documentation:
              "Route this query directly to the source, bypassing federation. Only applies to single-source queries with a native driver.",
            insertText: "route=direct",
            range: mkRange(cmdStart),
          },
          {
            label: "join=broadcast",
            kind: monaco.languages.CompletionItemKind.EnumMember,
            detail: "Broadcast join strategy",
            documentation:
              "Sets federated engine join_distribution_type=BROADCAST. Broadcasts the smaller table to all nodes — best for small dimension tables.",
            insertText: "join=broadcast",
            range: mkRange(cmdStart),
          },
          {
            label: "join=partitioned",
            kind: monaco.languages.CompletionItemKind.EnumMember,
            detail: "Partitioned join strategy",
            documentation:
              "Sets federated engine join_distribution_type=PARTITIONED. Hash-partitions both sides — best for large fact-to-fact joins.",
            insertText: "join=partitioned",
            range: mkRange(cmdStart),
          },
          {
            label: "reorder=off",
            kind: monaco.languages.CompletionItemKind.EnumMember,
            detail: "Disable join reordering",
            documentation:
              "Sets federated engine join_reordering_strategy=NONE. Use when the federation engine's cost-based reordering produces a bad plan.",
            insertText: "reorder=off",
            range: mkRange(cmdStart),
          },
          {
            label: "broadcast_size=",
            kind: monaco.languages.CompletionItemKind.EnumMember,
            detail: "Max broadcast table size",
            documentation:
              "Sets federated engine join_max_broadcast_table_size. E.g. broadcast_size=512MB.",
            insertText: "broadcast_size=",
            range: mkRange(cmdStart),
          },
        ],
      };
    }

    // After "# " or "# @<partial>" → suggest @provisa
    const atMatch = commentContent.match(/^(@\S*)$/);
    if (atMatch || commentContent === "" || /^\S*$/.test(commentContent)) {
      const typed = atMatch ? atMatch[1] : commentContent;
      const typedStart = position.column - typed.length;
      return {
        suggestions: [
          {
            label: "@provisa",
            kind: monaco.languages.CompletionItemKind.Keyword,
            detail: "Provisa query hint",
            documentation: "Add a Provisa execution hint, e.g. route=federated or route=direct.",
            insertText: "@provisa ",
            range: mkRange(typedStart),
            command: { id: "editor.action.triggerSuggest", title: "Trigger suggest" },
          },
        ],
      };
    }

    return { suggestions: [] };
  },
});

const colors = {
  keyword: "hsl(var(--color-primary))",
  def: "hsl(var(--color-tertiary))",
  property: "hsl(var(--color-info))",
  qualifier: "hsl(var(--color-secondary))",
  attribute: "hsl(var(--color-tertiary))",
  number: "hsl(var(--color-success))",
  string: "hsl(var(--color-warning))",
  builtin: "hsl(var(--color-success))",
  string2: "hsl(var(--color-secondary))",
  variable: "hsl(var(--color-secondary))",
  atom: "hsl(var(--color-tertiary))",
};

const arrowOpen = (
  <svg
    width={5}
    height={8}
    viewBox="0 0 5 8"
    fill="currentColor"
    style={{ width: "var(--px-16)", transform: "rotate(90deg)" }}
  >
    <path d="M0 0L5 4L0 8Z" />
  </svg>
);
const arrowClosed = (
  <svg width={5} height={8} viewBox="0 0 5 8" fill="currentColor" style={{ width: "var(--px-16)" }}>
    <path d="M0 0L5 4L0 8Z" />
  </svg>
);
const checkboxUnchecked = (
  <svg
    width={15}
    height={15}
    viewBox="0 0 15 15"
    stroke="currentColor"
    fill="none"
    style={{ marginRight: "var(--px-4)" }}
  >
    <rect x="1.5" y="1.5" width="12" height="12" rx="1.5" strokeWidth="1.5" />
  </svg>
);
const checkboxChecked = (
  <svg
    width={15}
    height={15}
    viewBox="0 0 15 15"
    fill="currentColor"
    style={{ fill: "hsl(var(--color-info))", marginRight: "var(--px-4)" }}
  >
    <rect x="1.5" y="1.5" width="12" height="12" rx="1.5" />
    <path
      d="M4.5 7.5L6.5 9.5L10.5 5.5"
      stroke="white"
      strokeWidth="1.5"
      fill="none"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  </svg>
);

const explorerStyles = {
  buttonStyle: { cursor: "pointer", fontSize: "2em", lineHeight: 0 },
  explorerActionsStyle: { paddingTop: "var(--px-16)" },
  actionButtonStyle: {},
};

/** Custom ExplorerPlugin that syncs GraphiQL state with the Explorer. */
export function SyncedExplorerContent() {
  const { setOperationName, run } = useGraphiQLActions();
  const schema = useGraphiQL((s) => s.schema);
  const activeTabIndex = useGraphiQL((s) => s.activeTabIndex);
  const { domains } = useDomains();
  const domainLabels = useMemo(() => {
    const map: Record<string, string> = {};
    for (const d of domains) map[domainGqlAlias(d)] = d.id;
    return map;
  }, [domains]);
  // Canonical per-tab query from the store — reliable during tab switches
  const tabQuery = useGraphiQL((s) => s.tabs[s.activeTabIndex]?.query ?? "");
  const [liveQuery, setQuery] = useOptimisticState(useOperationsEditorState());
  // During a tab switch, liveQuery is still synced to the previous tab's editor.
  // Use tabQuery as the source of truth on the first render after a switch.
  const prevTabIndexRef = useRef(activeTabIndex);
  /* eslint-disable-next-line react-hooks/refs --
     sanctioned React render-phase pattern for detecting a prop change (activeTabIndex) to pick the source-of-truth query; the ref is read and updated in the same render with no side effects */
  const isTabSwitching = prevTabIndexRef.current !== activeTabIndex;
  /* eslint-disable-next-line react-hooks/refs --
     updating the previous-value ref during render is part of the same sanctioned prop-change detection pattern */
  if (isTabSwitching) prevTabIndexRef.current = activeTabIndex;
  const query = isTabSwitching ? tabQuery : liveQuery || tabQuery;

  const handleRunOperation = useCallback(
    (operationName?: string) => {
      if (operationName) setOperationName(operationName);
      run();
    },
    [run, setOperationName],
  );

  return (
    <Explorer
      key={activeTabIndex}
      schema={schema}
      query={query}
      onEdit={setQuery}
      onRunOperation={handleRunOperation}
      explorerIsOpen={true}
      colors={colors}
      arrowOpen={arrowOpen}
      arrowClosed={arrowClosed}
      checkboxUnchecked={checkboxUnchecked}
      checkboxChecked={checkboxChecked}
      styles={explorerStyles}
      title=""
      domainLabels={domainLabels}
    />
  );
}

const syncedExplorerPlugin = {
  title: "GraphiQL Explorer",
  icon: () => (
    <svg height="1em" strokeWidth={1.5} viewBox="0 0 24 24" stroke="currentColor" fill="none">
      <path d="M18 6H20M22 6H20M20 6V4M20 6V8" strokeLinecap="round" strokeLinejoin="round" />
      <path d="M21.4 20H2.6C2.26863 20 2 19.7314 2 19.4V11H21.4C21.7314 11 22 11.2686 22 11.6V19.4C22 19.7314 21.7314 20 21.4 20Z" />
      <path
        d="M2 11V4.6C2 4.26863 2.26863 4 2.6 4H8.77805C8.92127 4 9.05977 4.05124 9.16852 4.14445L12.3315 6.85555C12.4402 6.94876 12.5787 7 12.722 7H14"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  ),
  content: () => <SyncedExplorerContent />,
};

const REDIRECT_FORMAT_OPTIONS = [
  { value: "", labelKey: "queryPage.formatNone", mime: "" },
  { value: "parquet", labelKey: "queryPage.formatParquet", mime: "application/vnd.apache.parquet" },
  { value: "arrow", labelKey: "queryPage.formatArrow", mime: "application/vnd.apache.arrow.stream" },
  { value: "csv", labelKey: "queryPage.formatCsv", mime: "text/csv" },
  { value: "ndjson", labelKey: "queryPage.formatNdjson", mime: "application/x-ndjson" },
  { value: "json", labelKey: "queryPage.formatJson", mime: "application/json" },
] as const;

interface RedirectInfo {
  url: string;
  row_count: number;
  expires_in: number;
  content_type: string;
}

interface RedirectSettings {
  format: string;
  threshold: string;
  statsEnabled: boolean;
  onRedirect: (result: RedirectInfo | null) => void;
}

function createProvisaFetch(
  settingsRef: React.RefObject<RedirectSettings>,
): typeof globalThis.fetch {
  return async (input, init) => {
    const settings = settingsRef.current;
    settings.onRedirect(null);
    const headers = new Headers(init?.headers);
    headers.set("Accept", "application/json");
    if (settings.format) {
      headers.set("X-Provisa-Redirect-Format", settings.format);
      if (settings.threshold === "all") {
        headers.set("X-Provisa-Redirect", "true");
      } else if (settings.threshold) {
        headers.set("X-Provisa-Redirect-Threshold", settings.threshold);
      }
    }
    if (settings.statsEnabled) {
      headers.set("X-Provisa-Stats", "true");
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 60000);
    let res: Response;
    const _t0 = performance.now();
    try {
      res = await fetch(input, { ...init, headers, signal: controller.signal });
      setLastQueryElapsedMs(performance.now() - _t0);
    } finally {
      clearTimeout(timeoutId);
    }
    const contentType = res.headers.get("content-type") ?? "";

    if (contentType.includes("application/json")) {
      const body = await res.json();
      // Single-field redirect
      if (body.redirect) {
        settings.onRedirect({
          url: body.redirect.redirect_url,
          row_count: body.redirect.row_count,
          expires_in: body.redirect.expires_in,
          content_type: body.redirect.content_type,
        });
        const synthetic = {
          data: {
            ...body.data,
            __redirect: {
              url: body.redirect.redirect_url,
              row_count: body.redirect.row_count,
              expires_in: body.redirect.expires_in,
              content_type: body.redirect.content_type,
            },
          },
        };
        return new Response(JSON.stringify(synthetic), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }
      // Multi-field redirects (some or all fields redirected)
      if (body.redirects) {
        const redirectEntries = Object.entries(
          body.redirects as Record<
            string,
            {
              redirect_url: string;
              row_count: number;
              expires_in: number;
              content_type: string;
            }
          >,
        );
        const redirectData: Record<string, unknown> = {};
        for (const [field, info] of redirectEntries) {
          redirectData[`__redirect_${field}`] = {
            field,
            url: info.redirect_url,
            row_count: info.row_count,
            expires_in: info.expires_in,
            content_type: info.content_type,
          };
        }
        const synthetic = {
          data: {
            ...body.data,
            ...redirectData,
          },
        };
        return new Response(JSON.stringify(synthetic), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }
      return new Response(JSON.stringify(body), {
        status: res.status,
        headers: { "content-type": "application/json" },
      });
    }

    const body = await res.text();
    return new Response(
      JSON.stringify({
        errors: [{ message: body || `HTTP ${res.status}` }],
      }),
      { status: 200, headers: { "content-type": "application/json" } },
    );
  };
}

/** Opens a new GraphiQL tab, populates it, and executes — triggered by navigation from NL page. */
function AutoRunFromNav({ query }: { query: string }) {
  const { addTab, updateActiveTabValues, run } = useGraphiQLActions();
  const queryEditor = useGraphiQL((s) => s.queryEditor);
  const didRun = useRef(false);

  // Wait for queryEditor to become available (it's set asynchronously by GraphiQL).
  // Once it's ready, add a tab, populate, and execute exactly once.
  useEffect(() => {
    if (!queryEditor || didRun.current) return;
    didRun.current = true;
    addTab();
    updateActiveTabValues({ query });
    queryEditor.setValue(query);
    const t = setTimeout(() => run(), 100);
    return () => clearTimeout(t);
  // eslint-disable-next-line react-hooks/exhaustive-deps -- run once when queryEditor becomes available (guarded by didRun); other deps must not re-trigger
  }, [queryEditor]);
  return null;
}

/** Query development page — embeds GraphiQL with Explorer (REQ-062). */
export function QueryPage() {
  const { t } = useTranslation();
  const { colorScheme } = useMantineColorScheme();
  // GraphiQL has its own light/dark theme — force it to follow the app scheme.
  const graphiqlTheme = colorScheme === "light" ? "light" : "dark";
  const { role } = useAuth();
  const { checkedDomains } = useDomainFilter();
  const location = useLocation();
  const [domainSchema, setDomainSchema] = useState<GraphQLSchema | null>(null);
  // Frozen initial values — never updated so GraphiQL owns these states after mount.
  const locationState = (location.state as { query?: string; autoRun?: boolean } | null);
  const [initialQuery] = useState<string | undefined>(() => locationState?.query ?? undefined);
  const [autoRunQuery] = useState<string | undefined>(
    () => locationState?.autoRun && locationState.query ? locationState.query : undefined,
  );
  const [initialVisiblePlugin] = useState<string | undefined>(
    () => localStorage.getItem("query:visiblePlugin") ?? undefined,
  );
  const [initialEditorTab] = useState<"variables" | "headers">(
    () => (localStorage.getItem("query:editorTab") as "variables" | "headers") ?? "variables",
  );
  const [redirectFormat, setRedirectFormat] = useState(
    () => localStorage.getItem("query:redirectFormat") ?? "",
  );
  const [redirectThreshold, setRedirectThreshold] = useState(
    () => localStorage.getItem("query:redirectThreshold") ?? "",
  );
  const [statsEnabled, setStatsEnabled] = useState(() => localStorage.getItem("query:statsEnabled") === "true");
  const [queryElapsedMs, setQueryElapsedMs] = useState<number | null>(null);
  const [redirectResult, setRedirectResult] = useState<RedirectInfo | null>(null);
  useEffect(() => subscribeQueryTiming(setQueryElapsedMs), []);

  // Persist which secondary editor tab (Variables/Headers) is active.
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      const name = (e.target as HTMLElement).closest<HTMLElement>("[data-name]")?.dataset.name;
      if (name === "variables" || name === "headers") {
        localStorage.setItem("query:editorTab", name);
      }
    };
    document.addEventListener("click", handler, true);
    return () => document.removeEventListener("click", handler, true);
  }, []);

  const [serverSchemaVersion, setServerSchemaVersion] = useState<number | null>(null);
  const [schemaError, setSchemaError] = useState<string | null>(null);

  // Poll /data/schema-version every 30s and on page focus.
  // The version counter is bumped server-side on every schema rebuild.
  useEffect(() => {
    let cancelled = false;
    const check = () => {
      fetch("/data/schema-version")
        .then((r) => r.json())
        .then((j) => {
          if (!cancelled) setServerSchemaVersion(j.version);
        })
        .catch(() => {});
    };
    check();
    const timer = setInterval(check, 30_000);
    window.addEventListener("focus", check);
    // Cross-tab: SqlPage writes provisa.schema.version to localStorage on rebuild
    const onStorage = (e: StorageEvent) => {
      if (e.key === "provisa.schema.version") check();
    };
    window.addEventListener("storage", onStorage);
    return () => {
      cancelled = true;
      clearInterval(timer);
      window.removeEventListener("focus", check);
      window.removeEventListener("storage", onStorage);
    };
  }, []);

  useEffect(() => {
    /* eslint-disable react-hooks/set-state-in-effect -- deliberate reset of externally-fetched schema state when prerequisites are absent; the effect's job is to sync domainSchema to a network introspection fetch */
    if (!role || checkedDomains.size === 0 || serverSchemaVersion === null) {
      setDomainSchema(null);
      return;
    }
    /* eslint-enable react-hooks/set-state-in-effect */
    const domain = [...checkedDomains].sort().join(",");
    const cacheKey = `introspection:${role.id}:${domain}:${serverSchemaVersion}`;
    const cached = sessionStorage.getItem(cacheKey);
    if (cached) {
      try {
        setDomainSchema(buildClientSchema(JSON.parse(cached)));
        setSchemaError(null);
        return;
      } catch {
        sessionStorage.removeItem(cacheKey);
      }
    }
    const controller = new AbortController();
    setSchemaError(null);
    fetch(`/data/introspection?domain=${encodeURIComponent(domain)}`, {
      headers: { "X-Provisa-Role": role.id },
      signal: controller.signal,
    })
      .then((r) => r.json())
      .then((json) => {
        if (json.data) {
          sessionStorage.setItem(cacheKey, JSON.stringify(json.data));
          // Prune stale entries from prior schema versions
          for (let i = sessionStorage.length - 1; i >= 0; i--) {
            const k = sessionStorage.key(i);
            if (k && k.startsWith("introspection:") && k !== cacheKey) sessionStorage.removeItem(k);
          }
          setDomainSchema(buildClientSchema(json.data));
          setSchemaError(null);
        } else {
          setSchemaError(json.detail ?? "Schema unavailable");
        }
      })
      .catch((err) => {
        if (err.name !== "AbortError") setSchemaError(err.message ?? "Schema fetch failed");
      });
    return () => controller.abort();
    /* eslint-disable-next-line react-hooks/exhaustive-deps --
       keyed on role.id only; the full role object identity must not retrigger the introspection fetch */
  }, [role?.id, checkedDomains, serverSchemaVersion]);

  const settingsRef = useRef<RedirectSettings>({
    format: redirectFormat,
    threshold: redirectThreshold,
    statsEnabled,
    onRedirect: setRedirectResult,
  });
  /* eslint-disable-next-line react-hooks/refs --
     latest-value ref: createProvisaFetch reads current redirect settings at request time; the fetcher is memoized on role only and must not be recreated when settings change */
  settingsRef.current = {
    format: REDIRECT_FORMAT_OPTIONS.find((o) => o.value === redirectFormat)?.mime ?? "",
    threshold: redirectThreshold,
    statsEnabled,
    onRedirect: setRedirectResult,
  };

  const fetcher = useMemo((): Fetcher | null => {
    if (!role) return null;
    const roleId = role.id;
    const base = createGraphiQLFetcher({
      url: `/data/graphql`,
      headers: { "X-Provisa-Role": roleId },
      /* eslint-disable-next-line react-hooks/refs --
         latest-value ref intentionally passed to the fetch wrapper; it is dereferenced per-request inside createProvisaFetch, never during render */
      fetch: createProvisaFetch(settingsRef),
    });
    return async function* (request, opts) {
      // Detect subscription operations and stream via SSE
      let isSubscription = false;
      try {
        const doc = parse(request.query ?? "");
        const op = getOperationAST(doc, request.operationName ?? undefined);
        isSubscription = op?.operation === "subscription";
      } catch {
        // unparseable — fall through to server
      }

      if (isSubscription) {
        const controller = new AbortController();
        try {
          const response = await fetch("/data/graphql", {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Accept: "text/event-stream",
              "X-Provisa-Role": roleId,
            },
            body: JSON.stringify({
              query: request.query,
              variables: request.variables,
              operationName: request.operationName,
            }),
            signal: controller.signal,
          });
          if (!response.ok || !response.body) {
            yield { errors: [{ message: `HTTP ${response.status}` }] };
            return;
          }
          const reader = response.body.getReader();
          const decoder = new TextDecoder();
          let buffer = "";
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });
            const chunks = buffer.split("\n\n");
            buffer = chunks.pop() ?? "";
            for (const chunk of chunks) {
              const dataLine = chunk.split("\n").find((l) => l.startsWith("data: "));
              if (!dataLine) continue;
              const raw = dataLine.slice(6).trim();
              if (!raw) continue;
              try {
                yield JSON.parse(raw);
              } catch {
                // skip malformed
              }
            }
          }
        } finally {
          controller.abort();
        }
        return;
      }

      // Non-subscription: use standard fetcher
      // base() is async, so it returns Promise<AsyncGenerator|ExecutionResult>
      const result = await base(request, opts);
      if (
        result &&
        typeof (result as { [Symbol.asyncIterator]?: unknown })[Symbol.asyncIterator] === "function"
      ) {
        yield* result as AsyncIterable<unknown>;
      } else {
        yield result;
      }
    };
    /* eslint-disable-next-line react-hooks/exhaustive-deps --
       keyed on role.id only; the full role object identity changes on unrelated field updates and must not recreate the fetcher */
  }, [role?.id]);

  const provisaPlugin = useMemo(() => {
    if (!role) return null;
    return provisaToolsPlugin(role.id);
    /* eslint-disable-next-line react-hooks/exhaustive-deps --
       keyed on role.id only; recreating the plugin on full role identity changes is unnecessary and disruptive */
  }, [role?.id]);

  const plugins = useMemo(
    () => (provisaPlugin ? [syncedExplorerPlugin, provisaPlugin] : null),
    [provisaPlugin],
  );

  const onFormatChange = useCallback((value: string | null) => {
    const v = value ?? "";
    localStorage.setItem("query:redirectFormat", v);
    setRedirectFormat(v);
  }, []);
  const onThresholdChange = useCallback((value: string | number) => {
    const v = value === "" ? "" : String(value);
    localStorage.setItem("query:redirectThreshold", v);
    setRedirectThreshold(v);
  }, []);
  const onStatsChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    setStatsEnabled(e.currentTarget.checked);
    localStorage.setItem("query:statsEnabled", String(e.currentTarget.checked));
  }, []);
  const onPluginVisibilityChange = useCallback(
    (plugin: { title: string } | null) => localStorage.setItem("query:visiblePlugin", plugin?.title ?? ""),
    [],
  );

  if (!role || !fetcher || !plugins) return <div className="page">{t("queryPage.selectRole")}</div>;

  return (
    <div className="query-page">
      <Group className="query-options" gap="md" wrap="nowrap" px="sm" py={6}>
        <Select
          label={t("queryPage.redirectLabel")}
          size="xs"
          data-testid="redirect-format-select"
          data={REDIRECT_FORMAT_OPTIONS.map((o) => ({ value: o.value, label: t(o.labelKey) }))}
          value={redirectFormat}
          onChange={onFormatChange}
          allowDeselect={false}
          w={140}
        />
        <NumberInput
          label={t("queryPage.thresholdLabel")}
          size="xs"
          min={0}
          placeholder={t("queryPage.thresholdPlaceholder")}
          value={redirectThreshold === "" ? "" : Number(redirectThreshold)}
          onChange={onThresholdChange}
          data-testid="redirect-threshold-input"
          w={100}
          style={{ visibility: redirectFormat ? "visible" : "hidden" }}
        />
        <Text
          className="query-hint"
          size="xs"
          fs="italic"
          c="dimmed"
          style={{ visibility: redirectFormat && !redirectThreshold ? "visible" : "hidden" }}
        >
          {t("queryPage.redirectHint")}
        </Text>
        {queryElapsedMs !== null && (
          <Text className="query-elapsed" size="xs" ml="auto">
            {Math.round(queryElapsedMs)} ms
          </Text>
        )}
        <Checkbox
          label={t("queryPage.queryStats")}
          checked={statsEnabled}
          onChange={onStatsChange}
          data-testid="query-stats-checkbox"
          ml={queryElapsedMs !== null ? undefined : "auto"}
        />
      </Group>
      {schemaError && (
        <Alert color="red" variant="light" py={4} radius={0}>
          {t("queryPage.schemaError", { message: schemaError })}
        </Alert>
      )}
      {redirectResult && (
        <Alert color="green" variant="light" py={4} radius={0}>
          <Group gap="md" wrap="nowrap">
            <Text size="xs" c="green">
              {t("queryPage.redirectReady", {
                rowCount: redirectResult.row_count,
                contentType: redirectResult.content_type,
              })}
            </Text>
            <Text
              component="a"
              href={redirectResult.url}
              download
              size="xs"
              fw={600}
              c="green"
              td="underline"
            >
              {t("queryPage.download")}
            </Text>
            <ActionIcon
              variant="transparent"
              color="green"
              size="sm"
              ml="auto"
              aria-label={t("queryPage.dismissRedirect")}
              onClick={() => setRedirectResult(null)}
            >
              <X size={14} />
            </ActionIcon>
          </Group>
        </Alert>
      )}
      <GraphiQL
        fetcher={fetcher}
        plugins={plugins}
        forcedTheme={graphiqlTheme}
        schema={domainSchema ?? undefined}
        visiblePlugin={initialVisiblePlugin}
        onTogglePluginVisibility={onPluginVisibilityChange}
        defaultEditorToolsVisibility={initialEditorTab}
        defaultQuery={initialQuery}
        shouldPersistHeaders
      >
        {autoRunQuery && <AutoRunFromNav query={autoRunQuery} />}
        <GraphiQL.Footer>
          <ResponseTableOverlay />
          <HeadersQuickInsert />
        </GraphiQL.Footer>
      </GraphiQL>
    </div>
  );
}
