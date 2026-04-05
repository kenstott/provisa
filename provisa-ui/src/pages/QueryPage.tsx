import { useRef, useCallback, useState } from "react";
import { GraphiQL } from "graphiql";
import { createGraphiQLFetcher } from "@graphiql/toolkit";
import "@graphiql/react/style.css";
import "@graphiql/plugin-explorer/style.css";
import "@graphiql/plugin-doc-explorer/style.css";
import "graphiql/graphiql.css";
import "./QueryPage.css";
import { useAuth } from "../context/AuthContext";
import { provisaToolsPlugin } from "../plugins/provisa-tools";
import { ResponseTableOverlay } from "../plugins/table-view";
import {
  useGraphiQL,
  useGraphiQLActions,
  useOperationsEditorState,
  useOptimisticState,
} from "@graphiql/react";

// @ts-ignore — CJS fork, no type declarations
import { Explorer } from "graphiql-explorer";

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
    width={5} height={8} viewBox="0 0 5 8" fill="currentColor"
    style={{ width: "var(--px-16)", transform: "rotate(90deg)" }}
  />
);
const arrowClosed = (
  <svg
    width={5} height={8} viewBox="0 0 5 8" fill="currentColor"
    style={{ width: "var(--px-16)" }}
  />
);
const checkboxUnchecked = (
  <svg width={15} height={15} viewBox="0 0 15 15" stroke="currentColor" fill="none"
    style={{ marginRight: "var(--px-4)" }} />
);
const checkboxChecked = (
  <svg width={15} height={15} viewBox="0 0 15 15" fill="currentColor"
    style={{ fill: "hsl(var(--color-info))", marginRight: "var(--px-4)" }} />
);

const explorerStyles = {
  buttonStyle: { cursor: "pointer", fontSize: "2em", lineHeight: 0 },
  explorerActionsStyle: { paddingTop: "var(--px-16)" },
  actionButtonStyle: {},
};

/** Custom ExplorerPlugin that seeds the query from localStorage before Monaco loads. */
export function SyncedExplorerContent() {
  const { setOperationName, run } = useGraphiQLActions();
  const schema = useGraphiQL((s) => s.schema);
  const [liveQuery, setQuery] = useOptimisticState(useOperationsEditorState());
  // initialQuery is set from storage (graphiql:query or tabState) during provider init.
  // Use it as fallback before Monaco populates liveQuery.
  const initialQuery = useGraphiQL((s) => s.initialQuery);
  const query = liveQuery || initialQuery;

  const handleRunOperation = useCallback(
    (operationName?: string) => {
      if (operationName) setOperationName(operationName);
      run();
    },
    [run, setOperationName],
  );

  return (
    <Explorer
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
    />
  );
}

const syncedExplorerPlugin = {
  title: "GraphiQL Explorer",
  icon: () => (
    <svg height="1em" strokeWidth={1.5} viewBox="0 0 24 24" stroke="currentColor" fill="none">
      <path d="M18 6H20M22 6H20M20 6V4M20 6V8" strokeLinecap="round" strokeLinejoin="round" />
      <path d="M21.4 20H2.6C2.26863 20 2 19.7314 2 19.4V11H21.4C21.7314 11 22 11.2686 22 11.6V19.4C22 19.7314 21.7314 20 21.4 20Z" />
      <path d="M2 11V4.6C2 4.26863 2.26863 4 2.6 4H8.77805C8.92127 4 9.05977 4.05124 9.16852 4.14445L12.3315 6.85555C12.4402 6.94876 12.5787 7 12.722 7H14" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  ),
  content: () => <SyncedExplorerContent />,
};

const REDIRECT_FORMAT_OPTIONS = [
  { value: "", label: "None", mime: "" },
  { value: "parquet", label: "Parquet", mime: "application/vnd.apache.parquet" },
  { value: "arrow", label: "Arrow", mime: "application/vnd.apache.arrow.stream" },
  { value: "csv", label: "CSV", mime: "text/csv" },
  { value: "ndjson", label: "NDJSON", mime: "application/x-ndjson" },
  { value: "json", label: "JSON", mime: "application/json" },
] as const;

interface RedirectSettings {
  format: string;
  threshold: string;
}

function createProvisaFetch(
  settingsRef: React.RefObject<RedirectSettings>,
): typeof globalThis.fetch {
  return async (input, init) => {
    const settings = settingsRef.current;
    const headers = new Headers(init?.headers);
    headers.set("Accept", "application/json");
    if (settings.format) {
      headers.set("X-Provisa-Redirect-Format", settings.format);
      if (settings.threshold) {
        headers.set("X-Provisa-Redirect-Threshold", settings.threshold);
      }
    }

    const res = await fetch(input, { ...init, headers });
    const contentType = res.headers.get("content-type") ?? "";

    if (contentType.includes("application/json")) {
      const body = await res.json();
      // Single-field redirect
      if (body.redirect) {
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
        const redirectEntries = Object.entries(body.redirects as Record<string, {
          redirect_url: string; row_count: number; expires_in: number; content_type: string;
        }>);
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

    return new Response(
      JSON.stringify({
        data: { __error: { message: `Unexpected content type: ${contentType}` } },
      }),
      { status: 200, headers: { "content-type": "application/json" } },
    );
  };
}

/** Query development page — embeds GraphiQL with Explorer (REQ-062). */
export function QueryPage() {
  const { role } = useAuth();
  const [redirectFormat, setRedirectFormat] = useState("");
  const [redirectThreshold, setRedirectThreshold] = useState("");

  const settingsRef = useRef<RedirectSettings>({
    format: redirectFormat,
    threshold: redirectThreshold,
  });
  settingsRef.current = {
    format:
      REDIRECT_FORMAT_OPTIONS.find((o) => o.value === redirectFormat)?.mime ??
      "",
    threshold: redirectThreshold,
  };

  const fetcher = useMemo(() => {
    if (!role) return null;
    return createGraphiQLFetcher({
      url: `/data/graphql`,
      headers: { "X-Provisa-Role": role.id },
      fetch: createProvisaFetch(settingsRef),
    });
  }, [role?.id]);

  const provisaPlugin = useMemo(() => {
    if (!role) return null;
    return provisaToolsPlugin(role.id);
  }, [role?.id]);

  const plugins = useMemo(
    () => (provisaPlugin ? [syncedExplorerPlugin, provisaPlugin] : null),
    [provisaPlugin],
  );

  const onFormatChange = useCallback(
    (e: React.ChangeEvent<HTMLSelectElement>) =>
      setRedirectFormat(e.target.value),
    [],
  );
  const onThresholdChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) =>
      setRedirectThreshold(e.target.value),
    [],
  );

  if (!role || !fetcher || !plugins)
    return <div className="page">Select a role.</div>;

  return (
    <div className="query-page">
      <div className="query-options">
        <label className="query-option">
          Redirect
          <select value={redirectFormat} onChange={onFormatChange}>
            {REDIRECT_FORMAT_OPTIONS.map((o) => (
              <option key={o.value} value={o.value}>
                {o.label}
              </option>
            ))}
          </select>
        </label>
        <label className="query-option" style={{ visibility: redirectFormat ? "visible" : "hidden" }}>
          Threshold
          <input
            type="number"
            min="0"
            placeholder="all"
            value={redirectThreshold}
            onChange={onThresholdChange}
            className="threshold-input"
          />
        </label>
        <span className="query-hint" style={{ visibility: redirectFormat && !redirectThreshold ? "visible" : "hidden" }}>
          All results redirect to S3
        </span>
      </div>
      <GraphiQL
        fetcher={fetcher}
        plugins={plugins}
        forcedTheme="dark"
      >
        <GraphiQL.Footer>
          <ResponseTableOverlay />
        </GraphiQL.Footer>
      </GraphiQL>
    </div>
  );
}
