import { useMemo, useRef, useCallback, useState } from "react";
import { GraphiQL } from "graphiql";
import { createGraphiQLFetcher } from "@graphiql/toolkit";
import { explorerPlugin } from "@graphiql/plugin-explorer";
import "@graphiql/react/style.css";
import "@graphiql/plugin-explorer/style.css";
import "@graphiql/plugin-doc-explorer/style.css";
import "graphiql/graphiql.css";
import "./QueryPage.css";
import { useAuth } from "../context/AuthContext";

const explorer = explorerPlugin();

const REDIRECT_FORMAT_OPTIONS = [
  { value: "", label: "None", mime: "" },
  { value: "parquet", label: "Parquet", mime: "application/vnd.apache.parquet" },
  { value: "arrow", label: "Arrow", mime: "application/vnd.apache.arrow.stream" },
  { value: "csv", label: "CSV", mime: "text/csv" },
  { value: "ndjson", label: "NDJSON", mime: "application/x-ndjson" },
  { value: "json", label: "JSON", mime: "application/json" },
] as const;

interface RedirectSettings {
  format: string; // mime type, empty = no redirect
  threshold: string; // empty = force, number = conditional
}

/**
 * Wrap fetch to intercept redirect responses and non-JSON inline responses,
 * converting them into GraphQL-shaped JSON that GraphiQL can display.
 */
function createProvisaFetch(
  settingsRef: React.RefObject<RedirectSettings>,
): typeof globalThis.fetch {
  return async (input, init) => {
    const settings = settingsRef.current;
    const headers = new Headers(init?.headers);

    // Always request JSON inline
    headers.set("Accept", "application/json");

    // Redirect headers
    if (settings.format) {
      headers.set("X-Provisa-Redirect-Format", settings.format);
      if (settings.threshold) {
        headers.set("X-Provisa-Redirect-Threshold", settings.threshold);
      }
      // No threshold = force redirect (server infers this)
    }

    const res = await fetch(input, { ...init, headers });
    const contentType = res.headers.get("content-type") ?? "";

    if (contentType.includes("application/json")) {
      const body = await res.json();

      if (body.redirect) {
        const synthetic = {
          data: {
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

      return new Response(JSON.stringify(body), {
        status: res.status,
        headers: { "content-type": "application/json" },
      });
    }

    // Shouldn't happen with Accept: application/json, but handle gracefully
    return new Response(
      JSON.stringify({
        data: {
          __error: {
            message: `Unexpected content type: ${contentType}`,
          },
        },
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
      headers: {
        "X-Provisa-Role": role.id,
      },
      fetch: createProvisaFetch(settingsRef),
    });
  }, [role?.id]);

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

  if (!role || !fetcher) return <div className="page">Select a role.</div>;

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
        {redirectFormat && (
          <label className="query-option">
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
        )}
        {redirectFormat && !redirectThreshold && (
          <span className="query-hint">All results redirect to S3</span>
        )}
      </div>
      <GraphiQL fetcher={fetcher} plugins={[explorer]} forcedTheme="dark" />
    </div>
  );
}
