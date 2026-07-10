// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback, useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import "./DocsPage.css";

interface DocEntry {
  slug: string;
  title: string;
  repoPath: string;
}

// Live-fallback base: the public repo's raw content, used only when a doc isn't
// bundled locally (offline/airgap installs serve everything same-origin).
const RAW_BASE = "https://raw.githubusercontent.com/kenstott/provisa/main";

export function DocsPage() {
  const [params, setParams] = useSearchParams();
  const [manifest, setManifest] = useState<DocEntry[]>([]);
  const [content, setContent] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [source, setSource] = useState<"bundled" | "live" | null>(null);

  const activeSlug = params.get("doc");

  // Load the manifest once; default to the first doc.
  useEffect(() => {
    let cancelled = false;
    fetch("/docs/manifest.json")
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`manifest ${r.status}`))))
      .then((rows: DocEntry[]) => {
        if (cancelled) return;
        setManifest(rows);
        if (!activeSlug && rows.length > 0) {
          setParams({ doc: rows[0].slug }, { replace: true });
        }
      })
      .catch(() => {
        if (!cancelled) setError("Documentation index could not be loaded.");
      });
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- run once on mount
  }, []);

  const active = useMemo(
    () => manifest.find((d) => d.slug === activeSlug) ?? null,
    [manifest, activeSlug],
  );

  // Load the active doc: bundled (same-origin) first, live GitHub-raw fallback.
  useEffect(() => {
    if (!active) return;
    let cancelled = false;
    setLoading(true);
    setError(null);
    setContent("");

    (async () => {
      try {
        const local = await fetch(`/docs/${active.slug}.md`);
        if (local.ok) {
          const text = await local.text();
          if (!cancelled) {
            setContent(text);
            setSource("bundled");
          }
          return;
        }
        throw new Error(`bundled ${local.status}`);
      } catch {
        try {
          const live = await fetch(`${RAW_BASE}/${active.repoPath}`);
          if (!live.ok) throw new Error(`live ${live.status}`);
          const text = await live.text();
          if (!cancelled) {
            setContent(text);
            setSource("live");
          }
        } catch {
          if (!cancelled) setError("This document is not available offline. Reconnect to load it.");
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [active]);

  // Rewrite in-doc relative *.md links to stay inside the reader.
  const slugForPath = useCallback(
    (href: string): string | null => {
      const file = href.split("/").pop() ?? href;
      const base = file.replace(/\.md(#.*)?$/i, "");
      const hit = manifest.find((d) => d.slug === base || d.repoPath.endsWith(`${base}.md`));
      return hit ? hit.slug : null;
    },
    [manifest],
  );

  return (
    <div className="docs-page">
      <nav className="docs-sidebar" aria-label="Documentation">
        {manifest.map((d) => (
          <button
            key={d.slug}
            className={`docs-nav-item${d.slug === activeSlug ? " docs-nav-active" : ""}`}
            onClick={() => setParams({ doc: d.slug })}
          >
            {d.title}
          </button>
        ))}
      </nav>

      <article className="docs-content">
        {loading && <div className="docs-status">Loading…</div>}
        {error && <div className="docs-status docs-error">{error}</div>}
        {!loading && !error && (
          <>
            {source === "live" && (
              <div className="docs-live-note">Showing the latest version from the public repository.</div>
            )}
            <ReactMarkdown
              remarkPlugins={[remarkGfm]}
              components={{
                a({ href, children, ...rest }) {
                  const target = href && /\.md(#.*)?$/i.test(href) ? slugForPath(href) : null;
                  if (target) {
                    return (
                      <a
                        href={`?doc=${target}`}
                        onClick={(e) => {
                          e.preventDefault();
                          setParams({ doc: target });
                        }}
                        {...rest}
                      >
                        {children}
                      </a>
                    );
                  }
                  return (
                    <a href={href} target="_blank" rel="noreferrer" {...rest}>
                      {children}
                    </a>
                  );
                },
              }}
            >
              {content}
            </ReactMarkdown>
          </>
        )}
      </article>
    </div>
  );
}
