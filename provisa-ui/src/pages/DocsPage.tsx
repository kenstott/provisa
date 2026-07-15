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
import { useTranslation } from "react-i18next";
import { Alert, Box, Loader, NavLink, Stack } from "@mantine/core";
import { AlertCircle, Info } from "lucide-react";
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
  const { t } = useTranslation();
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
    fetch("/guides-md/manifest.json")
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`manifest ${r.status}`))))
      .then((rows: DocEntry[]) => {
        if (cancelled) return;
        setManifest(rows);
        if (!activeSlug && rows.length > 0) {
          setParams({ doc: rows[0].slug }, { replace: true });
        }
      })
      .catch(() => {
        if (!cancelled) setError(t("docsPage.manifestError"));
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
    // eslint-disable-next-line react-hooks/set-state-in-effect -- resets loading/error/content before async doc fetch; loading depends on in-flight async result and cannot be derived during render
    setLoading(true);
    setError(null);
    setContent("");

    (async () => {
      try {
        const local = await fetch(`/guides-md/${active.slug}.md`);
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
          if (!cancelled) setError(t("docsPage.docUnavailable"));
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
      <Stack component="nav" className="docs-sidebar" gap={2} aria-label={t("docsPage.nav")}>
        {manifest.map((d) => (
          <NavLink
            key={d.slug}
            label={d.title}
            active={d.slug === activeSlug}
            aria-current={d.slug === activeSlug ? "page" : undefined}
            data-testid={`docs-nav-item-${d.slug}`}
            onClick={() => setParams({ doc: d.slug })}
          />
        ))}
      </Stack>

      <article className="docs-content">
        {loading && (
          <Box className="docs-status" data-testid="docs-loading">
            <Loader size="sm" aria-label={t("docsPage.loading")} />
          </Box>
        )}
        {error && (
          <Alert
            color="red"
            icon={<AlertCircle size={16} />}
            data-testid="docs-error"
            className="docs-status"
          >
            {error}
          </Alert>
        )}
        {!loading && !error && (
          <>
            {source === "live" && (
              <Alert
                color="gray"
                icon={<Info size={16} />}
                data-testid="docs-live-note"
                className="docs-live-note"
              >
                {t("docsPage.liveNote")}
              </Alert>
            )}
            <ReactMarkdown
              remarkPlugins={[remarkGfm]}
              components={{
                img({ src, alt, ...rest }) {
                  // Relative doc images (docs/images/*.png) are bundled flat under
                  // /guides-md/images/. Fall back to the live repo if not bundled.
                  if (typeof src === "string" && !/^https?:\/\//.test(src)) {
                    const file = src.split("/").pop();
                    return (
                      <img
                        src={`/guides-md/images/${file}`}
                        alt={alt ?? ""}
                        onError={(e) => {
                          const img = e.currentTarget;
                          const live = `${RAW_BASE}/docs/images/${file}`;
                          if (img.src !== live) img.src = live;
                        }}
                        {...rest}
                      />
                    );
                  }
                  return <img src={src} alt={alt ?? ""} {...rest} />;
                },
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
