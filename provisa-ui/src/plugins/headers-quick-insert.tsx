// Copyright (c) 2026 Kenneth Stott
// Canary: 3f8a2b91-dc4e-4f7e-8b3e-1c9f7d3e5a2b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * HeadersQuickInsert
 *
 * Injects quick-insert buttons at the top of the GraphiQL headers editor
 * content area (only visible when the "Headers" tab is active). Clicking
 * a button toggles the corresponding X-Provisa-* header in the JSON editor.
 */

import { useState, useEffect, useRef, useCallback } from "react";
import { createPortal } from "react-dom";
import { useGraphiQL } from "@graphiql/react";

interface HeaderDef {
  label: string;
  key: string;
  defaultValue: string;
  title: string;
}

const PROVISA_HEADERS: HeaderDef[] = [
  {
    label: "Role",
    key: "X-Provisa-Role",
    defaultValue: "admin",
    title: "Override the role used for this request",
  },
  {
    label: "Redirect",
    key: "X-Provisa-Redirect",
    defaultValue: "true",
    title: "Force redirect response to S3 regardless of row count",
  },
  {
    label: "Format: Parquet",
    key: "X-Provisa-Redirect-Format",
    defaultValue: "application/vnd.apache.parquet",
    title: "Set redirect format to Parquet",
  },
  {
    label: "Format: Arrow",
    key: "X-Provisa-Redirect-Format",
    defaultValue: "application/vnd.apache.arrow.stream",
    title: "Set redirect format to Arrow IPC stream",
  },
  {
    label: "Format: CSV",
    key: "X-Provisa-Redirect-Format",
    defaultValue: "text/csv",
    title: "Set redirect format to CSV",
  },
  {
    label: "Threshold",
    key: "X-Provisa-Redirect-Threshold",
    defaultValue: "1000",
    title: "Row count threshold above which redirect is triggered",
  },
];

function parseHeaders(text: string): Record<string, string> {
  try {
    const parsed = JSON.parse(text || "{}");
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      return parsed as Record<string, string>;
    }
  } catch {
    // ignore
  }
  return {};
}

function isHeadersTabActive(): boolean {
  // GraphiQL v3: section.graphiql-editor-tool gets aria-label="Headers" when that tab is active
  return !!document.querySelector('section.graphiql-editor-tool[aria-label="Headers"]');
}

function findHeadersEditor(): HTMLElement | null {
  // section.graphiql-editor-tool contains two .graphiql-editor divs:
  //   [0] VariableEditor  (class="graphiql-editor hidden" when headers active)
  //   [1] HeaderEditor    (class="graphiql-editor" — the Monaco container)
  // We want to insert the bar before the HeaderEditor div (last child).
  const section = document.querySelector("section.graphiql-editor-tool");
  if (!section) return null;
  const editors = section.querySelectorAll(":scope > .graphiql-editor");
  if (!editors.length) return null;
  return editors[editors.length - 1] as HTMLElement;
}

export function HeadersQuickInsert() {
  const headerEditor = useGraphiQL((s) => s.headerEditor);
  const [headersText, setHeadersText] = useState("");
  const [portalTarget, setPortalTarget] = useState<HTMLElement | null>(null);
  const [headersActive, setHeadersActive] = useState(false);
  const barRef = useRef<HTMLElement | null>(null);

  // Subscribe to header editor content changes via Monaco API
  useEffect(() => {
    if (!headerEditor) return;
    setHeadersText(headerEditor.getValue() ?? "");
    const disposable = (headerEditor as unknown as {
      onDidChangeModelContent?: (cb: () => void) => { dispose: () => void };
    }).onDidChangeModelContent?.(() => {
      setHeadersText(headerEditor.getValue() ?? "");
    });
    return () => disposable?.dispose();
  }, [headerEditor]);

  // Mount bar inside the headers editor content area; watch for tab switches
  useEffect(() => {
    let observer: MutationObserver | null = null;

    const mount = () => {
      const headersEditorEl = findHeadersEditor();
      if (!headersEditorEl) return false;
      const parent = headersEditorEl.parentElement;
      if (!parent) return false;

      // Re-use existing bar if already injected
      let bar = parent.querySelector(":scope > .provisa-headers-bar") as HTMLElement | null;
      if (!bar) {
        bar = document.createElement("div");
        bar.className = "provisa-headers-bar";
        parent.insertBefore(bar, headersEditorEl);
      }
      barRef.current = bar;
      setPortalTarget(bar);

      const updateActive = () => setHeadersActive(isHeadersTabActive());
      updateActive();

      // Watch for aria-label changes on section.graphiql-editor-tool.
      // The section itself is the mutated element, so observe its parent with subtree:true.
      const section = document.querySelector("section.graphiql-editor-tool");
      const watchTarget = section?.parentElement ?? document.body;
      observer = new MutationObserver(updateActive);
      observer.observe(watchTarget, { attributes: true, subtree: true, attributeFilter: ["aria-label"] });
      return true;
    };

    if (!mount()) {
      const interval = setInterval(() => {
        if (mount()) clearInterval(interval);
      }, 200);
      return () => {
        clearInterval(interval);
        observer?.disconnect();
      };
    }
    return () => observer?.disconnect();
  }, []);

  const toggle = useCallback((def: HeaderDef) => {
    if (!headerEditor) return;
    const current = parseHeaders(headerEditor.getValue() ?? "");
    if (current[def.key] === def.defaultValue) {
      delete current[def.key];
    } else {
      current[def.key] = def.defaultValue;
    }
    const next = Object.keys(current).length ? JSON.stringify(current, null, 2) : "";
    headerEditor.setValue(next);
    setHeadersText(next);
  }, [headerEditor]);

  if (!portalTarget || !headersActive) return null;

  const headers = parseHeaders(headersText);

  return createPortal(
    <div className="provisa-headers-buttons">
      <span className="provisa-headers-label">X-Provisa</span>
      {PROVISA_HEADERS.map((def) => (
        <button
          key={`${def.key}:${def.defaultValue}`}
          className={`provisa-header-btn${headers[def.key] === def.defaultValue ? " active" : ""}`}
          title={def.title}
          onClick={() => toggle(def)}
        >
          {def.label}
        </button>
      ))}
    </div>,
    portalTarget,
  );
}
