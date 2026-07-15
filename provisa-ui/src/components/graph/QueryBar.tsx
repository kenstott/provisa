// Copyright (c) 2026 Kenneth Stott
// Canary: a7aa0c99-822a-4e7a-ac73-a4faacbe450c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useRef, useEffect, useCallback } from "react";
import { ActionIcon } from "@mantine/core";
import { CirclePlus, Play } from "lucide-react";
import { useTranslation } from "react-i18next";
import CodeMirror from "@uiw/react-codemirror";
import * as _neo4jCypherMod from "@neo4j-cypher/codemirror";
import "@neo4j-cypher/codemirror/css/cypher-codemirror.css";
/* eslint-disable @typescript-eslint/no-explicit-any -- @neo4j-cypher/codemirror ships no type declarations; its named exports must be destructured from an untyped module */
const { getCypherLanguageExtensions, useAutocompleteExtensions, cypherLinter } =
  _neo4jCypherMod as any;
/* eslint-enable @typescript-eslint/no-explicit-any */
import { oneDark } from "@codemirror/theme-one-dark";
import { EditorView, keymap } from "@codemirror/view";
import { Prec } from "@codemirror/state";
import { CopySymbolButton } from "../CopyButton";
import type { CypherSchema } from "./graph-schema-types";

interface QueryBarProps {
  onRun: (query: string) => void;
  initialQuery?: string;
  onQueryChange: (q: string) => void;
  cypherSchema?: CypherSchema;
  autoImpute: boolean;
  onToggleAutoImpute: () => void;
}

// Polyfill: @neo4j-cypher/codemirror 1.x calls view.newContentVersion() which doesn't exist on current @codemirror/view
const _evProto = EditorView.prototype as unknown as { newContentVersion?: () => number };
if (!_evProto.newContentVersion) {
  let _ver = 0;
  _evProto.newContentVersion = function () {
    return ++_ver;
  };
}

/* eslint-disable-next-line @typescript-eslint/no-explicit-any --
   getCypherLanguageExtensions comes from the untyped @neo4j-cypher/codemirror module and accepts an untyped options object */
const _cypherLangExts = getCypherLanguageExtensions({ cypherLanguage: true } as any);

export function QueryBar({
  onRun,
  initialQuery,
  onQueryChange,
  cypherSchema,
  autoImpute,
  onToggleAutoImpute,
}: QueryBarProps) {
  const { t } = useTranslation();
  const [query, setQuery] = useState(initialQuery ?? "MATCH (n) RETURN n LIMIT 25");
  const viewRef = useRef<EditorView | null>(null);
  const [focused, setFocused] = useState(false);
  const pendingFocusRef = useRef(false);

  useEffect(() => {
    if (!cypherSchema || !viewRef.current) return;
    try {
      // editorSupportField is included by getCypherLanguageExtensions, but the
      // package ships no type declaration for this CommonJS subpath.
      type CypherStateDefs = {
        editorSupportField: import("@codemirror/state").StateField<{
          setSchema: (s: CypherSchema) => void;
        }>;
      };
      /* eslint-disable @typescript-eslint/no-require-imports -- optional CommonJS subpath resolved at runtime inside try/catch; a static import would throw at module load when the subpath is unavailable */
      const _cypherStateDefs = require("@neo4j-cypher/codemirror/lib/cypher-state-definitions");
      /* eslint-enable @typescript-eslint/no-require-imports */
      const { editorSupportField } = _cypherStateDefs as CypherStateDefs;
      const editorSupport = viewRef.current.state.field(editorSupportField, false);
      if (editorSupport) editorSupport.setSchema(cypherSchema);
    } catch {
      /* subpath not resolved */
    }
  }, [cypherSchema]);

  const handleChange = (val: string) => {
    setQuery(val);
    onQueryChange(val);
  };

  const handleCollapsedClickWithFormat = useCallback(() => {
    const formatted = query
      .replace(/\s*\n\s*/g, " ")
      .replace(
        /\s+\b(MATCH|OPTIONAL MATCH|WHERE|WITH|RETURN|ORDER BY|LIMIT|SKIP|UNION ALL|UNION|CREATE|SET|DELETE|DETACH DELETE|MERGE|CALL|UNWIND)\b/gi,
        "\n$1",
      )
      .trimStart();
    if (formatted !== query) {
      setQuery(formatted);
      onQueryChange(formatted);
    }
    setFocused(true);
    pendingFocusRef.current = true;
  }, [query, onQueryChange]);

  return (
    <div className="graph-query-bar">
      <div className="graph-query-prompt">$</div>
      <div className="graph-query-editor-wrap">
        {!focused && (
          <div
            className="gf-header-query-collapsed"
            onClick={handleCollapsedClickWithFormat}
            title={query}
          >
            {query.replace(/\s*\n\s*/g, " ") || "MATCH (n) RETURN n LIMIT 25"}
          </div>
        )}
        {focused && (
          <CodeMirror
            className="graph-query-input"
            value={query}
            theme={oneDark}
            extensions={[
              ..._cypherLangExts,
              cypherLinter({ showErrors: false }),
              useAutocompleteExtensions,
              EditorView.lineWrapping,
              Prec.highest(
                keymap.of([
                  {
                    key: "Mod-Enter",
                    run: () => {
                      onRun(query.trim());
                      return true;
                    },
                  },
                  {
                    key: "Enter",
                    run: () => {
                      onRun(query.trim());
                      return true;
                    },
                  },
                ]),
              ),
            ]}
            onCreateEditor={(view) => {
              viewRef.current = view;
              if (pendingFocusRef.current) {
                pendingFocusRef.current = false;
                view.focus();
              }
            }}
            onUpdate={(vu) => {
              if (vu.focusChanged && !vu.view.hasFocus) setFocused(false);
            }}
            onChange={handleChange}
            basicSetup={{
              lineNumbers: false,
              foldGutter: false,
              highlightActiveLine: false,
              completionKeymap: false,
            }}
            placeholder="MATCH (n) RETURN n LIMIT 25"
          />
        )}
        <CopySymbolButton text={query} className="gf-copy-query-btn" title="Copy query" />
      </div>
      <ActionIcon
        className={`gf-icon-btn${autoImpute ? " gf-icon-btn--on" : ""}`}
        variant={autoImpute ? "filled" : "subtle"}
        aria-label={t(autoImpute ? "queryBar.autoImputeOn" : "queryBar.autoImputeOff")}
        aria-pressed={autoImpute}
        title={t(autoImpute ? "queryBar.autoImputeOn" : "queryBar.autoImputeOff")}
        onClick={onToggleAutoImpute}
        style={{ marginRight: 4, alignSelf: "stretch", height: "auto", width: 38 }}
      >
        <CirclePlus size={16} aria-hidden />
      </ActionIcon>
      <ActionIcon
        className="graph-run-btn"
        aria-label={t("queryBar.run")}
        title={t("queryBar.run")}
        onClick={() => onRun(query.trim())}
      >
        <Play size={16} aria-hidden />
      </ActionIcon>
    </div>
  );
}
