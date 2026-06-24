// Copyright (c) 2026 Kenneth Stott
// Canary: a7aa0c99-822a-4e7a-ac73-a4faacbe450c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useRef, useEffect } from "react";
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
  statsEnabled: boolean;
  onToggleStats: () => void;
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
  statsEnabled,
  onToggleStats,
}: QueryBarProps) {
  const [query, setQuery] = useState(initialQuery ?? "MATCH (n) RETURN n LIMIT 25");
  const viewRef = useRef<EditorView | null>(null);

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
      // @ts-expect-error -- @neo4j-cypher/codemirror ships no Node/CommonJS types, so `require` is not declared here
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

  return (
    <div className="graph-query-bar">
      <div className="graph-query-prompt">$</div>
      <div className="graph-query-editor-wrap">
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
        <CopySymbolButton text={query} className="gf-copy-query-btn" title="Copy query" />
      </div>
      <button
        className={`gf-icon-btn${autoImpute ? " gf-icon-btn--on" : ""}`}
        onClick={onToggleAutoImpute}
        title={
          autoImpute
            ? "Auto-impute relationships ON — click to disable"
            : "Auto-impute relationships between visible nodes"
        }
        style={{ marginRight: 4 }}
      >
        ⊕
      </button>
      <label style={{ display: "flex", alignItems: "center", gap: "0.3rem", fontSize: "0.8rem", cursor: "pointer", marginRight: 8 }}>
        <input
          type="checkbox"
          checked={statsEnabled}
          onChange={onToggleStats}
          style={{ marginRight: 2 }}
        />
        Query Stats
      </label>
      <button className="graph-run-btn" onClick={() => onRun(query.trim())} title="Run query (⌘↵)">
        ▶
      </button>
    </div>
  );
}
