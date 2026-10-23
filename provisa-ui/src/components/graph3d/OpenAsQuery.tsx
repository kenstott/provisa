// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2136: a graph view's "Open as query" (→): the query the view runs, its filters inside it,
// opened ready to edit in the surface chosen -- GraphQL (the query itself), SQL (its compiled
// semantic SQL) or Cypher (where it maps to Cypher). SPARQL waits for its endpoint. A focus
// (the page's search, or a node focused) is drawn as a neighbourhood the query does not state,
// so with one set the view does not open as its query.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router-dom";
import { ActionIcon, Menu } from "@mantine/core";
import { ArrowRight } from "lucide-react";
import { useAuth } from "../../context/AuthContext";
import { useCompileQuery } from "../../hooks/useAdminQueries";
import type { CompileResult } from "../../api/admin";
import { Tip } from "../Tip";

/** What the query compiles to: its semantic SQL, and its Cypher or why it has none. */
interface Compiled {
  sql: string;
  cypher: string | null;
  cypherError: string | null;
}

function compiledOf(raw: CompileResult | { queries: CompileResult[] }): Compiled {
  const results = "queries" in raw ? raw.queries : [raw];
  const cypher = results.map((r) => r.compiled_cypher).filter((c): c is string => !!c);
  return {
    sql: results.map((r) => r.semantic_sql).join(";\n\n"),
    // The server combines a query's roots into one Cypher statement when they combine.
    cypher: cypher.length > 0 ? cypher[0] : null,
    cypherError: results.map((r) => r.cypher_error).find((e): e is string => !!e) ?? null,
  };
}

export function OpenAsQuery({
  query,
  focused,
  testId,
}: {
  /** The view's query as it runs now; null while there is none (not read yet). */
  query: string | null;
  /** A focus is set: the view draws a neighbourhood its query does not state. */
  focused: boolean;
  testId: string;
}) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const { actingRoles } = useAuth();
  const { compileQuery } = useCompileQuery();
  // Compiled when the menu opens, for the query then; a failure is said in place of SQL and Cypher.
  const [compiled, setCompiled] = useState<{
    query: string;
    result: Compiled | null;
    error: string | null;
  } | null>(null);
  const current = compiled !== null && compiled.query === query ? compiled : null;

  const compile = () => {
    if (query === null || actingRoles === null || focused || current) return;
    compileQuery(actingRoles, query).then(
      (raw) => setCompiled({ query, result: compiledOf(raw), error: null }),
      (e: unknown) =>
        setCompiled({ query, result: null, error: e instanceof Error ? e.message : String(e) }),
    );
  };

  const unavailable = focused ? t("graph3d.openAsQueryFocused") : null;
  const sqlWhy =
    unavailable ?? current?.error ?? (current ? null : t("graph3d.openAsQueryCompiling"));
  const cypherWhy =
    unavailable ??
    current?.error ??
    (current
      ? current.result?.cypher
        ? null
        : (current.result?.cypherError ?? t("graph3d.openAsQueryNoCypher"))
      : t("graph3d.openAsQueryCompiling"));

  const item = (target: string, why: string | null, open: () => void) => {
    const entry = (
      <Menu.Item disabled={why !== null} onClick={open} data-testid={`${testId}-open-as-${target}`}>
        {t(`graph3d.openAs_${target}`)}
      </Menu.Item>
    );
    return why === null ? (
      entry
    ) : (
      <Tip key={target} text={why}>
        <div>{entry}</div>
      </Tip>
    );
  };

  return (
    <Menu position="bottom-end" withinPortal transitionProps={{ duration: 0 }} onOpen={compile}>
      <Menu.Target>
        <Tip text={t("graph3d.openAsQuery")} named>
          <ActionIcon
            size="input-xs"
            variant="default"
            disabled={query === null}
            aria-label={t("graph3d.openAsQuery")}
            data-testid={`${testId}-open-as-query`}
          >
            <ArrowRight size={14} aria-hidden />
          </ActionIcon>
        </Tip>
      </Menu.Target>
      <Menu.Dropdown>
        {item("graphql", unavailable, () => navigate("/query", { state: { query } }))}
        {item("sql", sqlWhy, () => navigate("/sql", { state: { sql: current?.result?.sql } }))}
        {item("cypher", cypherWhy, () =>
          navigate("/graph", { state: { query: current?.result?.cypher } }),
        )}
        {item("sparql", t("graph3d.openAsQuerySparqlLater"), () => undefined)}
      </Menu.Dropdown>
    </Menu>
  );
}
