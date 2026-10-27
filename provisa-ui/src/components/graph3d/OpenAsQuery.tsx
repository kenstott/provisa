// Copyright (c) 2026 Kenneth Stott
// Canary: 16574222-a0d2-4c00-8c9f-1a94bfa68d7b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2136: a graph view's "Open as query" (→): the query the view runs, its filters inside it,
// opened ready to edit in the surface chosen -- GraphQL (the query itself), SQL (its compiled
// semantic SQL), Cypher (where it maps to Cypher) or SPARQL (where its one root maps). A focus
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

/** What the query compiles to: its semantic SQL, its Cypher or why it has none, and its SPARQL or
 * why it has none. */
interface Compiled {
  sql: string;
  cypher: string | null;
  cypherError: string | null;
  sparql: string | null;
  /** Why there is no SPARQL: the root's own reason, or that the query has several roots. */
  sparqlWhy: "roots" | string | null;
}

function compiledOf(raw: CompileResult | { queries: CompileResult[] }): Compiled {
  const results = "queries" in raw ? raw.queries : [raw];
  // Each root field is its own Cypher statement, in the query's order, as it is its own SQL one;
  // a root with no Cypher form is said in its place, and leaves the others theirs.
  const translated = results.some((r) => !!r.compiled_cypher);
  const cypher = results
    .map((r) =>
      r.compiled_cypher ? r.compiled_cypher : `// ${r.root_field}: ${r.cypher_error ?? ""}`.trim(),
    )
    .join(";\n\n");
  return {
    sql: results.map((r) => r.semantic_sql).join(";\n\n"),
    cypher: translated ? cypher : null,
    cypherError: results.map((r) => r.cypher_error).find((e): e is string => !!e) ?? null,
    // The SPARQL endpoint answers one query: a read of several roots is several, opened one by one
    // from the GraphQL.
    sparql: results.length === 1 ? (results[0].compiled_sparql ?? null) : null,
    sparqlWhy:
      results.length !== 1
        ? "roots"
        : results[0].compiled_sparql
          ? null
          : (results[0].sparql_error ?? null),
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
    // A graph view opens as a graph: its Cypher is its nodes and relationships, which the frame
    // draws, not its fields.
    compileQuery(actingRoles, query, undefined, false, false, true).then(
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

  const sparqlWhy =
    unavailable ??
    current?.error ??
    (current
      ? current.result?.sparql
        ? null
        : current.result?.sparqlWhy === "roots"
          ? t("graph3d.openAsQuerySparqlRoots")
          : (current.result?.sparqlWhy ?? t("graph3d.openAsQueryNoSparql"))
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
            variant="subtle"
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
        {item("sparql", sparqlWhy, () =>
          navigate("/sparql", { state: { query: current?.result?.sparql } }),
        )}
      </Menu.Dropdown>
    </Menu>
  );
}
