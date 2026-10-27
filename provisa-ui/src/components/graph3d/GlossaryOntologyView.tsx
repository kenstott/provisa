// Copyright (c) 2026 Kenneth Stott
// Canary: 2c7e9a14-5b38-4f60-8d21-e6a3b9f4c705
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2130: the Glossary page's Ontology View -- the data product graph's engine scoped to the
// whole glossary, drawn in place with the same layer, filter and preset controls, and opened at
// 95% of the viewport by its expand button. Its data is read from the meta domain as the user
// when the view is opened; three.js loads then too.

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Alert, Loader } from "@mantine/core";
import { Network } from "lucide-react";
import { useAuth } from "../../context/AuthContext";
import { dataGraphql } from "../../api/metaGraph";
import { GraphExplorer } from "./GraphExplorer";
import { GLOSSARY_SCOPE, type GraphData, type LayerId, type QueryScope } from "./graphModel";
import {
  flatTerms,
  glossaryGraph,
  ontologyCountQuery,
  ontologyQuery,
  overviewGraph,
  termEdges,
  type CountRow,
  type OntologyRows,
} from "./glossaryGraphData";
import { NODE_BUDGET } from "./neighbourhood";
import { GraphModal } from "./KnowledgeGraphPanel";
import { Tip } from "../Tip";
import { metaReader } from "./useProductGraph";
import type { GraphKey } from "../../pages/tables/profile3d/graphSettings";

// Before the first scoped read: nothing drawn yet.
const EMPTY: GraphData = { nodes: [], edges: [] };

/** Every layer, no filter: what the view's controls choose from. */
const EVERYTHING: QueryScope = {
  layers: new Set(GLOSSARY_SCOPE.layerOrder as LayerId[]),
  domains: [],
  edgeTypes: [],
  hiddenStatuses: [],
  orphans: true,
  tags: [],
  sensitivities: [],
  search: [],
};

/** What a read of the ontology came to: the graph and the query that read it, or -- past the
 * budget -- how many terms it would have read, with each term's domains. */
type Loaded =
  | { data: GraphData; query: string; over?: undefined }
  | { over: number; rows: CountRow[]; query: string };

/** The ontology as ``scope`` reads it, in one query (REQ-2136): its terms counted first, and read
 * only within the budget (REQ-2143); null when the role cannot read the glossary. */
async function loadOntology(role: string, scope: QueryScope): Promise<Loaded | null> {
  const m = await metaReader(role);
  const query = ontologyQuery(m, scope);
  const count = ontologyCountQuery(m, scope);
  if (query === null || count === null) return null;
  const counted = (await dataGraphql<{ _terms: CountRow[] }>(count, {}, role))._terms;
  if (counted.length > NODE_BUDGET) return { over: counted.length, rows: counted, query };
  const rows = await dataGraphql<OntologyRows>(query, {}, role);
  // A search's neighbourhood is read nested, a level to each hop: each term once, flat.
  const terms = flatTerms(rows._terms);
  return { data: glossaryGraph(terms, termEdges(terms), rows.data_products), query };
}

/** The Glossary page's control opening the Ontology View in place of the term list. */
/** The org's one ontology graph, in place and expanded: one set of remembered settings. */
const ONTOLOGY_SETTINGS: GraphKey = { kind: "ontology", subject: null };

export function OntologyToggle({
  open,
  onChange,
}: {
  open: boolean;
  onChange: (open: boolean) => void;
}) {
  const { t } = useTranslation();
  // An icon in the page's toolbar, named by its tip and its label, filled while the view is on.
  return (
    <Tip text={t("graph3d.ontologyView")} named>
      <ActionIcon
        size="input-sm"
        variant={open ? "light" : "subtle"}
        onClick={() => onChange(!open)}
        aria-label={t("graph3d.ontologyView")}
        aria-pressed={open}
        data-testid="glossary-ontology-toggle"
      >
        <Network size={16} aria-hidden />
      </ActionIcon>
    </Tip>
  );
}

/** ``search``: the Glossary page's search, which focuses the graph (searchItems' rule). */
export function GlossaryOntologyView({ search }: { search: string }) {
  const { t } = useTranslation();
  const { actingRoles } = useAuth();
  const said = useCallback(
    (domain: string, count: number) =>
      t("graph3d.domainCount", {
        domain: domain === "*" ? t("glossaryTab.enterpriseDomainLabel") : domain,
        count,
      }),
    [t],
  );
  const [read, setRead] = useState<{ all: GraphData | null; error: string | null; done: boolean }>({
    all: null,
    error: null,
    done: false,
  });
  // What the controls keep, read afresh whenever they change; the last read is drawn meanwhile.
  const [scope, setScope] = useState<QueryScope | null>(null);
  // The scoped read: what is drawn, and the query that read it (Open as query's).
  const [scoped, setScoped] = useState<Loaded | null>(null);
  const [open, setOpen] = useState(false);
  // Polly's glossary tools change the ontology server-side (ChatPanel's provisa:glossary-changed):
  // the view reads it again, as the term list does.
  const [changes, setChanges] = useState(0);
  useEffect(() => {
    const changed = () => setChanges((n) => n + 1);
    window.addEventListener("provisa:glossary-changed", changed);
    return () => window.removeEventListener("provisa:glossary-changed", changed);
  }, []);
  useEffect(() => {
    if (actingRoles === null) return;
    let cancelled = false;
    loadOntology(actingRoles, EVERYTHING).then(
      (all) =>
        !cancelled &&
        setRead({
          // Past the budget the choices are the overview's: the domains.
          all:
            all === null ? null : all.over === undefined ? all.data : overviewGraph(all.rows, said),
          error: null,
          done: true,
        }),
      (e: unknown) =>
        !cancelled &&
        setRead({ all: null, error: e instanceof Error ? e.message : String(e), done: true }),
    );
    return () => {
      cancelled = true;
    };
  }, [actingRoles, said, changes]);
  useEffect(() => {
    if (actingRoles === null || scope === null) return;
    let cancelled = false;
    loadOntology(actingRoles, scope).then(
      (d) => !cancelled && setScoped(d),
      (e: unknown) =>
        !cancelled && setRead((r) => ({ ...r, error: e instanceof Error ? e.message : String(e) })),
    );
    return () => {
      cancelled = true;
    };
  }, [actingRoles, scope, changes]);
  const onScope = useCallback((s: QueryScope) => setScope(s), []);
  // REQ-2143: past the budget, the whole ontology unsearched is its overview -- each domain with
  // its count, picked to narrow to it -- and a search or a domain still past it is said.
  const overview =
    scoped?.over !== undefined &&
    scope !== null &&
    scope.search.length === 0 &&
    scope.domains.length === 0;
  const drawn: GraphData =
    scoped === null
      ? EMPTY
      : scoped.over === undefined
        ? scoped.data
        : overview
          ? overviewGraph(scoped.rows, said)
          : EMPTY;
  const notice =
    scoped?.over !== undefined && !overview
      ? t("graph3d.overBudget", { count: scoped.over })
      : null;
  const asQuery = (all: GraphData) => ({
    all,
    onScope,
    text: scoped?.query ?? null,
    notice,
    overview,
  });

  if (read.error) {
    return (
      <Alert color="red" data-testid="ontology-view-error">
        {t("graph3d.loadError", { error: read.error })}
      </Alert>
    );
  }
  if (!read.done) return <Loader size="sm" data-testid="ontology-view-loading" />;
  if (read.all === null) {
    return (
      <Alert color="yellow" data-testid="ontology-view-unreadable">
        {t("graph3d.glossaryUnreadable")}
      </Alert>
    );
  }
  return (
    // In place of the term list it takes the list's space: the stage fills down to the page's foot.
    <div
      data-testid="ontology-view"
      style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column" }}
    >
      <GraphExplorer
        data={drawn}
        query={asQuery(read.all)}
        scope={GLOSSARY_SCOPE}
        preset="ontology"
        zero={[]}
        search={search}
        onExpand={() => setOpen(true)}
        settingsKey={ONTOLOGY_SETTINGS}
        exportName="ontology"
        testId="ontology"
      />
      <GraphModal
        opened={open}
        onClose={() => setOpen(false)}
        title={t("graph3d.ontologyTitle")}
        data={drawn}
        query={asQuery(read.all)}
        scope={GLOSSARY_SCOPE}
        preset="ontology"
        focus={null}
        zero={[]}
        search={search}
        settingsKey={ONTOLOGY_SETTINGS}
        exportName="ontology"
      />
    </div>
  );
}
