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
import { glossaryGraph, ontologyQuery, type OntologyRows } from "./glossaryGraphData";
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
};

/** The ontology as ``scope`` reads it, in one query (REQ-2136); null when the role cannot read the
 * glossary. */
async function loadOntology(role: string, scope: QueryScope): Promise<GraphData | null> {
  const query = ontologyQuery(await metaReader(role), scope);
  if (query === null) return null;
  const rows = await dataGraphql<OntologyRows>(query, {}, role);
  return glossaryGraph(rows.glossary_terms, rows.glossary_term_edges ?? [], rows.data_products);
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
        variant={open ? "filled" : "default"}
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
  const [read, setRead] = useState<{ all: GraphData | null; error: string | null; done: boolean }>({
    all: null,
    error: null,
    done: false,
  });
  // What the controls keep, read afresh whenever they change; the last read is drawn meanwhile.
  const [scope, setScope] = useState<QueryScope | null>(null);
  const [data, setData] = useState<GraphData | null>(null);
  const [open, setOpen] = useState(false);
  useEffect(() => {
    if (actingRoles === null) return;
    let cancelled = false;
    loadOntology(actingRoles, EVERYTHING).then(
      (all) => !cancelled && setRead({ all, error: null, done: true }),
      (e: unknown) =>
        !cancelled &&
        setRead({ all: null, error: e instanceof Error ? e.message : String(e), done: true }),
    );
    return () => {
      cancelled = true;
    };
  }, [actingRoles, t]);
  useEffect(() => {
    if (actingRoles === null || scope === null) return;
    let cancelled = false;
    loadOntology(actingRoles, scope).then(
      (d) => !cancelled && setData(d),
      (e: unknown) =>
        !cancelled && setRead((r) => ({ ...r, error: e instanceof Error ? e.message : String(e) })),
    );
    return () => {
      cancelled = true;
    };
  }, [actingRoles, scope]);
  const onScope = useCallback((s: QueryScope) => setScope(s), []);

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
        data={data ?? EMPTY}
        query={{ all: read.all, onScope }}
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
        data={data ?? EMPTY}
        query={{ all: read.all, onScope }}
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
