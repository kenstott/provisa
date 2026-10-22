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

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Loader } from "@mantine/core";
import { Network } from "lucide-react";
import { useAuth } from "../../context/AuthContext";
import { dataGraphql } from "../../api/metaGraph";
import { GraphExplorer } from "./GraphExplorer";
import { GLOSSARY_SCOPE, type GraphData } from "./graphModel";
import {
  glossaryGraph,
  termEdgesQuery,
  termsQuery,
  type ProposalReason,
  type TermEdgeRow,
  type TermRow,
} from "./glossaryGraphData";
import { GraphModal } from "./KnowledgeGraphPanel";
import { metaReader } from "./useProductGraph";

async function loadGlossaryGraph(
  role: string,
  say: (reason: ProposalReason) => string,
): Promise<GraphData | null> {
  const m = await metaReader(role);
  const tq = termsQuery(m);
  if (tq === null) return null;
  const eq = termEdgesQuery(m);
  const [{ glossary_terms: terms }, edges] = await Promise.all([
    dataGraphql<{ glossary_terms: TermRow[] }>(tq, {}, role),
    eq === null
      ? Promise.resolve([] as TermEdgeRow[])
      : dataGraphql<{ glossary_term_edges: TermEdgeRow[] }>(eq, {}, role).then(
          (r) => r.glossary_term_edges,
        ),
  ]);
  return glossaryGraph(terms, edges, say);
}

/** The Glossary page's control opening the Ontology View in place of the term list. */
export function OntologyToggle({
  open,
  onChange,
}: {
  open: boolean;
  onChange: (open: boolean) => void;
}) {
  const { t } = useTranslation();
  return (
    <Button
      variant={open ? "filled" : "default"}
      leftSection={<Network size={14} />}
      onClick={() => onChange(!open)}
      aria-pressed={open}
      data-testid="glossary-ontology-toggle"
    >
      {t("graph3d.ontologyView")}
    </Button>
  );
}

export function GlossaryOntologyView() {
  const { t } = useTranslation();
  const { actingRoles } = useAuth();
  const [read, setRead] = useState<{ data: GraphData | null; error: string | null; done: boolean }>(
    { data: null, error: null, done: false },
  );
  const [open, setOpen] = useState(false);
  useEffect(() => {
    if (actingRoles === null) return;
    let cancelled = false;
    loadGlossaryGraph(actingRoles, (reason) => t(`graph3d.why_${reason}`)).then(
      (data) => !cancelled && setRead({ data, error: null, done: true }),
      (e: unknown) =>
        !cancelled &&
        setRead({ data: null, error: e instanceof Error ? e.message : String(e), done: true }),
    );
    return () => {
      cancelled = true;
    };
  }, [actingRoles, t]);

  if (read.error) {
    return (
      <Alert color="red" data-testid="ontology-view-error">
        {t("graph3d.loadError", { error: read.error })}
      </Alert>
    );
  }
  if (!read.done) return <Loader size="sm" data-testid="ontology-view-loading" />;
  if (read.data === null) {
    return (
      <Alert color="yellow" data-testid="ontology-view-unreadable">
        {t("graph3d.glossaryUnreadable")}
      </Alert>
    );
  }
  return (
    <div data-testid="ontology-view">
      <GraphExplorer
        data={read.data}
        scope={GLOSSARY_SCOPE}
        preset="ontology"
        zero={[]}
        onExpand={() => setOpen(true)}
        testId="ontology"
      />
      <GraphModal
        opened={open}
        onClose={() => setOpen(false)}
        title={t("graph3d.ontologyTitle")}
        data={read.data}
        scope={GLOSSARY_SCOPE}
        preset="ontology"
        focus={null}
        zero={[]}
      />
    </div>
  );
}
