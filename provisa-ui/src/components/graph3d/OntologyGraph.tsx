// Copyright (c) 2026 Kenneth Stott
// Canary: 2c7e9a14-5b38-4f60-8d21-e6a3b9f4c705
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-10-28, ONE ONTOLOGY GRAPH; EVERY OTHER GRAPH IS A SNIPPET OF IT): the one
// graph -- the model's ontology in lanes: terms, the columns they bind, their tables, the products
// those tables belong to -- drawn by one component. Every other graph is a snippet of it, named by
// a focus and a depth: a term's page opens it focused on the term, a product's panel on the
// product, and the Ontology View (REQ-2130) shows it whole. Expanding a snippet opens the whole
// ontology with the snippet's focus selected. Its data is read from the ontology schema as the
// user (ontologyData.ts) once the graph is in view; three.js loads then too.

import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Alert, Button, Loader } from "@mantine/core";
import { Network } from "lucide-react";
import { FitModal } from "../../pages/tables/profile3d/Expandable3D";
import type { GraphKey } from "../../pages/tables/profile3d/graphSettings";
import { GraphExplorer } from "./GraphExplorer";
import { ONTOLOGY_SCOPE, type LayerId } from "./graphModel";
import { WHOLE, type SnippetPreset } from "./ontologyPresets";
import { SNIPPET_LAYERS, useOntology, type Read } from "./useOntology";
import { useInView } from "./useInView";
import { Tip } from "../Tip";
import "../../pages/tables/profile3d/profile3d.css";
import "./graph3d.css";

/** The org's one ontology graph, in place and expanded: one set of remembered settings. */
const ONTOLOGY_SETTINGS: GraphKey = { kind: "ontology", subject: null };

/** A read not yet answered, failed, or the role's to make: said in place of the graph. */
function ReadState({ read, testId }: { read: Read; testId: string }) {
  const { t } = useTranslation();
  if (read.error)
    return (
      <Alert color="red" data-testid={`${testId}-error`}>
        {t("graph3d.loadError", { error: read.error })}
      </Alert>
    );
  if (!read.done) return <Loader size="sm" data-testid={`${testId}-loading`} />;
  return (
    <Alert color="yellow" data-testid={`${testId}-unreadable`}>
      {t("graph3d.glossaryUnreadable")}
    </Alert>
  );
}

/** The whole ontology at 95% of the viewport, read when it opens, with ``selected`` -- the
 * snippet's focus it was expanded from -- selected. ``search`` is the page's, as GraphExplorer
 * takes it. */
export function OntologyModal({
  opened,
  onClose,
  title,
  selected,
  search,
  layers,
  settingsKey = ONTOLOGY_SETTINGS,
}: {
  opened: boolean;
  onClose: () => void;
  title: string;
  selected: string | null;
  search?: string;
  /** The layers it opens with, where they are not its preset's (the Product map's products). */
  layers?: LayerId[];
  /** Its remembered settings: the whole ontology's, unless it is a view of its own. */
  settingsKey?: GraphKey;
}) {
  return (
    <FitModal opened={opened} onClose={onClose} title={title} testId="graph3d-modal">
      {opened && (
        <WholeOntology
          selected={selected}
          search={search}
          layers={layers}
          settingsKey={settingsKey}
        />
      )}
    </FitModal>
  );
}

function WholeOntology({
  selected,
  search,
  layers,
  settingsKey,
}: {
  selected: string | null;
  search?: string;
  layers?: LayerId[];
  settingsKey: GraphKey;
}) {
  const { read, drawn, query } = useOntology(WHOLE, true);
  if (read.error || read.all === null) return <ReadState read={read} testId="graph3d" />;
  return (
    <GraphExplorer
      data={drawn}
      query={query(read.all)}
      scope={ONTOLOGY_SCOPE}
      preset="ontology"
      selected={selected}
      depth={WHOLE.depth}
      layers={layers}
      search={search}
      settingsKey={settingsKey}
      exportName="ontology"
      testId="graph3d"
    />
  );
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

/** The Ontology page's graph: the whole ontology in place of the term list, focused by the page's
 * ``search`` (searchItems' rule), expanding to 95% of the viewport. */
export function OntologyView({ search }: { search: string }) {
  const { t } = useTranslation();
  const { read, drawn, query } = useOntology(WHOLE, true);
  const [open, setOpen] = useState(false);
  if (read.error || read.all === null) return <ReadState read={read} testId="ontology-view" />;
  return (
    // In place of the term list it takes the list's space: the stage fills down to the page's foot.
    <div
      data-testid="ontology-view"
      style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column" }}
    >
      <GraphExplorer
        data={drawn}
        query={query(read.all)}
        scope={ONTOLOGY_SCOPE}
        preset="ontology"
        depth={WHOLE.depth}
        search={search}
        onExpand={() => setOpen(true)}
        settingsKey={ONTOLOGY_SETTINGS}
        exportName="ontology"
        testId="ontology"
      />
      <OntologyModal
        opened={open}
        onClose={() => setOpen(false)}
        title={t("graph3d.ontologyTitle")}
        selected={null}
        search={search}
      />
    </div>
  );
}

/** A snippet of the ontology -- a term's page's (focused on the term), a product's panel's
 * (focused on the product) -- read and drawn once it scrolls into view. Its expand button opens
 * the whole ontology with its focus selected. ``name`` names its exports and its expanded view. */
export function OntologySnippet({
  preset,
  name,
  testId,
}: {
  preset: SnippetPreset;
  name: string;
  testId: string;
}) {
  const { t } = useTranslation();
  const { ref, seen } = useInView<HTMLDivElement>();
  const { read, drawn, query } = useOntology(preset, seen);
  const [open, setOpen] = useState(false);
  // Each snippet remembers its own 2D/3D choice, apart from the whole ontology's settings.
  const settingsKey = useMemo(
    () => ({ kind: "ontology-snippet", subject: preset.focus }),
    [preset.focus],
  );
  return (
    <div ref={ref} data-testid={`${testId}-snippet`}>
      {seen && !read.error && read.all !== null ? (
        <GraphExplorer
          data={drawn}
          query={query(read.all)}
          scope={ONTOLOGY_SCOPE}
          preset="ontology"
          layers={SNIPPET_LAYERS}
          focus={preset.focus}
          depth={preset.depth}
          snippet
          onExpand={() => setOpen(true)}
          settingsKey={settingsKey}
          exportName={name}
          testId={testId}
        />
      ) : (
        // Until it is read and drawn it holds the panel's place under the panel's test id: the
        // card is not empty, the reader (or a test) can scroll to it, and drawing does not move
        // the page (REQ-2128: what loads in view is the panel's content, not the panel).
        <div className="g3d-panel" data-testid={testId}>
          <div className="g3d-panel-bar" />
          <div className="p3d g3d-panel-view g3d-panel-wait">
            {seen && <ReadState read={read} testId={testId} />}
          </div>
        </div>
      )}
      <OntologyModal
        opened={open}
        onClose={() => setOpen(false)}
        title={t("graph3d.expandTitle", { product: name })}
        selected={preset.focus}
      />
    </div>
  );
}

/** The Product map's layers: the ontology's, with every product (the Data layer). */
const PRODUCT_MAP_LAYERS: LayerId[] = ["ontology", "domains", "columns", "data"];
const PRODUCT_MAP_SETTINGS: GraphKey = { kind: "product-map", subject: null };

/** The Data Products page's entry point: the whole ontology, every product among it. */
export function ProductMapButton() {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  return (
    <>
      <Button
        size="xs"
        variant="default"
        leftSection={<Network size={14} />}
        onClick={() => setOpen(true)}
        data-testid="product-map-open"
      >
        {t("graph3d.productMap")}
      </Button>
      <OntologyModal
        opened={open}
        onClose={() => setOpen(false)}
        title={t("graph3d.productMap")}
        selected={null}
        layers={PRODUCT_MAP_LAYERS}
        settingsKey={PRODUCT_MAP_SETTINGS}
      />
    </>
  );
}
