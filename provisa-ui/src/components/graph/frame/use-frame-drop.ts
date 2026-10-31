// Copyright (c) 2026 Kenneth Stott
// Canary: 7a3e9c15-0d62-4b78-a4f1-c86e2d7b5f03
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, type DragEvent } from "react";
import { EDGE_DRAG, PROPERTY_DRAG, type DropNode, type SparqlDrop } from "../sparqlDrop";

/** What a sidebar pill carries to a frame. */
const LABEL_DRAG = "text/x-provisa-label";
const DOMAIN_DRAG = "text/x-provisa-domain";
const DROPPABLE = [LABEL_DRAG, DOMAIN_DRAG, EDGE_DRAG, PROPERTY_DRAG];

/** What a dragged SPARQL vocabulary pill asks of its frame. */
function sparqlDropOf(data: DataTransfer): SparqlDrop | null {
  const iri = data.getData(LABEL_DRAG);
  if (iri) return { kind: "class", iri };
  const prefix = data.getData(DOMAIN_DRAG);
  if (prefix) return { kind: "domain", prefix };
  const edge = data.getData(EDGE_DRAG);
  if (edge) return { kind: "edge", iri: edge };
  const predicate = data.getData(PROPERTY_DRAG);
  return predicate ? { kind: "predicate", iri: predicate } : null;
}

/** A frame as a drop target: a pill on its way, and what one let go on the frame asks. */
export function useFrameDrop({
  frameId,
  onTableDrop,
  onDomainDrop,
  onSparqlDrop,
  nodeAt,
}: {
  frameId: string;
  onTableDrop?: (frameId: string, compoundLabel: string) => void;
  onDomainDrop?: (frameId: string, domainLabel: string) => void;
  onSparqlDrop?: (frameId: string, drop: SparqlDrop, on: DropNode | null) => void;
  /** The drawn node at a point of the page, if any. */
  nodeAt: (x: number, y: number) => DropNode | null;
}) {
  const [dragOver, setDragOver] = useState(false);
  return {
    dragOver,
    dropTarget: {
      onDragOver: (e: DragEvent) => {
        if (DROPPABLE.some((type) => e.dataTransfer.types.includes(type))) {
          e.preventDefault();
          setDragOver(true);
        }
      },
      onDragLeave: () => setDragOver(false),
      onDrop: (e: DragEvent) => {
        setDragOver(false);
        const asked = onSparqlDrop && sparqlDropOf(e.dataTransfer);
        if (asked) {
          e.preventDefault();
          onSparqlDrop(frameId, asked, nodeAt(e.clientX, e.clientY));
          return;
        }
        const label = e.dataTransfer.getData(LABEL_DRAG);
        if (label && onTableDrop) {
          e.preventDefault();
          onTableDrop(frameId, label);
          return;
        }
        const domain = e.dataTransfer.getData(DOMAIN_DRAG);
        if (domain && onDomainDrop) {
          e.preventDefault();
          onDomainDrop(frameId, domain);
        }
      },
    },
  };
}
