// Copyright (c) 2026 Kenneth Stott
// Canary: 3d9e7b16-c0a2-4f58-a174-5b86e2f1d093
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135: what the SPARQL tab holds beside the frames: the role's vocabulary, the prefixes in use
// with the viewer's edits, and what expanding a node or dropping a class merges into a frame.

import { useCallback, useEffect, useMemo, useRef, useState, type RefObject } from "react";
import type { FrameData, SparqlDescribed } from "./graph-model";
import { settingKey, useLocalStorage } from "./graph-persistence";
import {
  classConstruct,
  describeQuery,
  effectivePrefixes,
  parseNTriples,
  refusalText,
  type PrefixEdits,
  type SparqlVocabulary,
  type Triple,
} from "./sparql";

/** The DESCRIBEs a class's nodes are read by at once. One resource each: several in one query
 * count against graph.union_limit together. */
const DESCRIBE_AT_ONCE = 4;

export function useSparqlExplorer({
  active,
  roleHeader,
  framesRef,
  changeFrame,
}: {
  /** Whether the page is the SPARQL tab: the Cypher tab reads none of this. */
  active: boolean;
  roleHeader: string | undefined;
  framesRef: RefObject<FrameData[]>;
  changeFrame: (id: string, change: (frame: FrameData) => Partial<FrameData>) => void;
}) {
  // The role's vocabulary (read again when the role changes).
  const [vocabulary, setVocabulary] = useState<SparqlVocabulary | null>(null);
  const [vocabularyError, setVocabularyError] = useState<string | null>(null);
  useEffect(() => {
    if (!active) return;
    const headers: Record<string, string> = {};
    if (roleHeader) headers["X-Provisa-Role"] = roleHeader;
    let cancelled = false;
    fetch("/data/sparql/vocabulary", { headers })
      .then(async (r) => {
        // A refused request's body is its error, never a vocabulary.
        if (!r.ok) throw new Error(await refusalText(r));
        return r.json() as Promise<SparqlVocabulary>;
      })
      .then(
        (v) => !cancelled && (setVocabulary(v), setVocabularyError(null)),
        (e: unknown) =>
          !cancelled && setVocabularyError(e instanceof Error ? e.message : String(e)),
      );
    return () => {
      cancelled = true;
    };
  }, [active, roleHeader]);

  // The viewer's changes to the prefixes, kept like every setting of the tab.
  const [prefixEdits, setPrefixEdits] = useLocalStorage<PrefixEdits>(
    settingKey("sparql", "prefixes"),
    {},
  );
  const prefixes = useMemo(
    () => (vocabulary ? effectivePrefixes(vocabulary, prefixEdits) : {}),
    [vocabulary, prefixEdits],
  );

  /** One query posted to /data/sparql under the viewer's role, its N-Triples read. Throws the
   * refusal's own words. */
  const askTriples = useCallback(
    async (query: string): Promise<Triple[]> => {
      const headers: Record<string, string> = {
        "Content-Type": "application/sparql-query",
        Accept: "application/n-triples",
      };
      if (roleHeader) headers["X-Provisa-Role"] = roleHeader;
      const res = await fetch("/data/sparql", { method: "POST", headers, body: query });
      if (!res.ok) throw new Error(await refusalText(res));
      return parseNTriples(await res.text(), prefixes);
    },
    [roleHeader, prefixes],
  );

  const mergeIntoFrame = useCallback(
    async (id: string, key: string, query: string) => {
      let merged: Triple[];
      try {
        merged = await askTriples(query);
      } catch (err) {
        const error = err instanceof Error ? err.message : String(err);
        changeFrame(id, () => ({ sparqlMerge: { error } }));
        return;
      }
      changeFrame(id, (fr) =>
        merged.length === 0
          ? { sparqlMerge: { nothingMore: key } }
          : {
              sparqlMerge: undefined,
              sparqlExpansions: { ...fr.sparqlExpansions, [key]: merged },
            },
      );
    },
    [askTriples, changeFrame],
  );

  // The resources a DESCRIBE is in flight for, so a node picked again meanwhile asks nothing.
  const describing = useRef(new Set<string>());
  /** The same DESCRIBE an expansion asks, once per resource, kept apart from the drawing's triples
   * as literals (or the error), the frame changed once when all have answered. ``report`` says a
   * refusal on the frame too, where no selected node's panel would. */
  const describeAll = useCallback(
    async (id: string, iris: string[], report: boolean) => {
      const held = framesRef.current.find((fr) => fr.id === id)?.sparqlDescribed ?? {};
      const flight = (iri: string) => `${id}\u0000${iri}`;
      const asked = iris.filter((iri) => !(iri in held) && !describing.current.has(flight(iri)));
      if (asked.length === 0) return;
      asked.forEach((iri) => describing.current.add(flight(iri)));
      const answers: Record<string, SparqlDescribed> = {};
      const waiting = [...asked];
      const reader = async () => {
        for (let iri = waiting.shift(); iri !== undefined; iri = waiting.shift()) {
          answers[iri] = await askTriples(describeQuery(iri)).then(
            (triples): SparqlDescribed => ({
              literals: triples.filter((t) => t.o.rdfTerm === "literal"),
            }),
            (err: unknown): SparqlDescribed => ({
              error: err instanceof Error ? err.message : String(err),
            }),
          );
        }
      };
      await Promise.all(Array.from({ length: DESCRIBE_AT_ONCE }, reader));
      asked.forEach((iri) => describing.current.delete(flight(iri)));
      const refused = Object.values(answers).find((a) => "error" in a);
      changeFrame(id, (fr) => ({
        sparqlDescribed: { ...fr.sparqlDescribed, ...answers },
        ...(report && refused && "error" in refused ? { sparqlMerge: { error: refused.error } } : {}),
      }));
    },
    [framesRef, askTriples, changeFrame],
  );
  /** A selected node's properties, for its Properties panel. */
  const describeResource = useCallback(
    (id: string, iri: string) => void describeAll(id, [iri], false),
    [describeAll],
  );
  /** The nodes a class is captioned or sized by a property of. */
  const describeResources = useCallback(
    (id: string, iris: string[]) => void describeAll(id, iris, true),
    [describeAll],
  );

  /** A node's expansion: a DESCRIBE of its resource merged in, or, with ``toggle``, taken back. */
  const expandResource = useCallback(
    (id: string, iri: string, toggle: boolean) => {
      const held = framesRef.current.find((fr) => fr.id === id)?.sparqlExpansions ?? {};
      if (!(iri in held)) {
        void mergeIntoFrame(id, iri, describeQuery(iri));
      } else if (toggle) {
        const kept = Object.fromEntries(Object.entries(held).filter(([k]) => k !== iri));
        changeFrame(id, () => ({ sparqlExpansions: kept, sparqlMerge: undefined }));
      }
    },
    [framesRef, mergeIntoFrame, changeFrame],
  );

  /** A class pill dropped on a frame: a CONSTRUCT around the class merged in. */
  const dropClass = useCallback(
    (id: string, iri: string) => {
      void mergeIntoFrame(id, iri, classConstruct(iri));
    },
    [mergeIntoFrame],
  );

  return {
    vocabulary,
    vocabularyError,
    prefixes,
    prefixEdits,
    setPrefixEdits,
    expandResource,
    describeResource,
    describeResources,
    dropClass,
  };
}
