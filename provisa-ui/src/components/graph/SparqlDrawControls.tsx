// Copyright (c) 2026 Kenneth Stott
// Canary: 4c8e1f7a-92d3-4b60-a5e8-6f1d0b3c7e59
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135: how a SPARQL answer is drawn as asked -- a CONSTRUCT's literals as properties or as
// nodes, a SELECT whose variables pair into subject and object as a graph.

import { Button, Switch } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { SparqlDraw } from "./useSparqlDraw";

export function SparqlDrawControls({ draw, onDrawn }: { draw: SparqlDraw; onDrawn: () => void }) {
  const { t } = useTranslation();
  const { frame } = draw;
  if (frame.status !== "done") return null;
  return (
    <>
      {frame.sparqlTriples && (
        <Switch
          size="xs"
          className="gf-sparql-toggle"
          checked={draw.literalNodes}
          onChange={(e) => draw.setLiteralNodes(e.currentTarget.checked)}
          label={t("graphFrame.sparqlLiteralsAsNodes")}
          data-testid="graph-frame-literals-as-nodes"
        />
      )}
      {frame.sparqlPairing && !draw.drawSelect && (
        <Button
          size="xs"
          variant="default"
          className="gf-sparql-toggle"
          onClick={() => {
            draw.setDrawSelect(true);
            onDrawn();
          }}
          data-testid="graph-frame-draw-as-graph"
        >
          {t("graphFrame.sparqlDrawAsGraph")}
        </Button>
      )}
    </>
  );
}
