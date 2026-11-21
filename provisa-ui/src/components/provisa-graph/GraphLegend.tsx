// Copyright (c) 2026 Kenneth Stott
// Canary: e5b8d2c7-9a31-4f06-b7e4-3c1a8f6d9b20
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: a graph's one legend, bottom-right: each type drawn, as its node shows it -- the band's
// colour (a diamond for the junction), the icon, the type's name.

import { useTranslation } from "react-i18next";
import { NODE_TYPES, typeIcon, typeShape, type NodeType } from "./tokens";

export function GraphLegend({
  types,
  typeName,
}: {
  types: NodeType[];
  typeName: (type: NodeType) => string;
}) {
  const { t } = useTranslation();
  if (types.length === 0) return null;
  return (
    <div className="pg-legend" role="list" aria-label={t("provisaGraph.legend")} data-testid="graph-legend">
      {types.map((type) => (
        <span key={type} className="pg-legend-item" role="listitem">
          <i
            className={`pg-legend-swatch pg-legend-swatch--${typeShape(type)}`}
            style={{ background: NODE_TYPES[type].colour }}
          />
          <img src={typeIcon(type)} width={12} height={12} alt="" />
          {typeName(type)}
        </span>
      ))}
    </div>
  );
}
