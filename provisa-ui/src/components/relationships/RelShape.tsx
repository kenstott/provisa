// Copyright (c) 2026 Kenneth Stott
// Canary: 6b3f1d82-4e7a-4c95-a2d0-8f5c1e9b7a34
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1586 (amended 2026-11-22): a relationship's model type, shown wherever it is listed or
// detailed -- fk (a direct foreign key), jct (through a junction table, an edge whose other columns
// are its properties) or dim (through a shared dimension, a node of its own both sides join) --
// and a "prop" badge only where it has properties, its tip naming them.

import { useTranslation } from "react-i18next";
import { Badge } from "@mantine/core";
import type { RelationshipShape } from "../../types/admin";
import { Tip } from "../Tip";
import { SHAPE_CODE } from "./relationship-types";

const SHAPE_COLOR: Record<RelationshipShape, string> = {
  direct: "gray",
  junction: "grape",
  shared_dimension: "teal",
};

/** ``table``: the junction or dimension it goes through; null for a direct relationship, or a
 * dimension not yet named (a pair whose sides join two different dimensions). */
export function RelShapeBadge({
  shape,
  table,
}: {
  shape: RelationshipShape;
  table: string | null;
}) {
  const { t } = useTranslation();
  const tip =
    shape === "direct"
      ? t("relShape.directTip")
      : shape === "junction"
        ? t("relShape.junctionTip", { table })
        : table === null
          ? t("relShape.dimensionTipUnnamed")
          : t("relShape.dimensionTip", { table });
  return (
    <Tip text={tip}>
      <Badge size="xs" color={SHAPE_COLOR[shape]} variant="light" style={{ flexShrink: 0 }}>
        {SHAPE_CODE[shape]}
      </Badge>
    </Tip>
  );
}

/** Drawn only where the relationship has properties (its real property list from the server). */
export function PropBadge({ properties }: { properties: string[] }) {
  const { t } = useTranslation();
  if (properties.length === 0) return null;
  return (
    <Tip text={t("relShape.propTip", { properties: properties.join(", ") })}>
      <Badge size="xs" color="grape" variant="filled" style={{ flexShrink: 0 }}>
        {t("relShape.prop")}
      </Badge>
    </Tip>
  );
}
