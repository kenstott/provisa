// Copyright (c) 2026 Kenneth Stott
// Canary: 7d2e9a14-5c3b-4f86-a1e0-b8c4f6d2e971
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-10-28, EVERY LANE NODE INFORMS AND LINKS): what a node of the lane
// diagram tells its reader, in its hover card and the detail panel, each name a link to its own
// page -- a term its definition, status and stewards; a column its type, sensitivity or mask,
// description and table; a table its domain, source, description and quality; a product its
// status, owner, domain and member count; a "+N columns" node the columns it holds.

import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Anchor } from "@mantine/core";
import { Link } from "react-router-dom";
import { entityHref } from "../../lib/entityHref";
import type { GraphData, GraphNode } from "./graphModel";
import { nodeHref } from "./nodeHref";

/** A link to ``to``, named ``name``. */
function Named({ to, name, testId }: { to: string; name: string; testId?: string }) {
  return (
    <Anchor component={Link} to={to} size="xs" data-testid={testId}>
      {name}
    </Anchor>
  );
}

/** Names, each a link, one after another. */
function Names({ items }: { items: { to: string; name: string }[] }) {
  return (
    <span className="g3d-fact-names">
      {items.map((i) => (
        <Named key={i.to} to={i.to} name={i.name} />
      ))}
    </span>
  );
}

/** ``node``'s facts as rows of a list; ``definition`` adds a term's definition (the detail panel
 * shows it above, clamped). */
export function NodeFacts({
  node,
  data,
  definition = false,
  testId,
}: {
  node: GraphNode;
  /** The whole graph read: a column's table and a product's members are found in it. */
  data: GraphData;
  definition?: boolean;
  testId: string;
}) {
  const { t } = useTranslation();
  const info = node.info ?? {};
  const byId = new Map(data.nodes.map((n) => [n.id, n]));
  const rows: [string, ReactNode][] = [];
  const domains = () => {
    const ds = node.domains ?? [];
    if (ds.length > 0)
      rows.push([
        t("graph3d.factDomain", { count: ds.length }),
        <Names items={ds.map((d) => ({ to: entityHref("domain", d), name: d }))} />,
      ]);
  };
  const description = () => {
    if (info.description) rows.push([t("graph3d.factDescription"), info.description]);
  };

  if (node.type === "term") {
    if (definition && node.definition) rows.push([t("graph3d.factDefinition"), node.definition]);
    const status = node.status ?? info.status;
    if (status) rows.push([t("graph3d.factStatus"), t(`graph3d.status_${status}`)]);
    if (info.stewards)
      rows.push([
        t("graph3d.factStewards"),
        info.stewards.length === 0 ? t("graph3d.factNone") : info.stewards.join(", "),
      ]);
  } else if (node.type === "column") {
    if (info.dataType) rows.push([t("graph3d.factType"), info.dataType]);
    const sensitive = node.sensitivities ?? info.sensitive;
    if (sensitive && sensitive.length > 0)
      rows.push([t("graph3d.factSensitivity"), sensitive.join(", ")]);
    if (info.masked) rows.push([t("graph3d.factMask"), t("graph3d.factMasked")]);
    description();
    const table = node.parent ? byId.get(node.parent) : undefined;
    if (table)
      rows.push([
        t("graph3d.factTable"),
        <Named to={nodeHref(table)} name={table.label} testId={`${testId}-table`} />,
      ]);
  } else if (node.type === "table") {
    domains();
    if (info.source)
      rows.push([
        t("graph3d.factSource"),
        <Named to={entityHref("source", info.source)} name={info.source} />,
      ]);
    if (info.quality)
      rows.push([
        t("graph3d.factQuality"),
        info.checkedAt
          ? t("graph3d.qualityAt", {
              quality: t(`graph3d.quality_${info.quality}`),
              at: new Date(info.checkedAt).toLocaleString(),
            })
          : t(`graph3d.quality_${info.quality}`),
      ]);
    description();
  } else if (node.type === "product") {
    if (info.status)
      rows.push([t("graph3d.factStatus"), t(`dataProductsTab.status_${info.status}`)]);
    if (info.owner) rows.push([t("graph3d.factOwner"), info.owner]);
    domains();
    const members = data.edges.filter(
      (e) => e.kind === "member" && e.a === node.id && byId.get(e.b)?.type === "table",
    ).length;
    rows.push([t("graph3d.factMembers"), String(members)]);
  }
  if (rows.length === 0) return null;
  return (
    <dl className="g3d-facts" data-testid={testId}>
      {rows.map(([label, value]) => (
        <div key={label} className="g3d-fact">
          <dt>{label}</dt>
          <dd>{value}</dd>
        </div>
      ))}
    </dl>
  );
}

/** A "+N columns" node's facts: the columns it holds, each a link to its page. */
export function MoreFacts({ columns, testId }: { columns: GraphNode[]; testId: string }) {
  const { t } = useTranslation();
  return (
    <dl className="g3d-facts" data-testid={testId}>
      <div className="g3d-fact">
        <dt>{t("graph3d.factColumns")}</dt>
        <dd>
          <Names items={columns.map((c) => ({ to: nodeHref(c), name: c.label }))} />
        </dd>
      </div>
    </dl>
  );
}
