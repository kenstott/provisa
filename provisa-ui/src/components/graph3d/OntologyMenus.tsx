// Copyright (c) 2026 Kenneth Stott
// Canary: f19b6553-d94c-44f9-aed2-abe78301c800
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Ontology View's choices as dropdowns beside its filters. Its terms and their edges are the
// graph itself, always drawn; Layers adds what is drawn with them (Domains, Data, Columns, and
// Tables, which draws the tables of the columns drawn and so waits for Columns). Terms chooses the
// terms drawn by standing, and whether a term with no relationship or grounding shown is drawn
// (Unconnected).

import { useTranslation } from "react-i18next";
import { SummaryMultiSelect } from "./SummaryMultiSelect";
import { TERM_STATUSES, type LayerId, type TermStatus } from "./graphModel";

const UNCONNECTED = "unconnected";

export function LayersMenu({
  offered,
  layers,
  onChange,
  testId,
}: {
  /** The layers a reader can add, in order. */
  offered: LayerId[];
  layers: ReadonlySet<LayerId>;
  onChange: (chosen: LayerId[]) => void;
  testId: string;
}) {
  const { t } = useTranslation();
  const label = (l: LayerId) =>
    l === "columns"
      ? t("graph3d.columnsToggle")
      : l === "ownerTables"
        ? t("graph3d.tablesToggle")
        : t(`graph3d.layer_${l}`);
  return (
    <SummaryMultiSelect
      label={t("graph3d.layersMenu")}
      prefix={t("graph3d.layersMenu")}
      names={2}
      data={offered.map((l) => ({
        value: l,
        label: label(l),
        disabled: l === "ownerTables" && !layers.has("columns"),
      }))}
      value={offered.filter((l) => layers.has(l))}
      onChange={(chosen) => onChange(chosen as LayerId[])}
      allText={t("graph3d.menuAll")}
      someText={(count, total) => t("graph3d.menuSome", { count, total })}
      testId={`${testId}-layers-menu`}
    />
  );
}

export function TermsMenu({
  hidden,
  unconnected,
  onChange,
  testId,
}: {
  hidden: TermStatus[];
  /** Whether unconnected terms are drawn; null where the graph does not offer the choice. */
  unconnected: boolean | null;
  onChange: (hidden: TermStatus[], unconnected: boolean) => void;
  testId: string;
}) {
  const { t } = useTranslation();
  const options = [
    ...TERM_STATUSES.map((st) => ({ value: st as string, label: t(`graph3d.status_${st}`) })),
    ...(unconnected === null
      ? []
      : [
          {
            value: UNCONNECTED,
            label: t("graph3d.unconnected"),
            hint: t("graph3d.unconnectedHelp"),
          },
        ]),
  ];
  return (
    <SummaryMultiSelect
      label={t("graph3d.termsMenu")}
      prefix={t("graph3d.termsMenu")}
      names={2}
      data={options}
      value={[
        ...TERM_STATUSES.filter((st) => !hidden.includes(st)),
        ...(unconnected ? [UNCONNECTED] : []),
      ]}
      onChange={(chosen) =>
        onChange(
          TERM_STATUSES.filter((st) => !chosen.includes(st)),
          chosen.includes(UNCONNECTED),
        )
      }
      allText={t("graph3d.menuAll")}
      someText={(count, total) => t("graph3d.menuSome", { count, total })}
      testId={`${testId}-terms-menu`}
    />
  );
}
