// Copyright (c) 2026 Kenneth Stott
// Canary: 9c2e4a71-0d3b-4f6e-a8c5-1b7d2e9f3a64
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2180 OFFERS: an offer's terms in words (its generated summary), and a contract's columns as
// the tables page's columns table reads them (REQ-2239, PRODUCT PAGES REUSE TABLE COMPONENTS).

import type { TFunction } from "i18next";
import type {
  ConsumerKind,
  OfferInput,
  OfferTerms,
  TermOfUse,
} from "../../../api/dataProductOffers";
import type { CoreTable } from "../productCore";
import type { MaskKind } from "../../../lib/masks";
import type { ColumnForm } from "../../tables/types";

/** An offer's terms of use, in the order its card, its contracts and a bid show them. */
export const TERMS_OF_USE: readonly TermOfUse[] = ["purpose", "usage", "limitations"];

/** The kinds of consumer a request is for, in the order the request form offers them. */
export const CONSUMER_KINDS: readonly ConsumerKind[] = ["person", "team", "data_product", "agent"];

/** The offer as one sentence: who gets what level of data, with which evidence, for how long. */
export function offerSummary(offer: OfferInput, datasets: number, t: TFunction): string {
  const level = t(`dataProductOffers.levelSummary_${offer.data_level}`, {
    count: offer.sample_views.length,
  });
  const evidence = [
    offer.dq_results ? t("dataProductOffers.withQuality") : t("dataProductOffers.withoutQuality"),
    offer.profiles ? t("dataProductOffers.withProfiles") : t("dataProductOffers.withoutProfiles"),
  ].join(", ");
  const lasts =
    offer.duration_days == null
      ? t("dataProductOffers.noEnd")
      : t("dataProductOffers.forDays", { count: offer.duration_days });
  const how = offer.open ? t("dataProductOffers.summaryOpen") : t("dataProductOffers.summaryBid");
  return t("dataProductOffers.summary", {
    role: offer.role_id || t("dataProductOffers.noRole"),
    level,
    datasets: t("dataProductOffers.datasets", { count: datasets }),
    evidence,
    lasts,
    how,
  });
}

/** A contract's columns as the columns table reads them: each as the offer's role sees it, a
 *  masked one carrying its mask's kind. */
export function contractColumns(terms: OfferTerms, role: string): ColumnForm[] {
  return terms.fields.map((f) => ({
    name: f.name,
    visibleTo: [role],
    masks: f.masked ? [{ to: [role], mask: { type: f.masked as MaskKind } }] : [],
    alias: "",
    description: f.description ?? "",
    selected: true,
    nativeFilterType: null,
    dataType: f.type,
    isPrimaryKey: false,
    scope: "",
  }));
}

/** A dataset's address, as the contracts endpoint names it: source.schema.table. */
export const memberOf = (port: CoreTable) =>
  `${port.sourceId}.${port.schemaName}.${port.tableName}`;
