// Copyright (c) 2026 Kenneth Stott
// Canary: d5606279-1a0c-46a0-a9d8-7a12037be320
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277 rule 9: why a list leaves out an item the page holds. A page names each narrowing it
// applies (the domains in view, the search, each filter) once; `leftOutOf` reads them for an item
// the address names, giving `DetailList.leftOut`. The action brings the item into view by undoing
// every narrowing that hides it.

import type { TFunction } from "i18next";
import { matchesQuery } from "../../lib/searchItems";
import type { DetailList, LeftOut } from "./useDetailParam";

/** One narrowing a list applies. */
export interface Narrowing<T> {
  /** Whether it leaves `item` out. */
  hides: (item: T) => boolean;
  /** Why, as a phrase. */
  reason: (item: T) => string;
  /** The action undoing it, as a button's label. */
  action: (item: T) => string;
  /** Undoes it, so far as `item` is concerned. */
  bringIn: (item: T) => void;
}

/**
 * `DetailList.leftOut` over `items`, the items the page holds (null while they load): the item
 * keyed `key` with every narrowing that hides it, or null where the page holds no such item or no
 * narrowing hides it.
 */
export function leftOutOf<T>(
  t: TFunction,
  items: readonly T[] | null,
  keyOf: (item: T) => string,
  nameOf: (item: T) => string,
  narrowings: readonly (Narrowing<T> | null)[],
): (key: string) => LeftOut | null {
  return (key) => {
    const item = items?.find((i) => keyOf(i) === key);
    if (item === undefined) return null;
    const hiding = narrowings.filter((n): n is Narrowing<T> => n !== null && n.hides(item));
    if (hiding.length === 0) return null;
    return {
      name: nameOf(item),
      reasons: hiding.map((n) => n.reason(item)),
      action: hiding.length === 1 ? hiding[0].action(item) : t("detailInList.showAll"),
      bringIn: () => {
        for (const n of hiding) n.bringIn(item);
      },
    };
  };
}

/** The items no narrowing leaves out: the rows the list shows. */
export function shownBy<T>(items: readonly T[], narrowings: readonly (Narrowing<T> | null)[]): T[];
export function shownBy<T>(
  items: readonly T[] | null,
  narrowings: readonly (Narrowing<T> | null)[],
): T[] | null;
export function shownBy<T>(
  items: readonly T[] | null,
  narrowings: readonly (Narrowing<T> | null)[],
): T[] | null {
  if (items === null) return null;
  return items.filter((item) => narrowings.every((n) => n === null || !n.hides(item)));
}

/**
 * The `DetailList` of a list showing `items` (null while they load) narrowed by `narrowings`: the
 * rows it shows, and why it leaves out an item it holds.
 */
export function detailListOf<T>(
  t: TFunction,
  items: readonly T[] | null,
  keyOf: (item: T) => string,
  nameOf: (item: T) => string,
  narrowings: readonly (Narrowing<T> | null)[],
): DetailList {
  const shown = shownBy(items, narrowings);
  return {
    shown: shown === null ? null : shown.map(keyOf),
    leftOut: leftOutOf(t, items, keyOf, nameOf, narrowings),
  };
}

/** The page's search over each item's `texts` (`matchesQuery`); null while there is none. */
export function searchNarrowing<T>(
  t: TFunction,
  search: string,
  clear: () => void,
  texts: (item: T) => (string | null | undefined)[],
): Narrowing<T> | null {
  return searchMatchNarrowing<T>(t, search, clear, (item) => matchesQuery(texts(item), search));
}

/**
 * The page's search where whether an item holds it is the page's own reading (`holds`), as for an
 * item drawn as several rows; null while there is none.
 */
export function searchMatchNarrowing<T>(
  t: TFunction,
  search: string,
  clear: () => void,
  holds: (item: T) => boolean,
): Narrowing<T> | null {
  if (search.trim() === "") return null;
  return {
    hides: (item) => !holds(item),
    reason: () => t("detailInList.bySearch", { search }),
    action: () => t("detailInList.clearSearch"),
    bringIn: clear,
  };
}

/** A filter the list applies, named as its control is labelled; null while it is off. */
export function filterNarrowing<T>(
  t: TFunction,
  on: boolean,
  filter: string,
  keeps: (item: T) => boolean,
  clear: () => void,
): Narrowing<T> | null {
  if (!on) return null;
  return {
    hides: (item) => !keeps(item),
    reason: () => t("detailInList.byFilter", { filter }),
    action: () => t("detailInList.clearFilter", { filter }),
    bringIn: clear,
  };
}

/**
 * The domains in view (`viewDomains`, null for no narrowing) over each item's domains: an item in
 * none of them is left out, and brought in by adding its domains to the view.
 */
export function domainNarrowing<T>(
  t: TFunction,
  viewDomains: ReadonlySet<string> | null,
  addDomain: (domain: string) => void,
  domainsOf: (item: T) => readonly string[],
): Narrowing<T> | null {
  if (viewDomains === null) return null;
  return {
    hides: (item) => {
      const own = domainsOf(item);
      return own.length > 0 && !own.some((d) => viewDomains.has(d));
    },
    reason: (item) => t("detailInList.byDomain", { domains: domainsOf(item).join(", ") }),
    action: (item) => t("detailInList.addDomain", { domains: domainsOf(item).join(", ") }),
    bringIn: (item) => {
      for (const d of domainsOf(item)) addDomain(d);
    },
  };
}
