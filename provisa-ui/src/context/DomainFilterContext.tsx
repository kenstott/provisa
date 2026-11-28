// Copyright (c) 2026 Kenneth Stott
// Canary: 207cfbc3-612b-487c-9276-59557514b55f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

/* eslint-disable react-refresh/only-export-components -- context Provider + hook colocated by design */
import { createContext, useCallback, useContext, useState, useEffect } from "react";
import { useAuth } from "./AuthContext";
import { fetchSettings } from "../api/admin";
import {
  PRODUCT_DOMAIN_KEYS,
  ROLE_DOMAIN_KEYS,
  type DomainSelectionKeys,
} from "../lib/domainFilterKeys";
import { getStoredJson, setStored } from "../lib/browserStorage";

interface DomainFilterContextValue {
  domains: string[];
  setDomains: (d: string[]) => void;
  selectedDomain: string;
  setSelectedDomain: (d: string) => void;
  checkedDomains: Set<string>;
  toggleDomain: (id: string) => void;
  ensureDomainChecked: (id: string) => void;
  domainsEnabled: boolean;
  /**
   * True while the domains of the active roles are being read: the checked domains are not yet the
   * reader's, so a list narrowed by them is not yet the one the page shows (REQ-2277 rule 9).
   */
  domainsPending: boolean;
  /**
   * REQ-1659 (amended 2026-11-28): the Data Products page's domains -- those of the products it
   * lists, any domain the role does not reach included -- with their own checked set, kept apart
   * from the role's (PRODUCT_DOMAIN_KEYS says why).
   */
  productDomains: string[];
  setProductDomains: (d: string[]) => void;
  checkedProductDomains: Set<string>;
  toggleProductDomain: (id: string) => void;
  ensureProductDomainChecked: (id: string) => void;
}

// Pure merge of the persisted domain-filter state against the currently-available domains. A domain
// that has appeared since the last visit (in `available`, absent from `known`) defaults to CHECKED —
// so a just-created view's domain is visible without a reload. A domain the user explicitly unchecked
// is in `known` but not `checked`, so it stays unchecked. Returns null when there is no persisted
// state to merge (caller then checks everything).
export function mergeCheckedDomains(
  available: string[],
  savedChecked: string[] | null,
  savedKnown: string[] | null,
): Set<string> | null {
  if (!savedChecked || !savedKnown) return null;
  const restored = new Set(savedChecked.filter((d) => available.includes(d)));
  for (const d of available) if (!savedKnown.includes(d)) restored.add(d);
  return restored;
}

// Restore the persisted checked set, but default any domain that has appeared since the last visit
// (e.g. a just-created view's domain, or the domain of a demo started since) to CHECKED -- otherwise
// a new domain, absent from the saved whitelist, would stay invisible forever. A domain the user
// explicitly unchecked is in the known set but not the checked set, so it stays unchecked. Both sets
// are saved as restored: saving only the known set made a new domain "known but not checked" on the
// next load, so it read as unchecked by the user and its tables vanished from every list.
export function restoreCheckedDomains(
  available: string[],
  keys: DomainSelectionKeys,
): Set<string> {
  try {
    const merged = mergeCheckedDomains(
      available,
      getStoredJson(keys.checked),
      getStoredJson(keys.known),
    );
    if (merged) {
      setStored(keys.known, JSON.stringify(available));
      setStored(keys.checked, JSON.stringify([...merged]));
      return merged;
    }
  } catch {
    // ignore
  }
  setStored(keys.known, JSON.stringify(available));
  return new Set(available);
}

const DomainFilterContext = createContext<DomainFilterContextValue>({
  domains: [],
  setDomains: () => {},
  selectedDomain: "all",
  setSelectedDomain: () => {},
  checkedDomains: new Set(),
  toggleDomain: () => {},
  ensureDomainChecked: () => {},
  domainsEnabled: true,
  domainsPending: false,
  productDomains: [],
  setProductDomains: () => {},
  checkedProductDomains: new Set(),
  toggleProductDomain: () => {},
  ensureProductDomainChecked: () => {},
});

/** One persisted selection: a toggle and a reveal over ``setChecked``, saved under ``keys``. */
function selectionActions(
  keys: DomainSelectionKeys,
  setDomains: (f: (prev: string[]) => string[]) => void,
  setChecked: (f: (prev: Set<string>) => Set<string>) => void,
) {
  function toggle(id: string) {
    setChecked((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      setStored(keys.checked, JSON.stringify([...next]));
      return next;
    });
  }
  // Reveal a domain immediately -- used when the user creates an asset in a domain (e.g. a new
  // view) so it appears in the filtered lists without a reload. Adds it to the known + available
  // sets too.
  function ensure(id: string) {
    if (!id) return;
    setDomains((prev) => (prev.includes(id) ? prev : [...prev, id]));
    setChecked((prev) => {
      if (prev.has(id)) return prev;
      const next = new Set(prev).add(id);
      setStored(keys.checked, JSON.stringify([...next]));
      return next;
    });
    const known = getStoredJson<string[]>(keys.known) ?? [];
    if (!known.includes(id)) setStored(keys.known, JSON.stringify([...known, id]));
  }
  return { toggle, ensure };
}

export function DomainFilterProvider({ children }: { children: React.ReactNode }) {
  const { selectedRoles } = useAuth();
  const [domains, setDomains] = useState<string[]>([]);
  const [selectedDomain, setSelectedDomain] = useState("all");
  const [checkedDomains, setCheckedDomains] = useState<Set<string>>(new Set());
  const [domainsEnabled, setDomainsEnabled] = useState(true);
  const [productDomains, setProductDomainList] = useState<string[]>([]);
  const [checkedProductDomains, setCheckedProductDomains] = useState<Set<string>>(new Set());
  // The role set whose domains have been answered (or refused): until it is the current one, the
  // checked domains are still being restored.
  const [answeredFor, setAnsweredFor] = useState<string | null>(null);

  useEffect(() => {
    fetchSettings()
      .then((s) => setDomainsEnabled(s.naming.use_domains !== false))
      .catch(() => {});
  }, []);

  // REQ-1319: the filter spans EVERY active role, not `selectedRoles[0]`. Under "Role: All" the
  // user is acting as the union of their roles, and `capabilities` upstream is already that union —
  // but this effect read one arbitrary role, so a user holding analyst (pet-store, shelter) plus
  // org_admin (*) got analyst's two domains and lost meta and ops entirely. Which role won was an
  // accident of list order, so the same account showed different domains as its role set changed.
  // The union of what each active role reaches, which the server answers from its flattened
  // domain_access: its data domains and the system domains its capabilities hold (REQ-1132). Every
  // role is asked, a wildcard one too: `*` is every data domain, so it answers no system domain
  // another role in the set may hold.
  const roleIds = selectedRoles.map((r) => r.id).join(",");
  const namedDomains = [
    ...new Set(selectedRoles.flatMap((r) => r.domain_access).filter((d) => d !== "*")),
  ].join(",");
  const roleSetKey = `${roleIds}|${namedDomains}`;
  const domainsPending = selectedRoles.length > 0 && answeredFor !== roleSetKey;
  useEffect(() => {
    if (selectedRoles.length === 0) return;
    let stale = false;
    Promise.all(
      selectedRoles.map(({ id }) =>
        fetch("/data/domains", { headers: { "X-Provisa-Role": id } }).then(
          (r) => r.json() as Promise<string[]>,
        ),
      ),
    )
      .then((answers) => {
        const ids = [...new Set(answers.flat())];
        if (!stale && ids.length > 0) {
          setDomains(ids);
          setCheckedDomains(restoreCheckedDomains(ids, ROLE_DOMAIN_KEYS));
        }
      })
      .catch(() => {})
      .finally(() => {
        if (!stale) setAnsweredFor(roleSetKey);
      });
    return () => {
      stale = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- keyed by the role SET (the joined ids), not the array identity, so the fetch does not re-run on every render
  }, [roleIds, namedDomains]);

  const roleSelection = selectionActions(ROLE_DOMAIN_KEYS, setDomains, setCheckedDomains);
  const productSelection = selectionActions(
    PRODUCT_DOMAIN_KEYS,
    setProductDomainList,
    setCheckedProductDomains,
  );

  // The page hands over the domains of the products it lists each time they change; the saved
  // product selection is restored against them as the role's is against the role's domains.
  const setProductDomains = useCallback((ids: string[]) => {
    setProductDomainList(ids);
    setCheckedProductDomains(restoreCheckedDomains(ids, PRODUCT_DOMAIN_KEYS));
  }, []);

  return (
    <DomainFilterContext.Provider
      value={{
        domains,
        setDomains,
        selectedDomain,
        setSelectedDomain,
        checkedDomains,
        toggleDomain: roleSelection.toggle,
        ensureDomainChecked: roleSelection.ensure,
        domainsEnabled,
        domainsPending,
        productDomains,
        setProductDomains,
        checkedProductDomains,
        toggleProductDomain: productSelection.toggle,
        ensureProductDomainChecked: productSelection.ensure,
      }}
    >
      {children}
    </DomainFilterContext.Provider>
  );
}

export function useDomainFilter() {
  return useContext(DomainFilterContext);
}
