// Copyright (c) 2026 Kenneth Stott
// Canary: 207cfbc3-612b-487c-9276-59557514b55f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

/* eslint-disable react-refresh/only-export-components -- context Provider + hook colocated by design */
import { createContext, useContext, useState, useEffect } from "react";
import { useAuth } from "./AuthContext";
import { fetchSettings } from "../api/admin";
import { CHECKED_DOMAINS_KEY, KNOWN_DOMAINS_KEY } from "../lib/domainFilterKeys";

interface DomainFilterContextValue {
  domains: string[];
  setDomains: (d: string[]) => void;
  selectedDomain: string;
  setSelectedDomain: (d: string) => void;
  checkedDomains: Set<string>;
  toggleDomain: (id: string) => void;
  ensureDomainChecked: (id: string) => void;
  domainsEnabled: boolean;
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

const DomainFilterContext = createContext<DomainFilterContextValue>({
  domains: [],
  setDomains: () => {},
  selectedDomain: "all",
  setSelectedDomain: () => {},
  checkedDomains: new Set(),
  toggleDomain: () => {},
  ensureDomainChecked: () => {},
  domainsEnabled: true,
});

export function DomainFilterProvider({ children }: { children: React.ReactNode }) {
  const { selectedRoles } = useAuth();
  const [domains, setDomains] = useState<string[]>([]);
  const [selectedDomain, setSelectedDomain] = useState("all");
  const [checkedDomains, setCheckedDomains] = useState<Set<string>>(new Set());
  const [domainsEnabled, setDomainsEnabled] = useState(true);

  useEffect(() => {
    fetchSettings()
      .then((s) => setDomainsEnabled(s.naming.use_domains !== false))
      .catch(() => {});
  }, []);

  // Restore the persisted checked set, but default any domain that has appeared since the last
  // visit (e.g. a just-created view's domain) to CHECKED — otherwise a new domain, absent from the
  // saved whitelist, would stay invisible forever. A domain the user explicitly unchecked is in the
  // known set but not the checked set, so it correctly stays unchecked.
  function restoreChecked(available: string[]): Set<string> {
    try {
      const savedChecked = localStorage.getItem(CHECKED_DOMAINS_KEY);
      const savedKnown = localStorage.getItem(KNOWN_DOMAINS_KEY);
      const merged = mergeCheckedDomains(
        available,
        savedChecked ? JSON.parse(savedChecked) : null,
        savedKnown ? JSON.parse(savedKnown) : null,
      );
      if (merged) {
        localStorage.setItem(KNOWN_DOMAINS_KEY, JSON.stringify(available));
        return merged;
      }
    } catch {
      // ignore
    }
    localStorage.setItem(KNOWN_DOMAINS_KEY, JSON.stringify(available));
    return new Set(available);
  }

  // REQ-1319: the filter spans EVERY active role, not `selectedRoles[0]`. Under "Role: All" the
  // user is acting as the union of their roles, and `capabilities` upstream is already that union —
  // but this effect read one arbitrary role, so a user holding analyst (pet-store, shelter) plus
  // org_admin (*) got analyst's two domains and lost meta and ops entirely. Which role won was an
  // accident of list order, so the same account showed different domains as its role set changed.
  // Any active role with `*` means the whole catalog; otherwise the union of the named domains.
  const roleIds = selectedRoles.map((r) => r.id).join(",");
  const hasWildcard = selectedRoles.some((r) => r.domain_access.includes("*"));
  const namedDomains = [
    ...new Set(selectedRoles.flatMap((r) => r.domain_access).filter((d) => d !== "*")),
  ].join(",");
  useEffect(() => {
    if (selectedRoles.length === 0) return;
    if (hasWildcard) {
      fetch("/data/domains", { headers: { "X-Role": roleIds.split(",")[0] } })
        .then((r) => r.json())
        .then((ids: string[]) => {
          if (ids.length > 0) {
            setDomains(ids);
            setCheckedDomains(restoreChecked(ids));
          }
        })
        .catch(() => {});
    } else {
      const ds = namedDomains ? namedDomains.split(",") : [];
      if (ds.length > 0) {
        /* eslint-disable-next-line react-hooks/set-state-in-effect --
           reset domain filter state in sync with an external input (the active role set) */
        setDomains(ds);
        setCheckedDomains(restoreChecked(ds));
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- keyed by the role SET (the joined ids), not the array identity, so the fetch does not re-run on every render
  }, [roleIds, hasWildcard, namedDomains]);

  function toggleDomain(id: string) {
    setCheckedDomains((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      localStorage.setItem(CHECKED_DOMAINS_KEY, JSON.stringify([...next]));
      return next;
    });
  }

  // Reveal a domain immediately — used when the user creates an asset in a domain (e.g. a new view)
  // so it appears in the filtered lists without a reload. Adds it to the known + available sets too.
  function ensureDomainChecked(id: string) {
    if (!id) return;
    setDomains((prev) => (prev.includes(id) ? prev : [...prev, id]));
    setCheckedDomains((prev) => {
      if (prev.has(id)) return prev;
      const next = new Set(prev).add(id);
      localStorage.setItem(CHECKED_DOMAINS_KEY, JSON.stringify([...next]));
      return next;
    });
    try {
      const known: string[] = JSON.parse(localStorage.getItem(KNOWN_DOMAINS_KEY) || "[]");
      if (!known.includes(id))
        localStorage.setItem(KNOWN_DOMAINS_KEY, JSON.stringify([...known, id]));
    } catch {
      // ignore
    }
  }

  return (
    <DomainFilterContext.Provider
      value={{
        domains,
        setDomains,
        selectedDomain,
        setSelectedDomain,
        checkedDomains,
        toggleDomain,
        ensureDomainChecked,
        domainsEnabled,
      }}
    >
      {children}
    </DomainFilterContext.Provider>
  );
}

export function useDomainFilter() {
  return useContext(DomainFilterContext);
}
