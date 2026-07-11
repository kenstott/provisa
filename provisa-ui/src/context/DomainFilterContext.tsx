// Copyright (c) 2026 Kenneth Stott
// Canary: 207cfbc3-612b-487c-9276-59557514b55f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

/* eslint-disable react-refresh/only-export-components -- context Provider + hook colocated by design */
import { createContext, useContext, useState, useEffect } from "react";
import { useAuth } from "./AuthContext";
import { fetchSettings } from "../api/admin";

interface DomainFilterContextValue {
  domains: string[];
  setDomains: (d: string[]) => void;
  selectedDomain: string;
  setSelectedDomain: (d: string) => void;
  checkedDomains: Set<string>;
  toggleDomain: (id: string) => void;
  domainsEnabled: boolean;
}

const DomainFilterContext = createContext<DomainFilterContextValue>({
  domains: [],
  setDomains: () => {},
  selectedDomain: "all",
  setSelectedDomain: () => {},
  checkedDomains: new Set(),
  toggleDomain: () => {},
  domainsEnabled: true,
});

export function DomainFilterProvider({ children }: { children: React.ReactNode }) {
  const { role } = useAuth();
  const [domains, setDomains] = useState<string[]>([]);
  const [selectedDomain, setSelectedDomain] = useState("all");
  const [checkedDomains, setCheckedDomains] = useState<Set<string>>(new Set());
  const [domainsEnabled, setDomainsEnabled] = useState(true);

  useEffect(() => {
    fetchSettings()
      .then((s) => setDomainsEnabled(s.naming.use_domains !== false))
      .catch(() => {});
  }, []);

  const CHECKED_DOMAINS_KEY = "provisa.checkedDomains";

  function restoreChecked(available: string[]): Set<string> {
    try {
      const saved = localStorage.getItem(CHECKED_DOMAINS_KEY);
      if (saved) {
        const parsed: string[] = JSON.parse(saved);
        const restored = parsed.filter((d) => available.includes(d));
        if (restored.length > 0) return new Set(restored);
      }
    } catch {
      // ignore
    }
    return new Set(available);
  }

  useEffect(() => {
    if (!role) return;
    if (role.domain_access.includes("*")) {
      fetch("/data/domains", { headers: { "X-Role": role.id } })
        .then((r) => r.json())
        .then((ids: string[]) => {
          if (ids.length > 0) {
            setDomains(ids);
            setCheckedDomains(restoreChecked(ids));
          }
        })
        .catch(() => {});
    } else {
      const ds = role.domain_access.filter((d) => d !== "*");
      if (ds.length > 0) {
        /* eslint-disable-next-line react-hooks/set-state-in-effect --
           reset domain filter state in sync with an external input (the active role) */
        setDomains(ds);
        setCheckedDomains(restoreChecked(ds));
      }
    }
  }, [role]);

  function toggleDomain(id: string) {
    setCheckedDomains((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      localStorage.setItem(CHECKED_DOMAINS_KEY, JSON.stringify([...next]));
      return next;
    });
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
