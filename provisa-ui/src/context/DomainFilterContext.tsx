// Copyright (c) 2026 Kenneth Stott
// Canary: f3a1b2c4-d5e6-7890-abcd-ef1234567890
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { createContext, useContext, useState, useEffect } from "react";
import { useAuth } from "./AuthContext";

interface DomainFilterContextValue {
  domains: string[];
  setDomains: (d: string[]) => void;
  selectedDomain: string;
  setSelectedDomain: (d: string) => void;
  checkedDomains: Set<string>;
  toggleDomain: (id: string) => void;
}

const DomainFilterContext = createContext<DomainFilterContextValue>({
  domains: [],
  setDomains: () => {},
  selectedDomain: "all",
  setSelectedDomain: () => {},
  checkedDomains: new Set(),
  toggleDomain: () => {},
});

export function DomainFilterProvider({ children }: { children: React.ReactNode }) {
  const { role } = useAuth();
  const [domains, setDomains] = useState<string[]>([]);
  const [selectedDomain, setSelectedDomain] = useState("all");
  const [checkedDomains, setCheckedDomains] = useState<Set<string>>(new Set());

  useEffect(() => {
    if (!role) return;
    if (role.domain_access.includes("*")) {
      fetch("/data/domains", { headers: { "X-Role": role.id } })
        .then((r) => r.json())
        .then((ids: string[]) => {
          if (ids.length > 0) {
            setDomains(ids);
            setCheckedDomains(new Set(ids));
          }
        })
        .catch(() => {});
    } else {
      const ds = role.domain_access.filter((d) => d !== "*");
      if (ds.length > 0) {
        setDomains(ds);
        setCheckedDomains(new Set(ds));
      }
    }
  }, [role?.id]);

  function toggleDomain(id: string) {
    setCheckedDomains((prev) => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });
  }

  return (
    <DomainFilterContext.Provider value={{ domains, setDomains, selectedDomain, setSelectedDomain, checkedDomains, toggleDomain }}>
      {children}
    </DomainFilterContext.Provider>
  );
}

export function useDomainFilter() {
  return useContext(DomainFilterContext);
}
