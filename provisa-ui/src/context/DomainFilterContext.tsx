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
}

const DomainFilterContext = createContext<DomainFilterContextValue>({
  domains: [],
  setDomains: () => {},
  selectedDomain: "all",
  setSelectedDomain: () => {},
});

export function DomainFilterProvider({ children }: { children: React.ReactNode }) {
  const { role } = useAuth();
  const [domains, setDomains] = useState<string[]>([]);
  const [selectedDomain, setSelectedDomain] = useState("all");

  useEffect(() => {
    if (!role) return;
    if (role.domain_access.includes("*")) {
      fetch("/data/domains", { headers: { "X-Role": role.id } })
        .then((r) => r.json())
        .then((ids: string[]) => { if (ids.length > 0) setDomains(ids); })
        .catch(() => {});
    } else {
      const ds = role.domain_access.filter((d) => d !== "*");
      if (ds.length > 0) setDomains(ds);
    }
  }, [role?.id]);

  return (
    <DomainFilterContext.Provider value={{ domains, setDomains, selectedDomain, setSelectedDomain }}>
      {children}
    </DomainFilterContext.Provider>
  );
}

export function useDomainFilter() {
  return useContext(DomainFilterContext);
}
