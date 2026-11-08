import "@mantine/core/styles.css";
import "./src/App.css";
import { useState } from "react";
import { createRoot } from "react-dom/client";
import { MantineProvider } from "@mantine/core";
import { SchemaBrowser } from "./src/pages/sql/SchemaBrowser";
const mk = (i: number) => ({ id: i, sourceId: "s", domainId: "sales", schemaName: "public", tableName: `orders${i}`, alias: null, description: `Table ${i} description`,
  columns: [1, 2, 3].map((c) => ({ id: c, columnName: `c${c}`, computedSqlAlias: `c${c}`, alias: null, description: `Column ${c}`, dataType: "int", visibleTo: [] })) });
const tables = [1, 2, 3, 4].map(mk) as never[];
function App() {
  const [open, setOpen] = useState(true);
  const [d, setD] = useState(new Set(["sales"]));
  const [t, setT] = useState(new Set(["orders1", "orders2"]));
  const [pages, setPages] = useState<Record<string, number>>({});
  const [, tick] = useState(0);
  (window as never as { tick: () => void }).tick = () => tick((n) => n + 1);
  return (<MantineProvider><div style={{ display: "flex", height: 600 }}>
    <SchemaBrowser sidebarOpen={open} setSidebarOpen={setOpen} domainGroups={{ sales: tables }} domainMap={{ sales: { id: "sales", description: "Sales data" } as never }}
      expandedDomains={d} expandedTables={t} domainPages={pages} topTab="sql" insertAtCursor={() => tick((n) => n + 1)}
      toggleDomain={(x) => setD((s) => { const n = new Set(s); if (n.has(x)) n.delete(x); else n.add(x); return n; })}
      toggleTable={(x) => setT((s) => { const n = new Set(s); if (n.has(x)) n.delete(x); else n.add(x); return n; })}
      setDomainPages={setPages} metrics={[{ name: "rev", description: "Revenue" } as never]} tables={tables} />
    <div style={{ flex: 1 }} /></div></MantineProvider>);
}
createRoot(document.getElementById("root")!).render(<App />);
