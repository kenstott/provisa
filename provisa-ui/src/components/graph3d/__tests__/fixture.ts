// Copyright (c) 2026 Kenneth Stott
// Canary: 2e6b9d47-1c8a-4f3e-b5d2-7a0c4e9f1b68
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2128: a data product's graph, as the meta domain gives it, for the graph's tests.

import type { GraphData } from "../graphModel";

// A product with member tables, a command, a checker, a profile run, a tag, glossary terms bound to
// member columns, a relationship inside and one outside, and upstream lineage.
export const DATA: GraphData = {
  nodes: [
    { id: "p:c360", type: "product", label: "customer_360", domains: ["sales"], tags: ["gold"] },
    { id: "p:billing", type: "product", label: "billing", domains: ["finance"], tags: [] },
    { id: "d:sales", type: "domain", label: "sales" },
    { id: "d:finance", type: "domain", label: "finance" },
    {
      id: "t:customers",
      type: "table",
      label: "sales.customers",
      domains: ["sales"],
      tags: ["gold"],
      sensitivity: "pii",
    },
    {
      id: "t:orders",
      type: "table",
      label: "sales.orders",
      domains: ["sales"],
      tags: [],
      sensitivity: null,
    },
    {
      id: "c:customers.email",
      type: "column",
      label: "email",
      parent: "t:customers",
      domains: ["sales"],
      sensitivity: "pii",
    },
    {
      id: "c:orders.total",
      type: "column",
      label: "total",
      parent: "t:orders",
      domains: ["sales"],
    },
    { id: "f:refresh", type: "command", label: "refresh_c360", domains: ["sales"] },
    { id: "k:orders_dq", type: "checker", label: "orders_dq" },
    { id: "r:orders_profile", type: "profile", label: "orders profile" },
    { id: "g:gold", type: "tag", label: "gold" },
    { id: "term:customer", type: "term", label: "Customer", domains: ["sales"] },
    { id: "term:party", type: "term", label: "Party", domains: [] },
    { id: "t:invoices", type: "relation", label: "finance.invoices", domains: ["finance"] },
    { id: "x:raw_orders", type: "relation", label: "raw.orders" },
  ],
  edges: [
    { id: "1", a: "p:c360", b: "d:sales", kind: "in_domain" },
    { id: "2", a: "p:billing", b: "d:finance", kind: "in_domain" },
    { id: "3", a: "p:c360", b: "p:billing", kind: "product_link", paths: 2 },
    { id: "4", a: "p:c360", b: "t:customers", kind: "member" },
    { id: "5", a: "p:c360", b: "t:orders", kind: "member" },
    { id: "6", a: "t:customers", b: "c:customers.email", kind: "has_column" },
    { id: "7", a: "t:orders", b: "c:orders.total", kind: "has_column" },
    { id: "8", a: "p:c360", b: "f:refresh", kind: "command" },
    { id: "9", a: "k:orders_dq", b: "t:orders", kind: "checks" },
    { id: "10", a: "r:orders_profile", b: "t:orders", kind: "profiled" },
    { id: "11", a: "g:gold", b: "p:c360", kind: "tagged" },
    { id: "12", a: "g:gold", b: "t:customers", kind: "tagged" },
    { id: "13", a: "term:customer", b: "c:customers.email", kind: "term_binding" },
    { id: "14", a: "term:customer", b: "term:party", kind: "term_edge", label: "KIND_OF" },
    { id: "15", a: "t:customers", b: "t:orders", kind: "relationship" },
    { id: "16", a: "t:orders", b: "t:invoices", kind: "relationship_outside" },
    { id: "17", a: "x:raw_orders", b: "t:orders", kind: "lineage" },
  ],
};
