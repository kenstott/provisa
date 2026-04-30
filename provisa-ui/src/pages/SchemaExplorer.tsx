// Copyright (c) 2026 Kenneth Stott
// Canary: 153bb4e6-5c29-4534-8de3-672359888375
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect } from "react";
import { useAuth } from "../context/AuthContext";
import { useDomainFilter } from "../context/DomainFilterContext";

/**
 * Schema Explorer — renders GraphQL Voyager in an iframe.
 * Pre-fetches introspection from the parent window, then passes it
 * as static data to Voyager inside the iframe, avoiding CDN/CORS issues.
 * When a domain is selected, filters to that domain + relationship-reachable tables.
 */
export function SchemaExplorer() {
  const { role } = useAuth();
  const { domains, checkedDomains } = useDomainFilter();
  const [srcDoc, setSrcDoc] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const domainParam = checkedDomains.size > 0 && checkedDomains.size < domains.length
    ? `?domain=${encodeURIComponent([...checkedDomains].sort().join(","))}`
    : "";

  useEffect(() => {
    if (!role) return;
    setError(null);
    setSrcDoc(null);
    setLoading(true);

    fetch(`/data/introspection${domainParam}`, {
      headers: { "X-Provisa-Role": role.id },
    })
      .then((r) => r.json())
      .then((json) => {
        if (!json.data) throw new Error("No data in introspection response");
        const escaped = JSON.stringify(json.data);

        setSrcDoc(`<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<style>body{margin:0;overflow:hidden}#voyager{height:100vh;width:100vw}</style>
<link rel="stylesheet" href="/voyager/voyager.css">
<script src="/voyager/react.production.min.js"><\/script>
<script src="/voyager/react-dom.production.min.js"><\/script>
<script src="/voyager/voyager.standalone.js"><\/script>
</head><body>
<div id="voyager"></div>
<script>
GraphQLVoyager.renderVoyager(document.getElementById('voyager'), {
  introspection: { data: ${escaped} },
  displayOptions: { skipRelay: false, sortByAlphabet: true }
});
setTimeout(function() {
  var link = document.querySelector('a[href*="graphql-voyager"]');
  if (link) link.parentElement.style.display = 'none';
}, 500);
<\/script></body></html>`);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [role?.id, domainParam]);

  if (!role) return <div className="page">Select a role to view schema.</div>;
  if (error) return <div className="page error">Failed to load schema: {error}</div>;
  if (loading || !srcDoc) return <div className="page">Loading schema...</div>;

  return (
    <div className="schema-explorer-page">
      <iframe
        title="GraphQL Voyager"
        style={{ width: "100%", height: "100%", border: "none" }}
        srcDoc={srcDoc}
      />
    </div>
  );
}
