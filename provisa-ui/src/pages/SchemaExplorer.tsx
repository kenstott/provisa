// Copyright (c) 2025 Kenneth Stott
// Canary: 153bb4e6-5c29-4534-8de3-672359888375
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect, useRef } from "react";
import { useAuth } from "../context/AuthContext";

/**
 * Schema Explorer — renders GraphQL Voyager in an iframe.
 * Pre-fetches introspection from the parent window, then passes it
 * as static data to Voyager inside the iframe, avoiding CDN/CORS issues.
 */
export function SchemaExplorer() {
  const { role } = useAuth();
  const [srcDoc, setSrcDoc] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!role) return;
    setError(null);
    setSrcDoc(null);
    setLoading(true);

    // Fetch introspection from the parent window (where proxy works)
    fetch("/data/graphql", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Provisa-Role": role.id,
      },
      body: JSON.stringify({
        query: `query IntrospectionQuery { __schema { queryType { name } mutationType { name } subscriptionType { name } types { ...FullType } directives { name description locations args { ...InputValue } } } } fragment FullType on __Type { kind name description fields(includeDeprecated: true) { name description args { ...InputValue } type { ...TypeRef } isDeprecated deprecationReason } inputFields { ...InputValue } interfaces { ...TypeRef } enumValues(includeDeprecated: true) { name description isDeprecated deprecationReason } possibleTypes { ...TypeRef } } fragment InputValue on __InputValue { name description type { ...TypeRef } defaultValue } fragment TypeRef on __Type { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name } } } } } } } }`,
      }),
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
  }, [role?.id]);

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
