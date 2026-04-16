// Shim for @neo4j-cypher/codemirror — package.json "exports" map does not
// expose a resolvable types entry under moduleResolution "bundler".
declare module "@neo4j-cypher/codemirror" {
  import type { Extension } from "@codemirror/state";
  export function cypherLanguage(): Extension;
}
