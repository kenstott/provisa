// Copyright (c) 2026 Kenneth Stott
// Canary: a0150b56-27c9-455b-905e-55c809a30cf6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { Plugin } from "vite";
import { parse } from "graphql";
import type { OperationDefinitionNode, FragmentDefinitionNode } from "graphql";
import path from "path";
import fs from "fs";

// Turns each named operation/fragment in a *.graphql / *.gql file into a named export whose
// DocumentNode carries only that definition plus its transitively-referenced fragments. Shared by
// the app build (vite.config.ts) and the test runner (vitest.config.ts) so component tests that
// transitively import a *.graphql module resolve it identically to production.
export const graphqlLoader = (): Plugin => {
  return {
    name: "graphql-module-loader",
    enforce: "pre",

    resolveId(id: string, importer: string | undefined) {
      if (id.endsWith(".graphql") || id.endsWith(".gql")) {
        const resolved = importer
          ? path.resolve(path.dirname(importer), id)
          : path.resolve(process.cwd(), id);
        return resolved;
      }
      return undefined;
    },

    load(id: string) {
      if (id.endsWith(".graphql") || id.endsWith(".gql")) {
        const content = fs.readFileSync(id, "utf-8");
        const ast = parse(content);

        // Direct fragment spreads referenced anywhere under a node's selections.
        const directSpreads = (node: unknown): string[] => {
          const found: string[] = [];
          const visit = (n: { kind?: string; name?: { value: string }; selectionSet?: { selections: unknown[] } }) => {
            if (!n || typeof n !== "object") return;
            if (n.kind === "FragmentSpread" && n.name) found.push(n.name.value);
            if (n.selectionSet) for (const s of n.selectionSet.selections) visit(s as typeof n);
          };
          visit(node as { selectionSet?: { selections: unknown[] } });
          return found;
        };

        const spreadsByFragment = new Map<string, string[]>();
        for (const def of ast.definitions) {
          const named = def as OperationDefinitionNode | FragmentDefinitionNode;
          if (named.kind === "FragmentDefinition" && named.name?.value) {
            spreadsByFragment.set(named.name.value, directSpreads(named));
          }
        }

        // Transitive closure of fragment names a definition depends on.
        const usedFragments = (def: OperationDefinitionNode | FragmentDefinitionNode): string[] => {
          const seen = new Set<string>();
          const stack = [...directSpreads(def)];
          while (stack.length) {
            const fragName = stack.pop()!;
            if (seen.has(fragName)) continue;
            seen.add(fragName);
            for (const dep of spreadsByFragment.get(fragName) ?? []) stack.push(dep);
          }
          return [...seen];
        };

        const lines: string[] = [
          `import { parse } from 'graphql';`,
          `const _source = \`${content.replace(/`/g, "\\`").replace(/\$/g, "\\$")}\`;`,
          `const _doc = parse(_source);`,
        ];

        for (const def of ast.definitions) {
          const named = def as OperationDefinitionNode | FragmentDefinitionNode;
          if (named.name?.value) {
            const name = named.name.value;
            const keep = new Set([name, ...usedFragments(named)]);
            const keepJson = JSON.stringify([...keep]);
            lines.push(
              `export const ${name} = {`,
              `  ...(_doc),`,
              `  definitions: _doc.definitions.filter(`,
              `    d => ${keepJson}.includes(d.name?.value)`,
              `  )`,
              `};`,
            );
          }
        }

        lines.push(`export default _doc;`);
        return lines.join("\n");
      }
      return undefined;
    },
  };
};
