// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1984: what a Node bundle's declarations say, read with the TypeScript compiler API.
//
//   <runtime> reader.mjs <path to typescript.js> <entry declaration file>
//
// Prints one JSON document: every export of the entry module with its JSDoc tags, its JSDoc text
// and, for a function, its call signatures' parameters and return types; and every named type
// those refer to. Types are written as trees ({"kind": ...}); a named object type is written
// once, under "types", and referred to by name. Nothing is decided here: which exports are
// tables, commands or streams, and which types map to what, is Provisa's (provisa/
// codesource/manifest.py). The bundle's JavaScript is never read or run. Node, Bun and Deno run it alike.

import { createRequire } from "node:module";
import process from "node:process";

const [typescriptPath, entry] = process.argv.slice(2);
const require = createRequire(import.meta.url);
const ts = require(typescriptPath);

// REQ-2100: "@provisa/services" is Provisa's own declarations (services.d.ts beside this
// reader), so a ReadContext parameter is read as the one Provisa gives.
const SERVICES_TYPES = new URL("./services.d.ts", import.meta.url).pathname;
const program = ts.createProgram([entry], {
  target: ts.ScriptTarget.ESNext,
  lib: ["lib.esnext.d.ts"],
  noEmit: true,
  skipLibCheck: true,
  strict: true,
  paths: { "@provisa/services": [SERVICES_TYPES] },
});
const checker = program.getTypeChecker();
const source = program.getSourceFile(entry);
const diagnostics = ts
  .getPreEmitDiagnostics(program, source)
  .filter((d) => d.category === ts.DiagnosticCategory.Error)
  .map((d) => ts.flattenDiagnosticMessageText(d.messageText, "\n"));

const types = {};
const inProgress = new Set();

const ITERABLES = {
  Iterable: [false, false],
  IterableIterator: [false, false],
  Generator: [false, true],
  AsyncIterable: [true, false],
  AsyncIterableIterator: [true, false],
  AsyncGenerator: [true, true],
};

function fromServices(type) {
  const symbol = type.aliasSymbol ?? type.getSymbol();
  return (symbol?.declarations ?? []).some((d) => d.getSourceFile().fileName === SERVICES_TYPES);
}

function symbolName(type) {
  const symbol = type.aliasSymbol ?? type.getSymbol();
  return symbol ? symbol.getName() : null;
}

function fromLib(type) {
  const symbol = type.getSymbol();
  const declaration = symbol?.declarations?.[0];
  return declaration ? program.isSourceFileDefaultLibrary(declaration.getSourceFile()) : false;
}

function field(property, at) {
  const declaration = property.valueDeclaration ?? property.declarations?.[0] ?? at;
  return {
    name: property.getName(),
    optional: (property.flags & ts.SymbolFlags.Optional) !== 0,
    type: write(checker.getTypeOfSymbolAtLocation(property, declaration), declaration),
    // The property's own doc comment: its column's description.
    description: ts.displayPartsToString(property.getDocumentationComment(checker)).trim(),
  };
}

function write(type, at) {
  const f = type.flags;
  if (f & ts.TypeFlags.Any) return { kind: "any" };
  if (f & ts.TypeFlags.Unknown) return { kind: "unknown" };
  if (f & ts.TypeFlags.Never) return { kind: "never" };
  if (f & (ts.TypeFlags.Void | ts.TypeFlags.Undefined)) return { kind: "undefined" };
  if (f & ts.TypeFlags.Null) return { kind: "null" };
  if (f & ts.TypeFlags.StringLiteral) return { kind: "literal", value: type.value };
  if (f & ts.TypeFlags.NumberLiteral) return { kind: "literal", value: type.value };
  if (f & ts.TypeFlags.BooleanLiteral) return { kind: "boolean" };
  if (f & ts.TypeFlags.String) return { kind: "string" };
  if (f & ts.TypeFlags.Number) return { kind: "number" };
  if (f & ts.TypeFlags.BigInt) return { kind: "bigint" };
  if (f & ts.TypeFlags.Boolean) return { kind: "boolean" };
  if (type.isUnion()) {
    const name = type.aliasSymbol ? type.aliasSymbol.getName() : null;
    return { kind: "union", name, members: type.types.map((t) => write(t, at)) };
  }
  if (!(f & ts.TypeFlags.Object) && !type.isIntersection()) {
    return { kind: "unsupported", text: checker.typeToString(type) };
  }
  if (checker.isTupleType(type)) return { kind: "unsupported", text: checker.typeToString(type) };
  if (checker.isArrayType(type)) {
    return { kind: "array", element: write(checker.getTypeArguments(type)[0], at) };
  }
  const name = symbolName(type);
  if (name && fromLib(type)) {
    if (name === "Date") return { kind: "date" };
    if (name === "Promise" || name === "PromiseLike") {
      return { kind: "promise", value: write(checker.getTypeArguments(type)[0], at) };
    }
    if (name in ITERABLES) {
      const [isAsync, generator] = ITERABLES[name];
      return {
        kind: "iterable",
        async: isAsync,
        generator,
        element: write(checker.getTypeArguments(type)[0], at),
      };
    }
    return { kind: "unsupported", text: checker.typeToString(type) };
  }
  if (type.getCallSignatures().length > 0) {
    return { kind: "unsupported", text: checker.typeToString(type) };
  }
  const properties = checker.getPropertiesOfType(type);
  const index = checker.getIndexInfoOfType(type, ts.IndexKind.String);
  if (index && properties.length === 0) return { kind: "record", value: write(index.type, at) };
  const named = name && name !== "__type" && name !== "__object" ? name : null;
  // REQ-2100: Provisa's own types (the ReadContext, services.d.ts) are named, never read: theirs
  // are no columns, and their names are no name of the code's.
  if (named && fromServices(type)) return { kind: "ref", name: named };
  if (named) {
    if (named in types || inProgress.has(named)) return { kind: "ref", name: named };
    inProgress.add(named);
    const fields = properties.map((p) => field(p, at));
    inProgress.delete(named);
    types[named] = { kind: "object", name: named, fields };
    return { kind: "ref", name: named };
  }
  return { kind: "object", name: null, fields: properties.map((p) => field(p, at)) };
}

// REQ-2100: a tag's argument (`@loader 100`).
function tagText(tag) {
  const comment = typeof tag.comment === "string" ? tag.comment : ts.getTextOfJSDocComment(tag.comment);
  return (comment ?? "").trim();
}

function exported(symbol) {
  const target = symbol.flags & ts.SymbolFlags.Alias ? checker.getAliasedSymbol(symbol) : symbol;
  const declaration = target.valueDeclaration ?? target.declarations?.[0];
  const jsdoc = declaration ? ts.getJSDocTags(declaration) : [];
  const tags = jsdoc.map((t) => t.tagName.text);
  // What each tag's argument means is Provisa's (provisa/codesource/manifest.py).
  const tagArguments = jsdoc.map((t) => ({ tag: t.tagName.text, text: tagText(t) }));
  const description = ts.displayPartsToString(target.getDocumentationComment(checker)).trim();
  const out = { name: symbol.getName(), tags, tag_arguments: tagArguments, description, signatures: [] };
  if (!declaration) return out;
  const type = checker.getTypeOfSymbolAtLocation(target, declaration);
  for (const signature of type.getCallSignatures()) {
    out.signatures.push({
      parameters: signature.getParameters().map((p) => {
        const d = p.valueDeclaration ?? declaration;
        return {
          name: p.getName(),
          optional: checker.isOptionalParameter(d),
          rest: Boolean(d.dotDotDotToken),
          type: write(checker.getTypeOfSymbolAtLocation(p, d), d),
        };
      }),
      returns: write(signature.getReturnType(), declaration),
    });
  }
  return out;
}

const module = source ? checker.getSymbolAtLocation(source) : undefined;
const exports = module ? checker.getExportsOfModule(module).map(exported) : [];
process.stdout.write(JSON.stringify({ exports, types, diagnostics }));
