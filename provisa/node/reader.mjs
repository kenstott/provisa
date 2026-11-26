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
// REQ-1984: "@provisa/sdk" is Provisa's own copy of the optional SDK's declarations (sdk.d.ts beside
// this reader), whatever version of it the bundle installed or whether it installed one at all. An
// export's SDK brand is read as the JSDoc tags it stands for, and the SDK's type aliases are read as
// what they stand for.
const SDK_TYPES = new URL("./sdk.d.ts", import.meta.url).pathname;
const program = ts.createProgram([entry], {
  target: ts.ScriptTarget.ESNext,
  lib: ["lib.esnext.d.ts"],
  noEmit: true,
  // REQ-1993 (amended 2026-11-25): code entered in the source form is JavaScript with JSDoc types,
  // read from itself; a bundle's .d.ts entry is read as before.
  allowJs: true,
  skipLibCheck: true,
  strict: true,
  paths: { "@provisa/services": [SERVICES_TYPES], "@provisa/sdk": [SDK_TYPES] },
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

function declaredIn(symbol, file) {
  return (symbol?.declarations ?? []).some((d) => d.getSourceFile().fileName === file);
}

// REQ-1984: an alias the SDK declares (Rows<R>, Batch<T>, Text) is read as the type it stands for.
function alias(type) {
  return type.aliasSymbol && !declaredIn(type.aliasSymbol, SDK_TYPES) ? type.aliasSymbol : null;
}

function symbolName(type) {
  const symbol = alias(type) ?? type.getSymbol();
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
    const name = alias(type)?.getName() ?? null;
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
    if (name === "Record") {
      const index = checker.getIndexInfoOfType(type, ts.IndexKind.String);
      if (index) return { kind: "record", value: write(index.type, at) };
    }
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
  if (named === "TableRef" && fromServices(type)) {
    // REQ-2226: a parameter taking a governed table, its row type the columns it declares.
    const row = (type.aliasTypeArguments ?? checker.getTypeArguments(type))[0];
    return { kind: "table_ref", row: write(row, at) };
  }
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

// REQ-1984: the SDK spelling this server reads, from its own copy of the SDK. A bundle that does not
// import "@provisa/sdk" never loads it, and carries no brand.
const sdkFile = program.getSourceFile(SDK_TYPES);
const sdkModule = sdkFile ? checker.getSymbolAtLocation(sdkFile) : undefined;
const sdkExports = sdkModule ? checker.getExportsOfModule(sdkModule) : [];
const brandKey = sdkExports.find((s) => s.getName() === "provisa");
const spellingSymbol = sdkExports.find((s) => s.getName() === "SPELLING");
const SPELLING = spellingSymbol
  ? checker.getTypeOfSymbolAtLocation(spellingSymbol, spellingSymbol.valueDeclaration).value
  : null;
if (sdkFile && (!brandKey || typeof SPELLING !== "number")) throw new Error(`${SDK_TYPES} declares no provisa brand or SPELLING`);

function brandOf(type) {
  if (!sdkFile) return null;
  const property = checker.getPropertiesOfType(type).find(isBrand);
  return property ? checker.getNonNullableType(checker.getTypeOfSymbol(property)) : null;
}

function isBrand(property) {
  const declaration = property.valueDeclaration ?? property.declarations?.[0];
  if (!declaration || declaration.getSourceFile().fileName !== SDK_TYPES) return false;
  const name = declaration.name;
  return Boolean(name && ts.isComputedPropertyName(name) && checker.getSymbolAtLocation(name.expression) === brandKey);
}

function member(type, name) {
  const property = checker.getPropertyOfType(type, name);
  return property ? checker.getNonNullableType(checker.getTypeOfSymbol(property)) : null;
}

// REQ-1984: an option's value is a literal in the declaration, which is all the reader sees; any
// other is refused by name.
function literal(type) {
  if (type.flags & (ts.TypeFlags.StringLiteral | ts.TypeFlags.NumberLiteral)) return String(type.value);
  if (type.flags & ts.TypeFlags.BooleanLiteral) return checker.typeToString(type);
  return null;
}

// REQ-1984: an SDK brand as the JSDoc tags and tag arguments its JSDoc twin is read as, or the
// problems that refuse it by name.
function brandTags(name, brand) {
  const problems = [];
  const tags = [];
  const tagArguments = [];
  const spelling = literal(member(brand, "spelling") ?? checker.getNeverType());
  if (spelling === null || Number(spelling) > SPELLING) {
    problems.push(`${name}: it is branded with @provisa/sdk spelling ${spelling ?? "unknown"}; this server reads up to spelling ${SPELLING}`);
    return { tags, tagArguments, problems };
  }
  const kind = literal(member(brand, "kind") ?? checker.getNeverType());
  if (kind === null) {
    problems.push(`${name}: its @provisa/sdk brand names no kind`);
    return { tags, tagArguments, problems };
  }
  tags.push(kind);
  tagArguments.push({ tag: kind, text: "" });
  const options = member(brand, "options");
  for (const option of options ? checker.getPropertiesOfType(options) : []) {
    const key = option.getName();
    const value = checker.getNonNullableType(checker.getTypeOfSymbol(option));
    if (key === "loader") {
      const size = literal(value);
      if (size === null) {
        problems.push(`${name}: a @loader size is a literal number, and this one is ${checker.typeToString(value)}`);
      } else {
        tags.push("loader");
        tagArguments.push({ tag: "loader", text: size === "true" ? "" : size });
      }
    } else if (key === "requires") {
      const items = checker.isTupleType(value) ? checker.getTypeArguments(value) : null;
      for (const item of items ?? [value]) {
        const text = items ? literal(item) : null;
        if (text === null) {
          problems.push(`${name}: a @requires name is a literal string, and this one is ${checker.typeToString(item)}`);
        } else {
          tags.push("requires");
          tagArguments.push({ tag: "requires", text });
        }
      }
    } else if (key === "insert" || key === "update" || key === "delete") {
      const table = literal(value);
      if (table === null) {
        problems.push(`${name}: an @${key} table is a literal string, and this one is ${checker.typeToString(value)}`);
      } else {
        // A write binding is read from its tag argument alone (manifest.py's WRITE_TAGS).
        tagArguments.push({ tag: key, text: table });
      }
    } else {
      problems.push(`${name}: @provisa/sdk has no option ${key} in spelling ${SPELLING}`);
    }
  }
  return { tags, tagArguments, problems };
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
  const brand = brandOf(type);
  if (brand) {
    const read = brandTags(out.name, brand);
    tags.push(...read.tags);
    tagArguments.push(...read.tagArguments);
    diagnostics.push(...read.problems);
  }
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
