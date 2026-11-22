// Copyright (c) 2026 Kenneth Stott
// Canary: f774aa40-cb6f-405a-8ea5-c31b33d2115a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

//! The Rust source type's reader (REQ-2227): what a crate's public functions say about
//! themselves.
//!
//!     reader <the crate's root file (src/lib.rs)>
//!
//! It parses the crate's source with syn -- the root file and every module file it declares --
//! and writes one JSON document, the same every language's reader writes
//! (docs/arch/code-sources.md, section 2): each public function at the crate's root, with the
//! tags of its doc comment (a line that is exactly @name, or @name argument), the comment's other
//! text, and its parameters' and result's types as trees. Nothing is decided here; which functions
//! are tables, commands or streams, and what each type maps to, is Provisa's
//! (provisa/codesource/manifest.py). Reading runs none of the code and compiles nothing.
//!
//! A type is read as serde writes it, since the host sends what serde makes of a value: a
//! struct's fields under their serde names (rename, rename_all, skip, flatten, default), an enum of
//! unit variants as the text of its variants, a newtype as what it wraps. A named type is one the
//! crate declares; a type from another crate is read only where its serde form is known (chrono's
//! dates, serde_json's Value, rust_decimal's Decimal). A trailing Result is how a Rust function
//! fails, so it is not part of what it returns.
//!
//! A function's tags may also come from Provisa's optional SDK (REQ-1984): a registration such as
//! `provisa_sdk::table!(rows, secret = "API_KEY");` at the crate's root adds to `rows` the tags its
//! doc comment would carry. With no name resolution, a registration is known by its path: the SDK
//! crate's name (each name `Cargo.toml` gives the provisa-sdk package, passed after the root file,
//! and each `use provisa_sdk as x;`), or a name a root `use provisa_sdk::{...}` or
//! `use provisa_sdk::*` brings in. Its function is named by an identifier and every option's value
//! is a literal; anything else, and an SDK macro invoked anywhere but at the crate's root, is
//! refused by name. The SDK's type aliases (`provisa_sdk::Rows<T>` and the rest) are read as the
//! types they name.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use quote::ToTokens;
use serde_json::{json, Map, Value};
use syn::ext::IdentExt;
use syn::punctuated::Punctuated;
use syn::visit::Visit;
use syn::{
    Attribute, Expr, ExprLit, Fields, FnArg, GenericArgument, GenericParam, Ident, Item, ItemEnum,
    ItemFn, ItemStruct, ItemType, Lit, LitStr, Meta, Pat, PathArguments, ReturnType, Type,
    TypeParamBound, UseTree,
};

/// Provisa's optional SDK (REQ-1984): the spelling of it this reader reads, the tag each
/// registration adds, the options each takes, the parts of a change registration, and the SDK's
/// type aliases.
const SDK_SPELLING: u32 = 1;
const SDK_MACROS: &[(&str, &str)] = &[
    ("table", "table"),
    ("stream", "stream"),
    ("command", "command"),
    ("query", "query"),
    ("columns", "columns"),
    ("change", "change"),
    ("on_start", "on-start"),
    ("on_stop", "on-stop"),
    ("health", "health"),
];
const SDK_MACRO_NAMES: &[&str] =
    &["table", "stream", "command", "query", "columns", "change", "on_start", "on_stop", "health"];
const READ_OPTIONS: &[&str] = &["requires", "secret"];
const COMMAND_OPTIONS: &[&str] = &["requires", "secret", "insert", "update", "delete"];
const CHANGE_PARTS: &[&str] = &["table", "key", "removed", "cursor"];
const SDK_TYPES: &[&str] = &["Rows", "Stream", "Batch", "Results", "Text", "BigInt", "Double", "Boolean"];

fn sdk_tag(name: &str) -> Option<&'static str> {
    SDK_MACROS.iter().find(|(m, _)| *m == name).map(|(_, tag)| *tag)
}

/// The options a registration takes beside its function.
fn sdk_options(name: &str) -> Vec<&'static str> {
    match name {
        "command" => COMMAND_OPTIONS.to_vec(),
        "change" => [CHANGE_PARTS, READ_OPTIONS].concat(),
        "on_stop" | "health" => Vec::new(),
        _ => READ_OPTIONS.to_vec(),
    }
}

/// What the crate's root says of the SDK: the names its crate goes by, the names a root `use`
/// brings in from it (each to the SDK item it is), whether a root `use` brings in all of it, and
/// the macros the root defines itself, which a glob import does not shadow.
#[derive(Default)]
struct Sdk {
    crates: HashSet<String>,
    names: HashMap<String, String>,
    glob: bool,
    local_macros: HashSet<String>,
}

impl Sdk {
    fn from_root(crates: &[String], all: &[(String, Item)]) -> Sdk {
        let mut sdk = Sdk { crates: crates.iter().cloned().collect(), ..Sdk::default() };
        let root = || all.iter().filter(|(module, _)| module.is_empty()).map(|(_, item)| item);
        for item in root() {
            match item {
                Item::ExternCrate(e) if sdk.crates.contains(&name_of(&e.ident)) => {
                    if let Some((_, alias)) = &e.rename {
                        sdk.crates.insert(name_of(alias));
                    }
                }
                Item::Macro(m) => {
                    if let Some(ident) = &m.ident {
                        sdk.local_macros.insert(name_of(ident));
                    }
                }
                _ => {}
            }
        }
        // Twice: a `use provisa_sdk as p;` may come after the `use p::...` it names.
        for _ in 0..2 {
            for item in root() {
                if let Item::Use(u) = item {
                    sdk.uses(&u.tree, false);
                }
            }
        }
        sdk
    }

    /// The names a root `use` tree brings in from the SDK; `under` once the tree is below the
    /// SDK crate's own name. A path deeper than the SDK's root names nothing the SDK offers.
    fn uses(&mut self, tree: &UseTree, under: bool) {
        match tree {
            UseTree::Path(p) if !under && self.crates.contains(&name_of(&p.ident)) => self.uses(&p.tree, true),
            UseTree::Name(n) if under && n.ident != "self" => {
                self.names.insert(name_of(&n.ident), name_of(&n.ident));
            }
            UseTree::Rename(r) if under && r.ident == "self" => {
                self.crates.insert(name_of(&r.rename));
            }
            UseTree::Rename(r) if under => {
                self.names.insert(name_of(&r.rename), name_of(&r.ident));
            }
            UseTree::Rename(r) if self.crates.contains(&name_of(&r.ident)) => {
                self.crates.insert(name_of(&r.rename));
            }
            UseTree::Glob(_) if under => self.glob = true,
            UseTree::Group(g) => {
                for item in &g.items {
                    self.uses(item, under);
                }
            }
            _ => {}
        }
    }

    /// The SDK item `path` names: `<the SDK crate>::item`, or, where the root's `use`s apply
    /// (`root`), a name one brought in, or one of `offered` a glob import brought in that `local`
    /// does not declare. A module's own `use`s are not followed.
    fn resolve(&self, path: &syn::Path, root: bool, offered: &[&str], local: impl Fn(&str) -> bool) -> Option<String> {
        let segments: Vec<String> = path.segments.iter().map(|s| name_of(&s.ident)).collect();
        match segments.as_slice() {
            [krate, item] if self.crates.contains(krate) => Some(item.clone()),
            [name] if root && path.leading_colon.is_none() => match self.names.get(name) {
                Some(item) => Some(item.clone()),
                None if self.glob && offered.contains(&name.as_str()) && !local(name) => Some(name.clone()),
                None => None,
            },
            _ => None,
        }
    }

    fn macro_item(&self, path: &syn::Path, root: bool) -> Option<String> {
        self.resolve(path, root, SDK_MACRO_NAMES, |name| self.local_macros.contains(name))
    }
}

/// Every SDK macro invoked inside an item: none is read there.
struct Nested<'a> {
    sdk: &'a Sdk,
    root: bool,
    found: Vec<String>,
}

impl<'ast> Visit<'ast> for Nested<'_> {
    fn visit_macro(&mut self, mac: &'ast syn::Macro) {
        if let Some(item) = self.sdk.macro_item(&mac.path, self.root) {
            self.found.push(item);
        }
    }
}

/// A registration's value, as its tag line writes it: a text literal, or, for a change's key,
/// an array of text literals joined by commas.
fn literal(value: &Expr, list: bool) -> Option<String> {
    match value {
        Expr::Lit(ExprLit { lit: Lit::Str(s), .. }) => Some(s.value()),
        Expr::Array(array) if list && !array.elems.is_empty() => {
            let parts: Option<Vec<String>> = array.elems.iter().map(|e| literal(e, false)).collect();
            parts.map(|p| p.join(","))
        }
        Expr::Paren(p) => literal(&p.expr, list),
        Expr::Group(g) => literal(&g.expr, list),
        _ => None,
    }
}

enum Decl {
    Struct(ItemStruct),
    Enum(ItemEnum),
    Alias(ItemType),
}

struct Declared {
    module: String,
    decl: Decl,
}

struct Reader {
    declared: HashMap<String, Vec<Declared>>,
    types: Map<String, Value>,
    reading: HashSet<String>,
    diagnostics: Vec<String>,
    sdk: Sdk,
}

fn unsupported(text: &str) -> Value {
    json!({"kind": "unsupported", "text": text})
}

/// A type as it is written, without the spaces the token printer puts between its tokens.
fn text<T: ToTokens>(item: &T) -> String {
    let raw = item.to_token_stream().to_string();
    let chars: Vec<char> = raw.chars().collect();
    let tight = |c: char| "<>:&,()[]".contains(c);
    let mut out = String::new();
    for (i, c) in chars.iter().enumerate() {
        if *c == ' ' {
            let before = i > 0 && tight(chars[i - 1]);
            let after = i + 1 < chars.len() && tight(chars[i + 1]);
            if before || after {
                continue;
            }
        }
        out.push(*c);
    }
    out.replace(',', ", ")
}

fn name_of(ident: &Ident) -> String {
    ident.unraw().to_string()
}

/// The text of a doc comment: each `///` line, its first space removed.
fn doc_lines(attrs: &[Attribute]) -> Vec<String> {
    let mut out = Vec::new();
    for attr in attrs {
        if !attr.path().is_ident("doc") {
            continue;
        }
        if let Meta::NameValue(nv) = &attr.meta {
            if let Expr::Lit(ExprLit { lit: Lit::Str(s), .. }) = &nv.value {
                for line in s.value().split('\n') {
                    out.push(line.strip_prefix(' ').unwrap_or(line).to_string());
                }
            }
        }
    }
    out
}

fn doc_text(attrs: &[Attribute]) -> String {
    doc_lines(attrs).join("\n").trim().to_string()
}

fn is_cfg_test(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        if !attr.path().is_ident("cfg") {
            return false;
        }
        let mut test = false;
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("test") {
                test = true;
            }
            Ok(())
        });
        test
    })
}

/// What a serde attribute says of an item, a field or a variant.
#[derive(Default)]
struct Serde {
    rename: Option<String>,
    rename_all: Option<String>,
    skip: bool,
    flatten: bool,
    default: bool,
}

fn skip_meta_value(meta: &syn::meta::ParseNestedMeta) -> syn::Result<()> {
    if meta.input.peek(syn::Token![=]) {
        let _: Expr = meta.value()?.parse()?;
    } else if meta.input.peek(syn::token::Paren) {
        let content;
        syn::parenthesized!(content in meta.input);
        let _: proc_macro2::TokenStream = content.parse()?;
    }
    Ok(())
}

/// The serialized name a `rename(serialize = "x")` or `rename = "x"` gives.
fn renamed(meta: &syn::meta::ParseNestedMeta) -> syn::Result<Option<String>> {
    if meta.input.peek(syn::Token![=]) {
        let s: LitStr = meta.value()?.parse()?;
        return Ok(Some(s.value()));
    }
    let mut found = None;
    meta.parse_nested_meta(|inner| {
        if inner.path.is_ident("serialize") {
            let s: LitStr = inner.value()?.parse()?;
            found = Some(s.value());
        } else {
            skip_meta_value(&inner)?;
        }
        Ok(())
    })?;
    Ok(found)
}

fn serde_of(attrs: &[Attribute]) -> Serde {
    let mut out = Serde::default();
    for attr in attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                out.rename = renamed(&meta)?;
            } else if meta.path.is_ident("rename_all") {
                out.rename_all = renamed(&meta)?;
            } else if meta.path.is_ident("skip") || meta.path.is_ident("skip_serializing") {
                out.skip = true;
            } else if meta.path.is_ident("flatten") {
                out.flatten = true;
            } else if meta.path.is_ident("default") {
                out.default = true;
                skip_meta_value(&meta)?;
            } else {
                skip_meta_value(&meta)?;
            }
            Ok(())
        });
    }
    out
}

fn words_of_snake(name: &str) -> Vec<String> {
    name.split('_').filter(|w| !w.is_empty()).map(str::to_lowercase).collect()
}

fn words_of_pascal(name: &str) -> Vec<String> {
    let mut words: Vec<String> = Vec::new();
    for c in name.chars() {
        if c.is_uppercase() || words.is_empty() {
            words.push(String::new());
        }
        words.last_mut().unwrap().extend(c.to_lowercase());
    }
    words
}

fn capitalized(word: &str) -> String {
    let mut chars = word.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => String::new(),
    }
}

/// A name under serde's `rename_all` rule, as serde applies it: a field's own name is written in
/// words joined by underscores, a variant's in capitalised words.
fn cased(name: &str, rule: &str, variant: bool) -> String {
    let words = if variant { words_of_pascal(name) } else { words_of_snake(name) };
    match rule {
        "lowercase" => words.concat(),
        "UPPERCASE" => words.concat().to_uppercase(),
        "PascalCase" => words.iter().map(|w| capitalized(w)).collect(),
        "camelCase" => words
            .iter()
            .enumerate()
            .map(|(i, w)| if i == 0 { w.clone() } else { capitalized(w) })
            .collect(),
        "snake_case" => words.join("_"),
        "SCREAMING_SNAKE_CASE" => words.join("_").to_uppercase(),
        "kebab-case" => words.join("-"),
        "SCREAMING-KEBAB-CASE" => words.join("-").to_uppercase(),
        _ => name.to_string(),
    }
}

/// The type arguments of a path segment, in order.
fn type_args(arguments: &PathArguments) -> Vec<&Type> {
    match arguments {
        PathArguments::AngleBracketed(ab) => ab
            .args
            .iter()
            .filter_map(|a| match a {
                GenericArgument::Type(t) => Some(t),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// The `Item = X` of an `Iterator<Item = X>` or `IntoIterator<Item = X>` bound.
fn iterator_item<'a>(bounds: impl Iterator<Item = &'a TypeParamBound>) -> Option<&'a Type> {
    for bound in bounds {
        if let TypeParamBound::Trait(tb) = bound {
            let last = tb.path.segments.last()?;
            if last.ident != "Iterator" && last.ident != "IntoIterator" {
                continue;
            }
            if let PathArguments::AngleBracketed(ab) = &last.arguments {
                for arg in &ab.args {
                    if let GenericArgument::AssocType(at) = arg {
                        if at.ident == "Item" {
                            return Some(&at.ty);
                        }
                    }
                }
            }
        }
    }
    None
}

/// Whether a type is written as a `Result`, and what it holds when it succeeds.
fn result_of(ty: &Type) -> Option<&Type> {
    if let Type::Path(tp) = ty {
        let last = tp.path.segments.last()?;
        if last.ident == "Result" {
            return type_args(&last.arguments).first().copied();
        }
    }
    None
}

fn is_unit(ty: &Type) -> bool {
    matches!(ty, Type::Tuple(t) if t.elems.is_empty())
}

fn is_option(ty: &Type) -> bool {
    matches!(ty, Type::Path(tp) if tp.path.segments.last().is_some_and(|s| s.ident == "Option"))
}

impl Reader {
    /// `ty` as a type tree.
    fn tree(&mut self, ty: &Type) -> Value {
        match ty {
            Type::Paren(p) => self.tree(&p.elem),
            Type::Group(g) => self.tree(&g.elem),
            // A borrowed value is written as the value it borrows (a parameter refuses it).
            Type::Reference(r) => self.tree(&r.elem),
            Type::Slice(s) => json!({"kind": "array", "element": self.tree(&s.elem)}),
            Type::Array(a) => json!({"kind": "array", "element": self.tree(&a.elem)}),
            Type::Tuple(t) if t.elems.is_empty() => json!({"kind": "undefined"}),
            Type::ImplTrait(it) => self.iterable(iterator_item(it.bounds.iter()), ty),
            Type::TraitObject(to) => self.iterable(iterator_item(to.bounds.iter()), ty),
            Type::Path(tp) if tp.qself.is_none() => self.path(tp, ty),
            _ => unsupported(&text(ty)),
        }
    }

    /// An iterator's tree: its items are rows, or rows each in a Result.
    fn iterable(&mut self, item: Option<&Type>, ty: &Type) -> Value {
        let Some(item) = item else {
            return unsupported(&text(ty));
        };
        let (element, errors) = match result_of(item) {
            Some(inner) => (self.tree(inner), true),
            None => (self.tree(item), false),
        };
        json!({"kind": "iterable", "async": false, "generator": true, "errors": errors, "element": element})
    }

    fn path(&mut self, tp: &syn::TypePath, ty: &Type) -> Value {
        let segments = &tp.path.segments;
        let last = segments.last().expect("a path has a segment");
        let name = name_of(&last.ident);
        let args = type_args(&last.arguments);
        let first = segments.first().map(|s| s.ident.to_string()).unwrap_or_default();
        // REQ-2226: a parameter taking a governed table, TableRef<Row>, Row the columns it
        // declares. A Rust source has no Provisa crate to import: the code declares TableRef.
        if name == "TableRef" && args.len() == 1 {
            return json!({"kind": "table_ref", "row": self.tree(args[0])});
        }
        let local = segments.len() == 1 || matches!(first.as_str(), "crate" | "self" | "super");
        if local && self.declared.contains_key(&name) {
            return self.named(&name, ty);
        }
        let declared = &self.declared;
        if let Some(item) = self.sdk.resolve(&tp.path, true, SDK_TYPES, |n| declared.contains_key(n)) {
            return self.sdk_type(&item, &args, ty);
        }
        match (name.as_str(), args.as_slice()) {
            ("String" | "str" | "char", []) => json!({"kind": "string"}),
            ("bool", []) => json!({"kind": "boolean"}),
            ("i8" | "i16" | "i32" | "i64" | "isize" | "u8" | "u16" | "u32" | "u64" | "usize", []) => {
                json!({"kind": "number", "integer": true})
            }
            ("f32" | "f64", []) => json!({"kind": "number"}),
            ("Option", [inner]) => {
                json!({"kind": "union", "members": [self.tree(inner), {"kind": "null"}]})
            }
            ("Vec" | "VecDeque" | "LinkedList" | "HashSet" | "BTreeSet" | "IndexSet", [inner, ..]) => {
                json!({"kind": "array", "element": self.tree(inner)})
            }
            ("Box" | "Rc" | "Arc" | "Cow", [inner]) => self.tree(inner),
            ("HashMap" | "BTreeMap" | "IndexMap", [key, _value, ..]) => {
                match self.tree(key).get("kind").and_then(Value::as_str) {
                    Some("string") => json!({"kind": "record"}),
                    _ => unsupported(&text(ty)),
                }
            }
            ("Value", []) if first == "serde_json" || segments.len() == 1 => json!({"kind": "any"}),
            ("DateTime", [_]) | ("NaiveDateTime", []) => json!({"kind": "date"}),
            ("NaiveDate", []) => json!({"kind": "day"}),
            ("Decimal", []) => json!({"kind": "decimal"}),
            _ if self.declared.contains_key(&name) => self.named(&name, ty),
            _ => unsupported(&text(ty)),
        }
    }

    /// One of the SDK's type aliases, read as the type it names.
    fn sdk_type(&mut self, item: &str, args: &[&Type], ty: &Type) -> Value {
        match (item, args) {
            ("Rows" | "Batch" | "Results", [inner]) => json!({"kind": "array", "element": self.tree(inner)}),
            ("Stream", [inner]) => self.iterable(Some(inner), ty),
            ("Text", []) => json!({"kind": "string"}),
            ("BigInt", []) => json!({"kind": "number", "integer": true}),
            ("Double", []) => json!({"kind": "number"}),
            ("Boolean", []) => json!({"kind": "boolean"}),
            _ => unsupported(&format!(
                "{} (not a type of provisa_sdk spelling {SDK_SPELLING}, as written)",
                text(ty)
            )),
        }
    }

    /// Adds to `exports` the tags of one registration, `mac`, of the SDK macro `item`. Its first
    /// argument names a `pub fn` at the crate's root (one of `exports`; `private` are the root's
    /// other functions); each option is `key = literal`, a key the registration takes.
    fn register(&mut self, item: &str, mac: &syn::Macro, exports: &mut [Value], private: &HashSet<String>) {
        let Some(tag) = sdk_tag(item) else {
            self.diagnostics.push(format!(
                "provisa_sdk::{item}! is not a registration this server reads; it reads SDK spelling {SDK_SPELLING}"
            ));
            return;
        };
        let written = format!("provisa_sdk::{item}!({})", text(&mac.tokens));
        let arguments = match mac.parse_body_with(Punctuated::<Expr, syn::Token![,]>::parse_terminated) {
            Ok(arguments) => arguments,
            Err(err) => {
                self.diagnostics.push(format!("{written}: its arguments cannot be read: {err}"));
                return;
            }
        };
        let mut arguments = arguments.into_iter();
        let function = match arguments.next() {
            Some(Expr::Path(p)) if p.qself.is_none() && p.path.get_ident().is_some() => {
                name_of(p.path.get_ident().expect("an identifier"))
            }
            Some(other) => {
                self.diagnostics.push(format!(
                    "{written}: a registration names a pub fn at the crate's root, not {}",
                    text(&other)
                ));
                return;
            }
            None => {
                self.diagnostics.push(format!("{written}: a registration names a pub fn at the crate's root"));
                return;
            }
        };
        let Some(target) = exports.iter().position(|e| e["name"] == function.as_str()) else {
            let why = if private.contains(&function) {
                format!("{function} is not pub, and Provisa serves a pub fn")
            } else {
                format!("there is no fn {function} at the crate's root")
            };
            self.diagnostics.push(format!("{written}: {why}"));
            return;
        };
        let allowed = sdk_options(item);
        let mut added = Vec::new();
        let mut parts = Vec::new();
        for argument in arguments {
            let Expr::Assign(assign) = &argument else {
                self.diagnostics.push(format!(
                    "{function}: an option of provisa_sdk::{item}! is key = literal, not {}",
                    text(&argument)
                ));
                continue;
            };
            let key = match &*assign.left {
                Expr::Path(p) if p.path.get_ident().is_some() => name_of(p.path.get_ident().expect("an identifier")),
                other => {
                    self.diagnostics.push(format!(
                        "{function}: an option of provisa_sdk::{item}! is key = literal, not {} = ...",
                        text(other)
                    ));
                    continue;
                }
            };
            if !allowed.contains(&key.as_str()) {
                let why = if key == "loader" {
                    ": a Rust source has no read context, so no @loader".to_string()
                } else {
                    String::new()
                };
                self.diagnostics.push(format!("{function}: provisa_sdk::{item}! takes no option {key}{why}"));
                continue;
            }
            let Some(value) = literal(&assign.right, key == "key") else {
                self.diagnostics.push(format!(
                    "{function}: provisa_sdk::{item}!'s {key} is a literal, not {}",
                    text(&*assign.right)
                ));
                continue;
            };
            match key.as_str() {
                "requires" => added.push(json!({"tag": "requires", "text": value})),
                "secret" => added.push(json!({"tag": "requires", "text": format!("{value}:secret")})),
                part if CHANGE_PARTS.contains(&part) && item == "change" => {
                    parts.push(part.to_string());
                    added.push(json!({"tag": "change", "text": format!("{part}={value}")}));
                }
                write => added.push(json!({"tag": write, "text": value})),
            }
        }
        if item == "change" && parts != CHANGE_PARTS {
            self.diagnostics.push(format!(
                "{function}: provisa_sdk::change! gives table, key, removed and cursor, once each and in that order"
            ));
            return;
        }
        let export = &mut exports[target];
        if item != "change" {
            let tags = export["tags"].as_array_mut().expect("an export's tags");
            if !tags.iter().any(|t| t == tag) {
                tags.push(Value::String(tag.to_string()));
            }
        }
        export["tag_arguments"].as_array_mut().expect("an export's tag arguments").extend(added);
    }

    /// A type the crate declares: a struct as a named object type, an enum of unit variants as
    /// the text of its variants, a newtype as what it wraps, an alias as what it names.
    fn named(&mut self, name: &str, ty: &Type) -> Value {
        let Some(found) = self.declared.get(name) else {
            return unsupported(&text(ty));
        };
        if found.len() > 1 {
            let modules: Vec<String> = found
                .iter()
                .map(|d| if d.module.is_empty() { "the crate's root".to_string() } else { d.module.clone() })
                .collect();
            self.diagnostics.push(format!(
                "two types are named {name}, in {}; a named type's name is its own",
                modules.join(" and ")
            ));
            return unsupported(&text(ty));
        }
        match &found[0].decl {
            Decl::Alias(alias) => {
                if !alias.generics.params.is_empty() || self.reading.contains(name) {
                    return unsupported(&text(ty));
                }
                let aliased = (*alias.ty).clone();
                self.reading.insert(name.to_string());
                let out = self.tree(&aliased);
                self.reading.remove(name);
                out
            }
            Decl::Enum(item) => {
                let item = item.clone();
                self.enumeration(&item, ty)
            }
            Decl::Struct(item) => {
                let item = item.clone();
                self.structure(&item, ty)
            }
        }
    }

    fn enumeration(&mut self, item: &ItemEnum, ty: &Type) -> Value {
        let name = name_of(&item.ident);
        if !item.generics.params.is_empty() || item.variants.is_empty() {
            return unsupported(&text(ty));
        }
        let rule = serde_of(&item.attrs).rename_all;
        let mut members = Vec::new();
        for variant in &item.variants {
            if !matches!(variant.fields, Fields::Unit) {
                return unsupported(&format!("{name} (an enum whose variants carry data)"));
            }
            let serde = serde_of(&variant.attrs);
            if serde.skip {
                continue;
            }
            let own = name_of(&variant.ident);
            let value = serde
                .rename
                .unwrap_or_else(|| rule.as_deref().map_or(own.clone(), |r| cased(&own, r, true)));
            members.push(json!({"kind": "literal", "value": value}));
        }
        json!({"kind": "union", "name": name, "members": members})
    }

    fn structure(&mut self, item: &ItemStruct, ty: &Type) -> Value {
        let name = name_of(&item.ident);
        if item.generics.params.iter().any(|p| !matches!(p, GenericParam::Lifetime(_))) {
            return unsupported(&format!("{} (a generic type)", text(ty)));
        }
        match &item.fields {
            Fields::Named(_) => {
                if !self.types.contains_key(&name) && !self.reading.contains(&name) {
                    self.reading.insert(name.clone());
                    let fields = self.fields(item);
                    self.types.insert(name.clone(), json!({"name": name, "fields": fields}));
                    self.reading.remove(&name);
                }
                json!({"kind": "ref", "name": name})
            }
            // A newtype is written as what it wraps.
            Fields::Unnamed(u) if u.unnamed.len() == 1 => {
                let inner = u.unnamed[0].ty.clone();
                self.tree(&inner)
            }
            _ => unsupported(&format!("{name} (a struct with no named fields)")),
        }
    }

    /// A struct's fields as serde writes them.
    fn fields(&mut self, item: &ItemStruct) -> Vec<Value> {
        let rule = serde_of(&item.attrs).rename_all;
        let mut out = Vec::new();
        let Fields::Named(named) = &item.fields else {
            return out;
        };
        for field in &named.named {
            let serde = serde_of(&field.attrs);
            if serde.skip {
                continue;
            }
            if serde.flatten {
                if let Type::Path(tp) = &field.ty {
                    let inner = name_of(&tp.path.segments.last().unwrap().ident);
                    let flat = match self.declared.get(&inner).map(Vec::as_slice) {
                        Some([Declared { decl: Decl::Struct(flat), .. }]) => Some(flat.clone()),
                        _ => None,
                    };
                    if let Some(flat) = flat {
                        out.extend(self.fields(&flat));
                        continue;
                    }
                }
                out.push(json!({
                    "name": field.ident.as_ref().map(name_of).unwrap_or_default(),
                    "optional": false, "default": false, "description": "",
                    "type": unsupported(&format!("{} (flattened)", text(&field.ty))),
                }));
                continue;
            }
            let own = field.ident.as_ref().map(name_of).unwrap_or_default();
            let name = serde
                .rename
                .unwrap_or_else(|| rule.as_deref().map_or(own.clone(), |r| cased(&own, r, false)));
            out.push(json!({
                "name": name,
                "optional": is_option(&field.ty),
                "default": serde.default,
                "type": self.tree(&field.ty),
                "description": doc_text(&field.attrs),
            }));
        }
        out
    }

    fn function(&mut self, item: &ItemFn) -> Value {
        let mut tags = Vec::new();
        let mut tag_arguments = Vec::new();
        let mut kept = Vec::new();
        for line in doc_lines(&item.attrs) {
            let trimmed = line.trim();
            match tag_of(trimmed) {
                Some((tag, None)) => tags.push(Value::String(tag)),
                Some((tag, Some(argument))) => tag_arguments.push(json!({"tag": tag, "text": argument})),
                None => kept.push(line),
            }
        }
        let sig = &item.sig;
        let mut parameters = Vec::new();
        for input in &sig.inputs {
            let FnArg::Typed(typed) = input else { continue };
            let name = match &*typed.pat {
                Pat::Ident(p) => name_of(&p.ident),
                other => text(other),
            };
            let tree = match &*typed.ty {
                // The host decodes each argument into a value the function owns.
                Type::Reference(_) => unsupported(&format!(
                    "{} (a parameter is taken by value, not borrowed)",
                    text(&*typed.ty)
                )),
                other => self.tree(other),
            };
            parameters.push(json!({
                "name": name, "optional": is_option(&typed.ty), "rest": false, "type": tree,
            }));
        }
        let (mut returns, fails) = match &sig.output {
            ReturnType::Default => (json!({"kind": "undefined"}), false),
            ReturnType::Type(_, ty) => match result_of(ty) {
                Some(inner) if is_unit(inner) => (json!({"kind": "undefined"}), true),
                Some(inner) => (self.tree(inner), true),
                None => (self.tree(ty), false),
            },
        };
        if sig.generics.params.iter().any(|p| !matches!(p, GenericParam::Lifetime(_))) {
            returns = unsupported("a generic function");
        }
        if sig.asyncness.is_some() {
            returns = unsupported("an async function (a Rust source's functions are not async)");
        }
        json!({
            "name": name_of(&sig.ident),
            "tags": tags,
            "tag_arguments": tag_arguments,
            "description": kept.join("\n").trim(),
            "signatures": [{"parameters": parameters, "returns": returns, "context": false, "fails": fails}],
        })
    }
}

/// A doc line's tag: `@name` alone, or `@name argument`.
fn tag_of(line: &str) -> Option<(String, Option<String>)> {
    let rest = line.strip_prefix('@')?;
    let mut parts = rest.split_whitespace();
    let tag = parts.next()?;
    let mut chars = tag.chars();
    let head = chars.next()?;
    if !(head.is_ascii_alphabetic() || head == '_')
        || !chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return None;
    }
    let argument = parts.next().map(str::to_string);
    if parts.next().is_some() {
        return None;
    }
    Some((tag.to_string(), argument))
}

/// Every item of the crate, by the module it is in ("" for the root), from the root file and the
/// module files it declares; a file that cannot be read or parsed is a diagnostic.
fn load(file: &Path, dir: &Path, module: &str, out: &mut Vec<(String, Item)>, diagnostics: &mut Vec<String>) {
    let source = match fs::read_to_string(file) {
        Ok(text) => text,
        Err(err) => {
            diagnostics.push(format!("{}: {err}", file.display()));
            return;
        }
    };
    match syn::parse_file(&source) {
        Ok(parsed) => items(parsed.items, dir, module, out, diagnostics),
        Err(err) => diagnostics.push(format!("{}: {err}", file.display())),
    }
}

fn items(found: Vec<Item>, dir: &Path, module: &str, out: &mut Vec<(String, Item)>, diagnostics: &mut Vec<String>) {
    for item in found {
        if let Item::Mod(m) = &item {
            if is_cfg_test(&m.attrs) {
                continue;
            }
            let name = name_of(&m.ident);
            let inner = if module.is_empty() { name.clone() } else { format!("{module}::{name}") };
            match &m.content {
                Some((_, nested)) => items(nested.to_vec(), &dir.join(&name), &inner, out, diagnostics),
                None => {
                    let explicit = m.attrs.iter().find(|a| a.path().is_ident("path")).and_then(|a| match &a.meta {
                        Meta::NameValue(nv) => match &nv.value {
                            Expr::Lit(ExprLit { lit: Lit::Str(s), .. }) => Some(dir.join(s.value())),
                            _ => None,
                        },
                        _ => None,
                    });
                    let candidates: Vec<(PathBuf, PathBuf)> = match explicit {
                        Some(path) => {
                            let parent = path.parent().map(Path::to_path_buf).unwrap_or_default();
                            vec![(path, parent)]
                        }
                        None => vec![
                            (dir.join(format!("{name}.rs")), dir.join(&name)),
                            (dir.join(&name).join("mod.rs"), dir.join(&name)),
                        ],
                    };
                    match candidates.into_iter().find(|(file, _)| file.is_file()) {
                        Some((file, child_dir)) => load(&file, &child_dir, &inner, out, diagnostics),
                        None => diagnostics.push(format!(
                            "module {inner}: there is no {name}.rs or {name}/mod.rs in {}",
                            dir.display()
                        )),
                    }
                }
            }
            continue;
        }
        out.push((module.to_string(), item));
    }
}

/// The crate's document; `sdk_crates` are the names its `Cargo.toml` gives the SDK.
fn read(root: &Path, sdk_crates: &[String]) -> Value {
    let mut all = Vec::new();
    let mut diagnostics = Vec::new();
    let dir = root.parent().map(Path::to_path_buf).unwrap_or_default();
    load(root, &dir, "", &mut all, &mut diagnostics);
    let sdk = Sdk::from_root(sdk_crates, &all);
    let mut registrations = Vec::new();
    let mut nested = Vec::new();
    for (module, item) in &all {
        match item {
            Item::Macro(m) if module.is_empty() && m.ident.is_none() => match sdk.macro_item(&m.mac.path, true) {
                Some(found) => registrations.push((found, m.mac.clone())),
                None => continue,
            },
            other => {
                let mut finder = Nested { sdk: &sdk, root: module.is_empty(), found: Vec::new() };
                finder.visit_item(other);
                let place = if module.is_empty() { "inside an item".to_string() } else { format!("in module {module}") };
                nested.extend(finder.found.into_iter().map(|found| {
                    format!(
                        "provisa_sdk::{found}! is invoked {place}, where Provisa does not read it; \
                         a registration is an item at the crate's root"
                    )
                }));
            }
        }
    }
    diagnostics.extend(nested);
    let mut reader = Reader {
        declared: HashMap::new(),
        types: Map::new(),
        reading: HashSet::new(),
        diagnostics,
        sdk,
    };
    let mut functions = Vec::new();
    let mut private = HashSet::new();
    for (module, item) in all {
        let (name, decl) = match item {
            Item::Struct(s) => (name_of(&s.ident), Decl::Struct(s)),
            Item::Enum(e) => (name_of(&e.ident), Decl::Enum(e)),
            Item::Type(t) => (name_of(&t.ident), Decl::Alias(t)),
            Item::Fn(f) if module.is_empty() && matches!(f.vis, syn::Visibility::Public(_)) => {
                functions.push(f);
                continue;
            }
            Item::Fn(f) if module.is_empty() => {
                private.insert(name_of(&f.sig.ident));
                continue;
            }
            _ => continue,
        };
        reader.declared.entry(name).or_default().push(Declared { module, decl });
    }
    let mut exports: Vec<Value> = functions.iter().map(|f| reader.function(f)).collect();
    for (item, mac) in &registrations {
        reader.register(item, mac, &mut exports, &private);
    }
    json!({"exports": exports, "types": reader.types, "diagnostics": reader.diagnostics})
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: reader <the crate's root file> [the name Cargo.toml gives provisa-sdk]...");
        std::process::exit(2);
    }
    println!("{}", read(Path::new(&args[1]), &args[2..]));
}
