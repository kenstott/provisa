// Copyright (c) 2026 Kenneth Stott
// Canary: 5d0e1c8a-7a43-4f0b-9a52-2f6f1b3c9e71
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Go source type's reader (REQ-2200): what a package's exported functions say about
// themselves.
//
//	reader <module directory> <package pattern>
//
// It type-checks the package with go/packages and go/types and writes one JSON document, the
// same every language's reader writes (docs/arch/code-sources.md, section 2): each exported
// top-level function of the package, with the tags of its doc comment (a line that is exactly
// @name, or @name argument), the comment's other text, and its parameters' and result's types
// as trees. Nothing is decided here; which functions are tables, commands or streams, and what
// each type maps to, is Provisa's (provisa/codesource/manifest.py). Reading types runs none of
// the code: the go command lists the package and its dependencies, which are already installed.
//
// A Go function's leading context.Context is the call's own (cancelled when the call is), not
// one of its inputs, so it is written as a flag on the signature and not as a parameter. A
// trailing error result is how a Go function fails, so it is not part of what it returns.
package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/types"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

var (
	tagLine     = regexp.MustCompile(`^@([A-Za-z_][A-Za-z0-9_]*)$`)
	tagArgument = regexp.MustCompile(`^@([A-Za-z_][A-Za-z0-9_]*)\s+(\S+)$`)
)

type node = map[string]any

type field struct {
	Name        string `json:"name"`
	Optional    bool   `json:"optional"`
	Default     bool   `json:"default"`
	Type        node   `json:"type"`
	Description string `json:"description"`
}

type objectType struct {
	Name   string  `json:"name"`
	Fields []field `json:"fields"`
}

type parameter struct {
	Name     string `json:"name"`
	Optional bool   `json:"optional"`
	Rest     bool   `json:"rest"`
	Type     node   `json:"type"`
}

type signature struct {
	Parameters []parameter `json:"parameters"`
	Returns    node        `json:"returns"`
	// Whether the function takes the call's context.Context first.
	Context bool `json:"context"`
	// Whether it returns an error last.
	Fails bool `json:"fails"`
}

type tagArg struct {
	Tag  string `json:"tag"`
	Text string `json:"text"`
}

type export struct {
	Name         string      `json:"name"`
	Tags         []string    `json:"tags"`
	TagArguments []tagArg    `json:"tag_arguments"`
	Description  string      `json:"description"`
	Signatures   []signature `json:"signatures"`
}

type document struct {
	Exports     []export              `json:"exports"`
	Types       map[string]objectType `json:"types"`
	Diagnostics []string              `json:"diagnostics"`
}

type reader struct {
	pkg     *packages.Package
	types   map[string]objectType
	reading map[string]bool
	// A named type's package path, by the name it is written as: two types of one name are
	// refused by name.
	owners map[string]string
	// A field's doc comment, by its position, for the package's own structs.
	fieldDocs   map[string]string
	diagnostics []string
}

func isNamed(t types.Type, path, name string) bool {
	named, ok := types.Unalias(t).(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return false
	}
	return named.Obj().Pkg().Path() == path && named.Obj().Name() == name
}

func isError(t types.Type) bool {
	return types.Identical(t, types.Universe.Lookup("error").Type())
}

func unsupported(text string) node {
	return node{"kind": "unsupported", "text": text}
}

// tree is t as a type tree.
func (r *reader) tree(t types.Type) node {
	t = types.Unalias(t)
	if isNamed(t, "time", "Time") {
		return node{"kind": "date"}
	}
	switch u := t.(type) {
	case *types.Basic:
		return basic(u)
	case *types.Pointer:
		return node{"kind": "union", "members": []node{r.tree(u.Elem()), {"kind": "null"}}}
	case *types.Slice:
		return node{"kind": "array", "element": r.tree(u.Elem())}
	case *types.Array:
		return node{"kind": "array", "element": r.tree(u.Elem())}
	case *types.Map:
		if key, ok := u.Key().Underlying().(*types.Basic); ok && key.Kind() == types.String {
			return node{"kind": "record"}
		}
		return unsupported(types.TypeString(u, r.qualifier))
	case *types.Interface:
		if u.Empty() {
			return node{"kind": "any"}
		}
		return unsupported(types.TypeString(u, r.qualifier))
	case *types.Struct:
		return node{"kind": "object", "fields": r.fields(u)}
	case *types.Named:
		return r.named(u)
	}
	return unsupported(types.TypeString(t, r.qualifier))
}

func basic(b *types.Basic) node {
	info := b.Info()
	switch {
	case info&types.IsBoolean != 0:
		return node{"kind": "boolean"}
	case info&types.IsString != 0:
		return node{"kind": "string"}
	case info&types.IsInteger != 0 && b.Kind() != types.Uintptr && b.Kind() != types.UnsafePointer:
		return node{"kind": "number", "integer": true}
	case info&types.IsFloat != 0:
		return node{"kind": "number"}
	}
	return unsupported(b.Name())
}

func (r *reader) qualifier(p *types.Package) string {
	if p == r.pkg.Types {
		return ""
	}
	return p.Name()
}

// named is a named type's tree: iter.Seq and iter.Seq2 as iterables; a struct as a named object
// type; a string type with constants of its own as an enum of them; any other as what it is.
func (r *reader) named(n *types.Named) node {
	args := n.TypeArgs()
	// An iterator function: iter.Seq yields rows, iter.Seq2 a row and an error with each.
	if isNamed(n, "iter", "Seq") {
		return node{"kind": "iterable", "async": false, "generator": true, "errors": false, "element": r.tree(args.At(0))}
	}
	if isNamed(n, "iter", "Seq2") {
		element := r.tree(args.At(0))
		if !isError(args.At(1)) {
			element = unsupported(types.TypeString(n, r.qualifier) + " (its second value is not an error)")
		}
		return node{"kind": "iterable", "async": false, "generator": true, "errors": true, "element": element}
	}
	// REQ-2226: a parameter taking a governed table: a struct the code declares as
	// TableRef[Row], Row the columns it declares. A Go source has no Provisa module to import.
	if n.Obj().Name() == "TableRef" && args.Len() == 1 {
		return node{"kind": "table_ref", "row": r.tree(args.At(0))}
	}
	if args.Len() > 0 {
		return unsupported(types.TypeString(n, r.qualifier))
	}
	switch u := n.Underlying().(type) {
	case *types.Struct:
		name := n.Obj().Name()
		path := ""
		if n.Obj().Pkg() != nil {
			path = n.Obj().Pkg().Path()
		}
		if owner, seen := r.owners[name]; seen && owner != path {
			r.diagnostics = append(r.diagnostics, fmt.Sprintf(
				"two types are named %s, in %s and in %s; a named type's name is its own", name, owner, path))
			return unsupported(types.TypeString(n, r.qualifier))
		}
		r.owners[name] = path
		if _, done := r.types[name]; !done && !r.reading[name] {
			r.reading[name] = true
			r.types[name] = objectType{Name: name, Fields: r.fields(u)}
			delete(r.reading, name)
		}
		return node{"kind": "ref", "name": name}
	case *types.Basic:
		if u.Info()&types.IsString != 0 {
			if values := r.constants(n); len(values) > 0 {
				members := make([]node, len(values))
				for i, v := range values {
					members[i] = node{"kind": "literal", "value": v}
				}
				return node{"kind": "union", "name": n.Obj().Name(), "members": members}
			}
		}
	}
	return r.tree(n.Underlying())
}

// constants are the string constants n's package declares of type n, in the order declared.
func (r *reader) constants(n *types.Named) []string {
	pkg := n.Obj().Pkg()
	if pkg == nil {
		return nil
	}
	type constant struct {
		pos   int
		value string
	}
	var found []constant
	scope := pkg.Scope()
	for _, name := range scope.Names() {
		c, ok := scope.Lookup(name).(*types.Const)
		if !ok || !types.Identical(c.Type(), n) {
			continue
		}
		var value string
		if err := json.Unmarshal([]byte(c.Val().ExactString()), &value); err != nil {
			continue
		}
		found = append(found, constant{int(c.Pos()), value})
	}
	sort.SliceStable(found, func(i, j int) bool { return found[i].pos < found[j].pos })
	values := make([]string, len(found))
	for i, c := range found {
		values[i] = c.value
	}
	return values
}

// fields are a struct's fields as encoding/json names them: the json tag's name, else the
// field's own; a field tagged "-" and an unexported field are not one; an embedded struct's
// fields are the struct's own. A pointer field is optional: Go has no default, and nil is a
// value left out.
func (r *reader) fields(s *types.Struct) []field {
	out := []field{}
	for i := 0; i < s.NumFields(); i++ {
		v := s.Field(i)
		tag := reflect.StructTag(s.Tag(i)).Get("json")
		name, _, _ := strings.Cut(tag, ",")
		if name == "-" && tag == "-" {
			continue
		}
		if v.Embedded() && name == "" {
			embedded := types.Unalias(v.Type())
			if p, ok := embedded.(*types.Pointer); ok {
				embedded = types.Unalias(p.Elem())
			}
			if st, ok := embedded.Underlying().(*types.Struct); ok {
				out = append(out, r.fields(st)...)
				continue
			}
		}
		if !v.Exported() {
			continue
		}
		if name == "" {
			name = v.Name()
		}
		_, pointer := types.Unalias(v.Type()).(*types.Pointer)
		out = append(out, field{
			Name:        name,
			Optional:    pointer,
			Type:        r.tree(v.Type()),
			Description: r.fieldDocs[r.pkg.Fset.Position(v.Pos()).String()],
		})
	}
	return out
}

func (r *reader) function(decl *ast.FuncDecl, fn *types.Func) export {
	tags, tagArguments, text := []string{}, []tagArg{}, []string{}
	for _, line := range strings.Split(decl.Doc.Text(), "\n") {
		trimmed := strings.TrimSpace(line)
		if m := tagLine.FindStringSubmatch(trimmed); m != nil {
			tags = append(tags, m[1])
		} else if m := tagArgument.FindStringSubmatch(trimmed); m != nil {
			tagArguments = append(tagArguments, tagArg{m[1], m[2]})
		} else {
			text = append(text, line)
		}
	}
	sig := fn.Type().(*types.Signature)
	params := sig.Params()
	out := signature{Parameters: []parameter{}}
	for i := 0; i < params.Len(); i++ {
		p := params.At(i)
		if i == 0 && isNamed(p.Type(), "context", "Context") {
			out.Context = true
			continue
		}
		variadic := sig.Variadic() && i == params.Len()-1
		out.Parameters = append(out.Parameters, parameter{
			Name: p.Name(), Optional: false, Rest: variadic, Type: r.tree(p.Type()),
		})
	}
	results := sig.Results()
	count := results.Len()
	if count > 0 && isError(results.At(count-1).Type()) {
		out.Fails = true
		count--
	}
	switch count {
	case 0:
		out.Returns = node{"kind": "undefined"}
	case 1:
		out.Returns = r.tree(results.At(0).Type())
	default:
		out.Returns = unsupported(fmt.Sprintf("%d results (a function returns one, and an error)", count))
	}
	if sig.TypeParams().Len() > 0 {
		out.Returns = unsupported("a generic function")
	}
	return export{
		Name:         fn.Name(),
		Tags:         tags,
		TagArguments: tagArguments,
		Description:  strings.TrimSpace(strings.Join(text, "\n")),
		Signatures:   []signature{out},
	}
}

func read(dir, pattern string) document {
	doc := document{Exports: []export{}, Types: map[string]objectType{}, Diagnostics: []string{}}
	config := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes |
			packages.NeedTypesInfo | packages.NeedImports | packages.NeedDeps,
		Dir:   dir,
		Tests: false,
	}
	pkgs, err := packages.Load(config, pattern)
	if err != nil {
		doc.Diagnostics = append(doc.Diagnostics, fmt.Sprintf("loading %s failed: %v", pattern, err))
		return doc
	}
	if len(pkgs) != 1 {
		names := make([]string, len(pkgs))
		for i, p := range pkgs {
			names[i] = p.PkgPath
		}
		doc.Diagnostics = append(doc.Diagnostics, fmt.Sprintf(
			"%s names %d packages (%s); a source is one package", pattern, len(pkgs), strings.Join(names, ", ")))
		return doc
	}
	pkg := pkgs[0]
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		for _, e := range p.Errors {
			doc.Diagnostics = append(doc.Diagnostics, e.Error())
		}
	})
	if len(doc.Diagnostics) > 0 || pkg.Types == nil {
		return doc
	}
	r := &reader{
		pkg: pkg, types: doc.Types, reading: map[string]bool{}, owners: map[string]string{},
		fieldDocs: map[string]string{},
	}
	for _, file := range pkg.Syntax {
		ast.Inspect(file, func(n ast.Node) bool {
			f, ok := n.(*ast.Field)
			if !ok {
				return true
			}
			text := strings.TrimSpace(f.Doc.Text())
			if text == "" {
				text = strings.TrimSpace(f.Comment.Text())
			}
			for _, name := range f.Names {
				r.fieldDocs[pkg.Fset.Position(name.Pos()).String()] = text
			}
			return true
		})
	}
	for _, file := range pkg.Syntax {
		for _, d := range file.Decls {
			decl, ok := d.(*ast.FuncDecl)
			if !ok || decl.Recv != nil || !decl.Name.IsExported() {
				continue
			}
			fn, ok := pkg.TypesInfo.Defs[decl.Name].(*types.Func)
			if !ok {
				continue
			}
			doc.Exports = append(doc.Exports, r.function(decl, fn))
		}
	}
	doc.Diagnostics = append(doc.Diagnostics, r.diagnostics...)
	return doc
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: reader <module directory> <package pattern>")
		os.Exit(2)
	}
	if err := json.NewEncoder(os.Stdout).Encode(read(os.Args[1], os.Args[2])); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
