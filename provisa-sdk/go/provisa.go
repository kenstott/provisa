// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Package provisa is Provisa's optional SDK for Go code sources (REQ-1984).
//
// Each registration is a doc-comment tag, spelled so the compiler holds the function to its
// tag's rules. A package-level registration
//
//	var _ = provisa.Table[RiskInputs, Risk](ChurnRisk, provisa.Requires("API_KEY", provisa.Secret))
//
// is the same function to Provisa as
//
//	// ChurnRisk is the churn risk of one account.
//	//
//	// @table
//	// @requires API_KEY:secret
//	func ChurnRisk(ctx context.Context, in RiskInputs) ([]Risk, error)
//
// Provisa's reader finds each registration by this package's import path, whatever name the
// import is given, and adds its tags to the function it names. The function must be an exported,
// top-level function of the package, named by its identifier, and every option's arguments must
// be constants. The SDK does nothing at run time: each registration returns an empty [Decl].
//
// The compiler checks the shape of a [Table], [Stream], [OnStart], [OnStop] or [Health]
// function. I and R are written out, because Go infers no type argument through a union
// constraint: I is the function's inputs struct ([NoInputs] when it takes none) and R its row.
// Commands, queries, column transforms and change functions take any number of parameters, which
// a constraint cannot spell, so their shape is checked when Provisa reads the code, as it is for
// the doc-comment form.
package provisa

import (
	"context"
	"iter"
	"time"
)

// Spelling is the version of the registrations this SDK spells. A server refuses, by name, a
// registration it does not read.
const Spelling = 1

// Decl is what a registration returns: nothing. Assign it to the blank identifier.
type Decl struct{}

// NoInputs stands for I when the function takes no inputs struct.
type NoInputs struct{}

// TableRef is a governed table a command or query takes (REQ-2226), Row the columns it reads.
// Provisa's reader recognises any generic type named TableRef; this is one to import.
type TableRef[Row any] struct{}

// Column types, aliases of what the reader already maps.
type (
	// Text is a text column.
	Text = string
	// BigInt is a bigint column.
	BigInt = int64
	// Double is a double column.
	Double = float64
	// Boolean is a boolean column.
	Boolean = bool
	// Timestamp is a timestamp column.
	Timestamp = time.Time
	// Json is a JSON column: a single field of a typed row may be loose.
	Json = any
)

// Option is a tag a registration adds: [Requires], or a [Table]'s [Loader].
type Option interface{ option() }

// CommandOption is a tag a [Command] adds: [Requires], [Insert], [Update] or [Delete].
type CommandOption interface{ commandOption() }

// Secrecy says whether a [Requires] value is a secret.
type Secrecy bool

// Secret marks a [Requires] value as a secret: @requires NAME:secret.
const Secret Secrecy = true

// Requirement is the @requires tag (REQ-2221).
type Requirement struct{}

func (Requirement) option()        {}
func (Requirement) commandOption() {}

// Requires is @requires NAME, or @requires NAME:secret with [Secret].
func Requires(name string, secret ...Secrecy) Requirement { return Requirement{} }

// Write binds a command to a table's writes: @insert, @update or @delete.
type Write struct{}

func (Write) commandOption() {}

// Insert is @insert TABLE.
func Insert(table string) Write { return Write{} }

// Update is @update TABLE.
func Update(table string) Write { return Write{} }

// Delete is @delete TABLE.
func Delete(table string) Write { return Write{} }

// Filter is a condition a read puts on a return column (REQ-2100): Column Op Value. Op is one of
// eq, ne, gt, gte, lt, lte, in (Value a list) and like (a SQL pattern).
type Filter struct {
	Column string `json:"column"`
	Op     string `json:"op"`
	Value  any    `json:"value"`
}

// Order is a key a read orders its rows by (REQ-2100).
type Order struct {
	Column     string `json:"column"`
	Descending bool   `json:"descending"`
	NullsFirst bool   `json:"nulls_first"`
}

// ReadContext is what a read asks of a @table or @stream function's rows (REQ-2100), the same as
// provisa_services.ReadContext: Provisa gives either to a function that takes it after its
// inputs. Every part is a hint Provisa applies again; Limit is nil when every row counts; Keys
// are a [Loader]'s join keys, each key's hash to its inputs, nil for any other read.
type ReadContext struct {
	Filters []Filter                  `json:"filters"`
	OrderBy []Order                   `json:"order_by"`
	Limit   *int64                    `json:"limit"`
	Columns []string                  `json:"columns"`
	Keys    map[string]map[string]any `json:"keys"`
}

// Loading is the @loader tag a [Table] adds: [Loader].
type Loading struct{}

func (Loading) option() {}

// Loader is @loader (REQ-2100): the table is called with a join's keys in its [ReadContext], all
// in one batch, or with Loader(N) at most N in each (@loader N).
func Loader(batch ...int) Loading { return Loading{} }

// ReadFunc is a @table function: an optional context.Context first, then its inputs struct and
// its [ReadContext], each left out or not; it returns []R, ([]R, error), iter.Seq[R] or
// iter.Seq2[R, error]. A [Loader] takes the ReadContext and returns map[string][]R, each key's
// hash to its rows, with or without an error.
type ReadFunc[I, R any] interface {
	~func() []R | ~func() ([]R, error) | ~func() iter.Seq[R] | ~func() iter.Seq2[R, error] |
		~func(I) []R | ~func(I) ([]R, error) | ~func(I) iter.Seq[R] | ~func(I) iter.Seq2[R, error] |
		~func(context.Context) []R | ~func(context.Context) ([]R, error) |
		~func(context.Context) iter.Seq[R] | ~func(context.Context) iter.Seq2[R, error] |
		~func(context.Context, I) []R | ~func(context.Context, I) ([]R, error) |
		~func(context.Context, I) iter.Seq[R] | ~func(context.Context, I) iter.Seq2[R, error] |
		~func(ReadContext) []R | ~func(ReadContext) ([]R, error) | ~func(ReadContext) iter.Seq[R] |
		~func(ReadContext) iter.Seq2[R, error] | ~func(I, ReadContext) []R |
		~func(I, ReadContext) ([]R, error) | ~func(I, ReadContext) iter.Seq[R] |
		~func(I, ReadContext) iter.Seq2[R, error] | ~func(context.Context, ReadContext) []R |
		~func(context.Context, ReadContext) ([]R, error) |
		~func(context.Context, ReadContext) iter.Seq[R] |
		~func(context.Context, ReadContext) iter.Seq2[R, error] |
		~func(context.Context, I, ReadContext) []R |
		~func(context.Context, I, ReadContext) ([]R, error) |
		~func(context.Context, I, ReadContext) iter.Seq[R] |
		~func(context.Context, I, ReadContext) iter.Seq2[R, error] |
		~func(ReadContext) map[string][]R | ~func(ReadContext) (map[string][]R, error) |
		~func(I, ReadContext) map[string][]R | ~func(I, ReadContext) (map[string][]R, error) |
		~func(context.Context, ReadContext) map[string][]R |
		~func(context.Context, ReadContext) (map[string][]R, error) |
		~func(context.Context, I, ReadContext) map[string][]R |
		~func(context.Context, I, ReadContext) (map[string][]R, error)
}

// StreamFunc is a @stream function: the parameters of a [ReadFunc], returning iter.Seq2[R, error].
type StreamFunc[I, R any] interface {
	~func() iter.Seq2[R, error] | ~func(I) iter.Seq2[R, error] |
		~func(context.Context) iter.Seq2[R, error] | ~func(context.Context, I) iter.Seq2[R, error] |
		~func(ReadContext) iter.Seq2[R, error] | ~func(I, ReadContext) iter.Seq2[R, error] |
		~func(context.Context, ReadContext) iter.Seq2[R, error] |
		~func(context.Context, I, ReadContext) iter.Seq2[R, error]
}

// StartFunc is an @on-start function: it takes nothing, or the setup values as one map.
type StartFunc interface {
	~func() | ~func() error | ~func(context.Context) | ~func(context.Context) error |
		~func(map[string]any) | ~func(map[string]any) error |
		~func(context.Context, map[string]any) | ~func(context.Context, map[string]any) error |
		~func(map[string]string) | ~func(map[string]string) error |
		~func(context.Context, map[string]string) | ~func(context.Context, map[string]string) error
}

// StopFunc is an @on-stop function: it takes nothing.
type StopFunc interface {
	~func() | ~func() error | ~func(context.Context) | ~func(context.Context) error
}

// HealthFunc is a @health function: it takes nothing and returns its status as text.
type HealthFunc interface {
	~func() string | ~func() (string, error) |
		~func(context.Context) string | ~func(context.Context) (string, error)
}

// Table is @table.
func Table[I, R any, F ReadFunc[I, R]](fn F, opts ...Option) Decl { return Decl{} }

// Stream is @stream.
func Stream[I, R any, F StreamFunc[I, R]](fn F, opts ...Option) Decl { return Decl{} }

// Command is @command.
func Command[F any](fn F, opts ...CommandOption) Decl { return Decl{} }

// Query is @query: a command whose kind is a query (REQ-2226).
func Query[F any](fn F, opts ...Option) Decl { return Decl{} }

// Columns is @columns: a batch column transform (REQ-2131).
func Columns[F any](fn F, opts ...Option) Decl { return Decl{} }

// Change is @change (REQ-2249): one @change line each for table, key (the columns, in order),
// removed and cursor.
func Change[F any](fn F, table string, key []string, removed, cursor string, opts ...Option) Decl {
	return Decl{}
}

// OnStart is @on-start.
func OnStart[F StartFunc](fn F, opts ...Option) Decl { return Decl{} }

// OnStop is @on-stop.
func OnStop[F StopFunc](fn F) Decl { return Decl{} }

// Health is @health.
func Health[F HealthFunc](fn F) Decl { return Decl{} }
