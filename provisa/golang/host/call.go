// Copyright (c) 2026 Kenneth Stott
// Canary: a26c4e81-07d3-4b95-8e1f-5d9c3a7b2e64
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"sort"
	"syscall"

	"github.com/apache/arrow-go/v18/arrow"
)

// A table's rows go in batches of this many.
const batch = 1000

var (
	registry    = map[string]any{}
	contextType = reflect.TypeOf((*context.Context)(nil)).Elem()
	errorType   = reflect.TypeOf((*error)(nil)).Elem()
)

// register is called by the generated exports.go with the code's tagged functions, by name.
func register(exports map[string]any) {
	for name, fn := range exports {
		registry[name] = fn
	}
}

func exportNames() []string {
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// argumentSet is one set of a call's arguments: their values, as the wire gives them, and their
// parameters' names.
type argumentSet struct {
	args  []any
	names []string
}

type call struct {
	conn    *connection
	id      uint64
	export  string
	kind    string
	sets    []argumentSet
	schema  *arrow.Schema
	takes   []string
	keyed   bool
	failure error
	credits chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc
}

func texts(value any) []string {
	items, _ := value.([]any)
	out := make([]string, len(items))
	for i, item := range items {
		out[i], _ = item.(string)
	}
	return out
}

func newCall(conn *connection, message map[string]any) *call {
	ctx, cancel := context.WithCancel(context.Background())
	c := &call{
		conn:    conn,
		id:      uint64(number(message["id"])),
		export:  fmt.Sprint(message["export"]),
		kind:    fmt.Sprint(message["kind"]),
		keyed:   message["keyed"] == true,
		credits: make(chan struct{}, 1<<16),
		ctx:     ctx,
		cancel:  cancel,
	}
	if sets, ok := message["calls"].([]any); ok {
		// REQ-2099: a table called once per argument set, in order, under the one call id.
		for _, set := range sets {
			fields, _ := set.(map[string]any)
			args, _ := fields["args"].([]any)
			c.sets = append(c.sets, argumentSet{args, texts(fields["names"])})
		}
	} else {
		args, _ := message["args"].([]any)
		c.sets = []argumentSet{{args, texts(message["names"])}}
	}
	if takes, ok := message["takes"].([]any); ok {
		c.takes = texts(takes)
	}
	for range number(message["window"]) {
		c.credits <- struct{}{}
	}
	if text, ok := message["schema"].(string); ok {
		raw, err := base64.StdEncoding.DecodeString(text)
		if err == nil {
			c.schema, err = readSchema(raw)
		}
		c.failure = err
	}
	return c
}

func (c *call) credit(n int) {
	for range n {
		select {
		case c.credits <- struct{}{}:
		default:
		}
	}
}

// send sends rows as one Arrow frame, once the call has a credit for it: the function is pulled
// no further than its reader's window ahead.
func (c *call) send(rows []any) error {
	ipc, err := arrowRows(c.schema, rows)
	if err != nil {
		return err
	}
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	case <-c.credits:
	}
	return c.conn.sendRows(c.id, ipc)
}

func cpu() float64 {
	var usage syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &usage) != nil {
		return 0
	}
	return float64(usage.Utime.Nano()+usage.Stime.Nano()) / 1e6
}

func (c *call) run() {
	started := cpu()
	defer c.conn.calls.Delete(c.id)
	defer c.cancel()
	err := c.serve()
	if c.ctx.Err() != nil {
		return // Provisa stopped the call and is no longer waiting for it
	}
	if err != nil {
		var failed *codeError
		message, stack := err.Error(), ""
		if errors.As(err, &failed) {
			stack = failed.stack
		}
		c.conn.send(map[string]any{"op": "error", "id": c.id, "message": message, "stack": stack})
		return
	}
	// REQ-2106: the CPU the call took in this process (every goroutine of it).
	c.conn.send(map[string]any{"op": "end", "id": c.id, "cpu_ms": cpu() - started})
}

// codeError is the code's own failure: an error it returned or yielded, or a panic, with where.
type codeError struct {
	message string
	stack   string
}

func (e *codeError) Error() string { return e.message }

func (c *call) serve() (err error) {
	defer func() {
		if panicked := recover(); panicked != nil {
			err = &codeError{fmt.Sprintf("panic: %v", panicked), string(debug.Stack())}
		}
	}()
	if c.failure != nil {
		return fmt.Errorf("the call's schema cannot be read: %w", c.failure)
	}
	fn, ok := registry[c.export]
	if !ok {
		return fmt.Errorf("the code has no function %s", c.export)
	}
	if c.keyed {
		return fmt.Errorf("%s: a Go source has no @loader", c.export)
	}
	for _, set := range c.sets {
		result, err := c.invoke(reflect.ValueOf(fn), set)
		if err != nil {
			return err
		}
		if err := c.answer(result); err != nil {
			return err
		}
	}
	return nil
}

// invoke calls fn with the call's context (when it takes one first), then its inputs struct (a
// table's or a stream's, made of the arguments by name, an argument left out leaving its field
// nil) or its own arguments in order; its result, or the error it returned.
func (c *call) invoke(fn reflect.Value, set argumentSet) (reflect.Value, error) {
	kind := fn.Type()
	var in []reflect.Value
	next := 0
	if kind.NumIn() > 0 && kind.In(0) == contextType {
		in = append(in, reflect.ValueOf(c.ctx))
		next = 1
	}
	if c.takes != nil {
		if len(c.takes) > 0 && c.takes[0] == "inputs" {
			if next >= kind.NumIn() {
				return reflect.Value{}, fmt.Errorf("%s takes no inputs", c.export)
			}
			given := map[string]any{}
			for i, name := range set.names {
				if !absent(set.args[i]) {
					given[name] = fromWire(set.args[i])
				}
			}
			value, err := decodeInto(given, kind.In(next))
			if err != nil {
				return reflect.Value{}, fmt.Errorf("%s: its inputs: %w", c.export, err)
			}
			in = append(in, value)
		}
	} else {
		if len(set.args) != kind.NumIn()-next {
			return reflect.Value{}, fmt.Errorf(
				"%s takes %d arguments, and the call gives %d", c.export, kind.NumIn()-next, len(set.args))
		}
		for i, arg := range set.args {
			parameter := kind.In(next + i)
			if absent(arg) {
				in = append(in, reflect.Zero(parameter))
				continue
			}
			value, err := decodeInto(fromWire(arg), parameter)
			if err != nil {
				return reflect.Value{}, fmt.Errorf("%s: argument %d: %w", c.export, i+1, err)
			}
			in = append(in, value)
		}
	}
	out := fn.Call(in)
	if n := len(out); n > 0 && kind.Out(n-1) == errorType {
		if failed := out[n-1]; !failed.IsNil() {
			return reflect.Value{}, &codeError{failed.Interface().(error).Error(), ""}
		}
		out = out[:n-1]
	}
	if len(out) == 0 {
		return reflect.Value{}, nil
	}
	return out[0], nil
}

func decodeInto(value any, kind reflect.Type) (reflect.Value, error) {
	text, err := json.Marshal(value)
	if err != nil {
		return reflect.Value{}, err
	}
	target := reflect.New(kind)
	if err := json.Unmarshal(text, target.Interface()); err != nil {
		return reflect.Value{}, err
	}
	return target.Elem(), nil
}

// answer sends what the function gave: a single value as its value message, or rows -- a slice,
// or an iterator function (iter.Seq, iter.Seq2 with an error) -- as Arrow frames.
func (c *call) answer(result reflect.Value) error {
	if c.schema == nil {
		var value any
		if result.IsValid() {
			plainValue, err := plain(result)
			if err != nil {
				return err
			}
			value = toWire(plainValue)
		}
		return c.conn.send(map[string]any{"op": "value", "id": c.id, "value": value})
	}
	if !result.IsValid() {
		return fmt.Errorf("%s returned no rows", c.export)
	}
	switch result.Kind() {
	case reflect.Slice, reflect.Array:
		for at := 0; at < result.Len(); at += batch {
			rows := make([]any, 0, batch)
			for i := at; i < min(at+batch, result.Len()); i++ {
				row, err := plain(result.Index(i))
				if err != nil {
					return err
				}
				rows = append(rows, row)
			}
			if err := c.send(rows); err != nil {
				return err
			}
		}
		return nil
	case reflect.Func:
		return c.iterate(result)
	}
	return fmt.Errorf("a %s is not a collection of rows", result.Type())
}

// iterate ranges over an iterator function: a stream's rows are sent each as it is yielded, a
// table's in batches. A yielded error ends the call with it; a cancel stops the range.
func (c *call) iterate(seq reflect.Value) error {
	if seq.IsNil() {
		return nil
	}
	yield := seq.Type().In(0)
	var rows []any
	var failed error
	step := func(args []reflect.Value) []reflect.Value {
		stop := []reflect.Value{reflect.ValueOf(false)}
		if len(args) == 2 && !args[1].IsNil() {
			failed = &codeError{args[1].Interface().(error).Error(), ""}
			return stop
		}
		row, err := plain(args[0])
		if err != nil {
			failed = err
			return stop
		}
		rows = append(rows, row)
		if c.kind == "stream" || len(rows) >= batch {
			failed = c.send(rows)
			rows = nil
		}
		if failed != nil || c.ctx.Err() != nil {
			return stop
		}
		return []reflect.Value{reflect.ValueOf(true)}
	}
	seq.Call([]reflect.Value{reflect.MakeFunc(yield, step)})
	if failed != nil {
		return failed
	}
	if len(rows) > 0 {
		return c.send(rows)
	}
	return c.ctx.Err()
}
