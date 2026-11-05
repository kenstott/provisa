// Copyright (c) 2026 Kenneth Stott
// Canary: 6b9e2f40-8d17-4c3a-b5e6-2a0f7c1d9e85
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

package main

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"
)

// JavaScript's exact integers: a JSON number outside them travels as {"$bigint": digits}.
const exact = 1<<53 - 1

var timeType = reflect.TypeOf(time.Time{})

// absent is whether a wire argument is one the read does not give ({"$absent": true}).
func absent(value any) bool {
	m, ok := value.(map[string]any)
	return ok && len(m) == 1 && m["$absent"] == true
}

// fromWire is a wire value as encoding/json reads it into the code's types: a timestamp's or a
// day's ISO text as a string, a big integer's or a decimal's digits as a number.
func fromWire(value any) any {
	switch v := value.(type) {
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = fromWire(item)
		}
		return out
	case map[string]any:
		if len(v) == 1 {
			for key, inner := range v {
				switch key {
				case "$date", "$day":
					return inner
				case "$bigint", "$decimal":
					return json.Number(fmt.Sprint(inner))
				}
			}
		}
		out := make(map[string]any, len(v))
		for key, inner := range v {
			out[key] = fromWire(inner)
		}
		return out
	}
	return value
}

// field is one of a struct's fields as encoding/json names it (the reader's rule).
type field struct {
	name  string
	index []int
}

// fields are a struct type's fields as encoding/json names them: the json tag's name, else the
// field's own; a field tagged "-" and an unexported field are not one; an embedded struct's
// fields are the struct's own.
func fields(kind reflect.Type) []field {
	var out []field
	for i := 0; i < kind.NumField(); i++ {
		f := kind.Field(i)
		tag := f.Tag.Get("json")
		name, _, _ := strings.Cut(tag, ",")
		if tag == "-" {
			continue
		}
		if f.Anonymous && name == "" {
			embedded := f.Type
			if embedded.Kind() == reflect.Pointer {
				embedded = embedded.Elem()
			}
			if embedded.Kind() == reflect.Struct {
				for _, inner := range fields(embedded) {
					out = append(out, field{inner.name, append([]int{i}, inner.index...)})
				}
				continue
			}
		}
		if !f.IsExported() {
			continue
		}
		if name == "" {
			name = f.Name
		}
		out = append(out, field{name, []int{i}})
	}
	return out
}

// plain is a value the code made as plain values: a struct as a map of its fields by their
// json names, a map as a map, a slice as a list, a number as an int64, a uint64 or a float64,
// a time as a time.Time, nil as nil.
func plain(v reflect.Value) (any, error) {
	if !v.IsValid() {
		return nil, nil
	}
	if v.Type() == timeType {
		return v.Interface().(time.Time), nil
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return nil, nil
		}
		return plain(v.Elem())
	case reflect.Struct:
		out := map[string]any{}
		for _, f := range fields(v.Type()) {
			inner, err := v.FieldByIndexErr(f.index)
			if err != nil {
				out[f.name] = nil // through an embedded pointer that is nil
				continue
			}
			if out[f.name], err = plain(inner); err != nil {
				return nil, err
			}
		}
		return out, nil
	case reflect.Map:
		if v.IsNil() {
			return nil, nil
		}
		out := make(map[string]any, v.Len())
		iter := v.MapRange()
		for iter.Next() {
			value, err := plain(iter.Value())
			if err != nil {
				return nil, err
			}
			out[fmt.Sprint(iter.Key().Interface())] = value
		}
		return out, nil
	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice && v.IsNil() {
			return nil, nil
		}
		out := make([]any, v.Len())
		for i := range out {
			value, err := plain(v.Index(i))
			if err != nil {
				return nil, err
			}
			out[i] = value
		}
		return out, nil
	case reflect.Bool:
		return v.Bool(), nil
	case reflect.String:
		return v.String(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint(), nil
	case reflect.Float32, reflect.Float64:
		return v.Float(), nil
	}
	return nil, fmt.Errorf("a %s has no form a row or a value can take", v.Type())
}

// toWire is a plain value as the wire's JSON carries it.
func toWire(value any) any {
	switch v := value.(type) {
	case time.Time:
		return map[string]any{"$date": v.Format(time.RFC3339Nano)}
	case int64:
		if v > exact || v < -exact {
			return map[string]any{"$bigint": fmt.Sprint(v)}
		}
		return v
	case uint64:
		if v > exact {
			return map[string]any{"$bigint": fmt.Sprint(v)}
		}
		return v
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil // JSON has no form for it
		}
		return v
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = toWire(item)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			out[key] = toWire(item)
		}
		return out
	}
	return value
}

func jsonText(value any) (string, error) {
	text, err := json.Marshal(toWire(value))
	return string(text), err
}
