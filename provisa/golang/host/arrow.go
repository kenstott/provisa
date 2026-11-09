// Copyright (c) 2026 Kenneth Stott
// Canary: d4170b6e-2c58-4a93-9f0e-8e3b5a6c1f27
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

package main

import (
	"bytes"
	"fmt"
	"math"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// The field metadata key naming the IR type a text field carries (arrow_rows.IR_KEY).
const irKey = "provisa.ir"

// readSchema is the schema a call gives: an Arrow IPC stream of it with no batches.
func readSchema(raw []byte) (*arrow.Schema, error) {
	reader, err := ipc.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	defer reader.Release()
	return reader.Schema(), nil
}

// arrowRows is plain rows as one Arrow IPC stream against schema.
func arrowRows(schema *arrow.Schema, rows []any) ([]byte, error) {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	for _, row := range rows {
		values, ok := row.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("a %T is not a row", row)
		}
		for i, f := range schema.Fields() {
			if err := appendValue(builder.Field(i), f, values[f.Name]); err != nil {
				return nil, err
			}
		}
	}
	record := builder.NewRecordBatch()
	defer record.Release()
	var out bytes.Buffer
	writer := ipc.NewWriter(&out, ipc.WithSchema(schema))
	if err := writer.Write(record); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func irOf(f arrow.Field) string {
	if at := f.Metadata.FindKey(irKey); at >= 0 {
		return f.Metadata.Values()[at]
	}
	return ""
}

func refuse(f arrow.Field, value any) error {
	return fmt.Errorf("field %s: a %T is not a %s", f.Name, value, f.Type)
}

// appendValue appends one plain value in the form f takes it: a json or numeric field as text,
// a struct's fields by name, a list's items in order.
func appendValue(b array.Builder, f arrow.Field, value any) error {
	if value == nil {
		b.AppendNull()
		return nil
	}
	switch irOf(f) {
	case "json":
		text, err := jsonText(value)
		if err != nil {
			return err
		}
		b.(*array.StringBuilder).Append(text)
		return nil
	case "numeric":
		b.(*array.StringBuilder).Append(fmt.Sprint(value))
		return nil
	}
	switch b := b.(type) {
	case *array.StringBuilder:
		text, ok := value.(string)
		if !ok {
			return refuse(f, value)
		}
		b.Append(text)
	case *array.BooleanBuilder:
		flag, ok := value.(bool)
		if !ok {
			return refuse(f, value)
		}
		b.Append(flag)
	case *array.Int64Builder:
		switch n := value.(type) {
		case int64:
			b.Append(n)
		case uint64:
			if n > math.MaxInt64 {
				return fmt.Errorf("field %s: %d is beyond a bigint", f.Name, n)
			}
			b.Append(int64(n))
		default:
			return refuse(f, value)
		}
	case *array.Float64Builder:
		switch n := value.(type) {
		case float64:
			b.Append(n)
		case int64:
			b.Append(float64(n))
		case uint64:
			b.Append(float64(n))
		default:
			return refuse(f, value)
		}
	case *array.TimestampBuilder:
		t, ok := value.(time.Time)
		if !ok {
			return refuse(f, value)
		}
		stamp, err := arrow.TimestampFromTime(t, f.Type.(*arrow.TimestampType).Unit)
		if err != nil {
			return fmt.Errorf("field %s: %w", f.Name, err)
		}
		b.Append(stamp)
	case *array.Date32Builder:
		t, ok := value.(time.Time)
		if !ok {
			return refuse(f, value)
		}
		b.Append(arrow.Date32FromTime(t))
	case *array.ListBuilder:
		items, ok := value.([]any)
		if !ok {
			return refuse(f, value)
		}
		b.Append(true)
		element := f.Type.(*arrow.ListType).ElemField()
		for _, item := range items {
			if err := appendValue(b.ValueBuilder(), element, item); err != nil {
				return err
			}
		}
	case *array.StructBuilder:
		values, ok := value.(map[string]any)
		if !ok {
			return refuse(f, value)
		}
		b.Append(true)
		kind := f.Type.(*arrow.StructType)
		for i := 0; i < kind.NumFields(); i++ {
			if err := appendValue(b.FieldBuilder(i), kind.Field(i), values[kind.Field(i).Name]); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("field %s: the Arrow type %s has no form here", f.Name, f.Type)
	}
	return nil
}
