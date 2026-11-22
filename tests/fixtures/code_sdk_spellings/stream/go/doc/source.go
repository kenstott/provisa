// Copyright (c) 2026 Kenneth Stott
// Canary: 21515204-07df-4ed1-9c92-4c545cbd8a73
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

package source

import (
	"context"
	"iter"
)

type Row struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

type Inputs struct {
	Region *string `json:"region"`
}

// Ticks is each row as it arrives.
//
// @stream
func Ticks(ctx context.Context, in Inputs) iter.Seq2[Row, error] { return nil }
