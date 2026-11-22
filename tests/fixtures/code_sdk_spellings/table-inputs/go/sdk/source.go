// Copyright (c) 2026 Kenneth Stott
// Canary: 28383a7b-80cb-4c33-ba4d-5dcbbc5b9677
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

package source

import (
	"iter"

	provisa "provisa.dev/sdk/go"
)

type Row struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

type Inputs struct {
	Region *string `json:"region"`
}

// Rows is the rows of a region.
func Rows(in Inputs) iter.Seq[Row] { return nil }

var _ = provisa.Table[Inputs, Row](Rows)
