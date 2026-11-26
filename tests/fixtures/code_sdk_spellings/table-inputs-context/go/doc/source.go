// Copyright (c) 2026 Kenneth Stott
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

	ps "provisa_services"
)

type Row struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

type Inputs struct {
	Region *string `json:"region"`
}

// Rows of a region, as the read asks.
//
// @table
func Rows(in Inputs, read ps.ReadContext) iter.Seq[Row] { return nil }
