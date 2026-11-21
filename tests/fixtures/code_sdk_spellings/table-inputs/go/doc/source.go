// Copyright (c) 2026 Kenneth Stott
// Canary: f9bbd1e5-9a27-4326-af37-84c1b8371b95
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

package source

import "iter"

type Row struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

type Inputs struct {
	Region *string `json:"region"`
}

// Rows is the rows of a region.
//
// @table
func Rows(in Inputs) iter.Seq[Row] { return nil }
