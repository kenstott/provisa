// Copyright (c) 2026 Kenneth Stott
// Canary: 7cbd41c1-634b-444c-99ed-1cc6749f3963
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

package source

type Row struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// TableRef is a governed table the code takes; Provisa's reader knows it by its name.
type TableRef[Row any] struct{}

type Match struct {
	LeftID  int64 `json:"left_id"`
	RightID int64 `json:"right_id"`
}

// Pairs is the rows of two tables that belong together.
//
// @query
func Pairs(left TableRef[Row], right TableRef[Row]) ([]Match, error) { return nil, nil }
