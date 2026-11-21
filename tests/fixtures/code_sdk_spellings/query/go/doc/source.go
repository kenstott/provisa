// Copyright (c) 2026 Kenneth Stott
// Canary: f1bb51ae-0874-4d36-8216-f3fed0bcd994
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

// Lookup is the rows of one name.
//
// @query
func Lookup(name string) []Row { return nil }
