// Copyright (c) 2026 Kenneth Stott
// Canary: 48560e40-be3a-4884-9047-77fd9f4f42ec
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

package source

import provisa "provisa.dev/sdk/go"

type Row struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// Lookup is the rows of one name.
func Lookup(name string) []Row { return nil }

var _ = provisa.Query(Lookup)
