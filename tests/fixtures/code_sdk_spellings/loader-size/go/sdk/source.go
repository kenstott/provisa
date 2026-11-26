// Copyright (c) 2026 Kenneth Stott
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

// Rows of the keys, 100 at a time.
func Rows(read provisa.ReadContext) (map[string][]Row, error) { return nil, nil }

var _ = provisa.Table[provisa.NoInputs, Row](Rows, provisa.Loader(100))
