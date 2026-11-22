// Copyright (c) 2026 Kenneth Stott
// Canary: d7242804-10ca-48f3-ac02-68c51d9a16e4
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

func Rows() []map[string]any { return nil }

var _ = provisa.Table[provisa.NoInputs, map[string]any](Rows)
