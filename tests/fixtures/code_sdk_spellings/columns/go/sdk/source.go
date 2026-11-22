// Copyright (c) 2026 Kenneth Stott
// Canary: dc695787-4891-47a3-bf65-bb79c5aec995
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

package source

import (
	"strings"

	provisa "provisa.dev/sdk/go"
)

type Row struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

type Upper struct {
	Upper string `json:"upper"`
}

// Shout is each name in capitals.
func Shout(names []string) []Upper {
	out := make([]Upper, len(names))
	for i, n := range names {
		out[i] = Upper{Upper: strings.ToUpper(n)}
	}
	return out
}

var _ = provisa.Columns(Shout)
