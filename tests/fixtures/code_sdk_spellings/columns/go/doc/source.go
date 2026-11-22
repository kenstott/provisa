// Copyright (c) 2026 Kenneth Stott
// Canary: 1b0ff7c2-26c5-49cb-9bf0-95290529d4a6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

package source

import "strings"

type Row struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

type Upper struct {
	Upper string `json:"upper"`
}

// Shout is each name in capitals.
//
// @columns
func Shout(names []string) []Upper {
	out := make([]Upper, len(names))
	for i, n := range names {
		out[i] = Upper{Upper: strings.ToUpper(n)}
	}
	return out
}
