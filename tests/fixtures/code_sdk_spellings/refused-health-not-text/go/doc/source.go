// Copyright (c) 2026 Kenneth Stott
// Canary: f3dbc11d-2797-4797-a458-ca9ec79534a0
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

// @health
func Status() int { return 1 }
