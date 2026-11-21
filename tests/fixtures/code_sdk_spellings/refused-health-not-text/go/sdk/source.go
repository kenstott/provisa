// Copyright (c) 2026 Kenneth Stott
// Canary: 1a00a113-4298-43af-8cd9-6ad341c36242
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

func Status() int { return 1 }

var _ = provisa.Health(Status)
