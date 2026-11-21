// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// elkjs as render.test.mjs's failing build bundles it: every layout's answer is a failure, as
// elkjs's worker answers one (a plain object carrying the message).
export default class ELK {
  layout() {
    return Promise.reject({ message: "UnsupportedGraphException: held to fail by the test" });
  }
}
