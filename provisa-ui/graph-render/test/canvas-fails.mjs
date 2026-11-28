// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// node-canvas as render.test.mjs's failing build bundles it: loading it fails as its native addon
// does when the host lacks one of the addon's shared libraries.
throw new Error(
  "libheld.so.1: cannot open shared object file: No such file or directory (held to fail by the test)",
);
