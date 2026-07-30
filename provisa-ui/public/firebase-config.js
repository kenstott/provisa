// Copyright (c) 2026 Kenneth Stott
// Canary: 5673b574-dbf4-4300-8d1f-3e2e0c0b5180
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Local-dev stand-in for the runtime-injected Firebase web config (REQ-1266).
// In a cloud deploy ui_server.py's explicit /firebase-config.js route shadows this
// static file (Starlette matches routes before mounts) and injects the node's env.
// null is the configured-off state: lib/firebase.ts then falls back to build-time
// VITE_FIREBASE_* env, and never imports Firebase at all under basic/none auth.
window.__PROVISA_FIREBASE__ = null;
