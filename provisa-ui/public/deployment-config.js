// Copyright (c) 2026 Kenneth Stott
// Canary: 9f3b6d21-4c7e-4a85-b0d2-5e18c6a7f340
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Local-dev stand-in for the runtime-injected base domain (REQ-1276), as firebase-config.js is for
// the Firebase config. In a deploy ui_server.py's explicit /deployment-config.js route shadows this
// static file and injects the node's VITE_ORG_BASE_DOMAIN. null declares no base domain:
// lib/authHost.ts then falls back to a build-time VITE_ORG_BASE_DOMAIN, and without one the host
// names no org and the UI runs as the deployment's one address.
window.__PROVISA_ORG_BASE_DOMAIN__ = null;
