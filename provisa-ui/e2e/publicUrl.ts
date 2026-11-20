// Copyright (c) 2026 Kenneth Stott
// Canary: b75d443d-4b71-493a-80c2-3f35b6189fd4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135: every e2e backend's public address (server.public_url), where its SPARQL vocabulary's
// and its subjects' IRIs start. Unset, the vocabulary every relationship surface and the SPARQL tab
// read is refused (400). playwright.config.ts's webServer backends and the engine-swap reboot
// harness (engine-swap-helpers.ts spawnRebootBackend) boot with this one value.
export const E2E_PUBLIC_URL = "https://e2e.provisa.example";
