// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1472: the break-glass session's bearer, and deliberately NOT `provisa_token`.
 *
 * `provisa_token` has an owner on a Firebase deployment: installFirebaseTokenSync (lib/firebase)
 * mirrors the SDK's token into it and removes it whenever `onIdTokenChanged` reports no user —
 * which is exactly what happens at boot when nobody is signed in through Firebase. An operator
 * session stored there was deleted before the first request left the page. Two credentials with
 * two owners get two keys.
 */
export const SUPERUSER_TOKEN_KEY = "provisa_su_token";

/**
 * The bearer this browser holds, whichever sign-in produced it.
 *
 * Every "am I signed in?" read goes through here rather than naming a key, so an operator session
 * is a session everywhere the app asks — the routing gate, the onboarding gate, the login page's
 * already-signed-in redirect — and not only on the requests authFetch decorates.
 *
 * This module imports nothing: lib/session reaches the Apollo cache, and the bearer is read from
 * lib/authFetch, which apolloClient itself imports.
 */
export function storedToken(): string | null {
  return localStorage.getItem(SUPERUSER_TOKEN_KEY) ?? localStorage.getItem("provisa_token");
}
