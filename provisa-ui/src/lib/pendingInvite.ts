// Copyright (c) 2026 Kenneth Stott
// Canary: 4e1d90b7-5c2a-41f8-9b6e-2c7d0a3f8151
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1616: the invitation an arriving visitor is holding, kept until it is redeemed.
 *
 * Redemption needs a bearer, so it cannot happen until the sign-in returns -- and between the
 * click on the link and that return the token lives in nothing but the address bar. An org
 * subdomain bounces the visitor to the control plane (REQ-1348), the control plane navigates away
 * from `/login` the moment a token exists, and a sign-in finished in a second tab never sees the
 * first tab's query at all. Each of those loses the token while the account it was meant to seat
 * is already created, and the visitor lands as a member of nothing: every request 401s with "Org
 * selection required" and the link they followed cannot be followed again to fix it.
 *
 * So the token is written down when it is first seen and read back where the redemption actually
 * happens. It is deliberately NOT in `SESSION_KEYS` (lib/session): clearing it at sign-in would
 * erase it in the one step it has to survive, and it is not the previous identity's state -- an
 * invitation link is addressed to whoever follows it, so the identity that comes back from the
 * provider is its redeemer.
 */
const PENDING_INVITE_KEY = "provisa_pending_invite";

/** Write down an invitation token seen in the address bar, before anything navigates. */
export function rememberInvite(token: string): void {
  localStorage.setItem(PENDING_INVITE_KEY, token);
}

/** The invitation still owed a redemption, or null. */
export function pendingInvite(): string | null {
  return localStorage.getItem(PENDING_INVITE_KEY);
}

/** Drop the invitation: it was redeemed, or the server refused it and a retry would refuse it too. */
export function forgetInvite(): void {
  localStorage.removeItem(PENDING_INVITE_KEY);
}
