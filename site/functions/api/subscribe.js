// Copyright (c) 2026 Kenneth Stott
// Canary: 3bc5f222-ba90-4a90-ad09-8f2f602f9395
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// POST /api/subscribe — step 1 of the double-opt-in release-email signup (REQ-2362). Stores
// {name, email} as a pending signup in D1 and emails a confirmation link; the address joins the
// Resend release list only when that link is clicked (functions/api/subscribe/confirm.js).
import { isValidEmail } from "../_lib/license.js";
import { savePendingSignup, sendConfirmEmail } from "../_lib/releases.js";

export async function onRequestPost(context) {
  const { request, env } = context;

  let body;
  try {
    body = await request.json();
  } catch {
    return json({ error: "Invalid request." }, 400);
  }

  const name = String(body.name ?? "").trim();
  const email = String(body.email ?? "").trim().toLowerCase();
  // Honeypot: a hidden field real users never fill. Bots do. Silently accept + drop.
  const trap = String(body.company ?? "").trim();
  if (trap) return json({ ok: true });

  if (!name || name.length > 200) {
    return json({ error: "Please enter your first name." }, 400);
  }
  if (!isValidEmail(email)) {
    return json({ error: "Please enter a valid email." }, 400);
  }

  let token;
  try {
    token = await savePendingSignup(env, email, name, null);
  } catch {
    return json({ error: "Could not save right now. Try again later." }, 500);
  }

  try {
    await sendConfirmEmail(env, email, name, token);
  } catch {
    return json({ error: "Saved, but the confirmation email failed to send. Try again." }, 502);
  }

  return json({ ok: true });
}

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json" },
  });
}
