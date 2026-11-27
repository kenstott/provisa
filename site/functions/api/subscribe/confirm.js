// Copyright (c) 2026 Kenneth Stott
// Canary: 646133ac-ae1e-48f0-b591-8ff3e7fd8783
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// GET /api/subscribe/confirm?token=... — REQ-2362 step 2: the link from the email api/subscribe.js
// sent. Only here, once the address is proven, does it join the Resend release list. Navigated to
// directly by a browser, so every response is an HTML page, not JSON.
import { confirmSignup } from "../../_lib/releases.js";

// Same window as the license confirmation link (REQ-1793): an older link means sign up again.
const EXPIRY_HOURS = 48;

export async function onRequestGet(context) {
  const { request, env } = context;
  const token = new URL(request.url).searchParams.get("token") ?? "";
  if (!/^[0-9a-f]{64}$/.test(token)) {
    return html(page("This confirmation link is malformed.", true), 400);
  }

  const row = await env.DB.prepare("SELECT * FROM release_signups WHERE token = ?")
    .bind(token)
    .first();

  if (!row) {
    return html(page("This confirmation link is invalid or was replaced by a newer one.", true), 404);
  }
  if (row.confirmed_at) {
    return html(page("You're already confirmed. You'll get an email when a new release ships.", false));
  }
  const ageHours = (Date.now() - Date.parse(row.created_at + "Z")) / 3_600_000;
  if (ageHours > EXPIRY_HOURS) {
    return html(page("This confirmation link has expired. Please sign up again.", true), 410);
  }

  try {
    await confirmSignup(env, row);
  } catch {
    return html(
      page("Something went wrong adding you to the list. Please try again, or write to support@provisa.dev.", true),
      502
    );
  }

  return html(
    page("You're confirmed. You'll get an email when a new release ships, and every one has an unsubscribe link.", false)
  );
}

function page(message, isError) {
  const next = isError ? `<a href="/#releases">Sign up again</a>` : `<a href="/">Back home</a>`;
  return (
    `<!doctype html><html lang="en"><head><meta charset="utf-8">` +
    `<title>Release emails - Provisa</title>` +
    `<meta name="viewport" content="width=device-width, initial-scale=1">` +
    `<link rel="stylesheet" href="/styles.css"></head>` +
    `<body><main class="container" style="padding:4rem 1rem;max-width:640px;margin:0 auto;">` +
    `<h1>Provisa</h1><p>${message}</p><p>${next}</p></main></body></html>`
  );
}

function html(body, status = 200) {
  return new Response(body, { status, headers: { "content-type": "text/html; charset=utf-8" } });
}
