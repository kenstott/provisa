// Copyright (c) 2026 Kenneth Stott
// Canary: 3bc5f222-ba90-4a90-ad09-8f2f602f9395
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// POST /api/subscribe — appends a {name, email} row to the private D1 table
// `subscribers`. The table lives in your Cloudflare account only; export it with
// `wrangler d1 export provisa-subscribers`. Binding `DB` is declared in wrangler.jsonc.
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
    return json({ error: "Please enter your name." }, 400);
  }
  if (email.length > 320 || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    return json({ error: "Please enter a valid email." }, 400);
  }

  try {
    await env.DB.prepare(
      "INSERT INTO subscribers (email, name) VALUES (?, ?) " +
        "ON CONFLICT(email) DO UPDATE SET name = excluded.name"
    )
      .bind(email, name)
      .run();
  } catch {
    return json({ error: "Could not save right now. Try again later." }, 500);
  }

  return json({ ok: true });
}

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json" },
  });
}
