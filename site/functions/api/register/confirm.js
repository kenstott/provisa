// Copyright (c) 2026 Kenneth Stott
// Canary: 8c3f6a15-2d9e-4b70-a1c8-6f5e3d2b9a47
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// GET /api/register/confirm?token=... — REQ-1793 step 2: the link a registrant clicks from the
// email api/register.js sent. Only here, once the link is clicked, is the license actually signed
// and emailed — proving control of the address before anything is issued. Navigated to directly
// by a browser, so every response is an HTML page, not JSON.
import { signLicense } from "../../_lib/license.js";

// Unconfirmed registrations older than this are treated as abandoned, not confirmable — the
// registrant re-registers to get a fresh link rather than confirming a week-old request.
const EXPIRY_HOURS = 48;

export async function onRequestGet(context) {
  const { request, env } = context;
  const token = new URL(request.url).searchParams.get("token") ?? "";
  if (!/^[0-9a-f]{64}$/.test(token)) {
    return html(errorPage("This confirmation link is malformed."), 400);
  }

  const row = await env.DB.prepare(
    "SELECT * FROM license_registrations WHERE token = ?"
  )
    .bind(token)
    .first();

  if (!row) {
    return html(errorPage("This confirmation link is invalid or has already been used."), 404);
  }
  if (row.confirmed_at) {
    return html(
      successPage(
        "Already confirmed — check your inbox for the license file we already sent you."
      )
    );
  }
  const ageHours = (Date.now() - Date.parse(row.created_at + "Z")) / 3_600_000;
  if (ageHours > EXPIRY_HOURS) {
    return html(errorPage("This confirmation link has expired. Please register again."), 410);
  }

  const payload = {
    company: row.company,
    position: row.position,
    role: row.role,
    first_name: row.first_name,
    last_name: row.last_name,
    email: row.email,
    machine_id: row.machine_id,
    issued_at: new Date().toISOString().slice(0, 10),
  };
  if (row.phone) payload.phone = row.phone;

  let licenseJson;
  try {
    payload.sig = await signLicense(payload, env.LICENSE_PRIVATE_KEY_PKCS8_B64);
    licenseJson = JSON.stringify(payload, null, 2);
  } catch (err) {
    return html(
      errorPage("Something went wrong signing your license. Please contact license@provisa.dev."),
      500
    );
  }

  try {
    await sendLicenseEmail(env, row.email, row.first_name, licenseJson);
  } catch (err) {
    return html(
      errorPage(
        "Your license was signed but the email failed to send. Please contact license@provisa.dev."
      ),
      502
    );
  }

  await env.DB.prepare(
    "UPDATE license_registrations SET confirmed_at = datetime('now') WHERE token = ?"
  )
    .bind(token)
    .run();

  return html(
    successPage(
      "Registration confirmed! Your license file is on its way to your inbox — apply it with " +
        "<code>provisa license apply &lt;file&gt;</code>."
    )
  );
}

async function sendLicenseEmail(env, to, firstName, licenseJson) {
  const res = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.RESEND_API_KEY}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      from: "Provisa Licensing <license@provisa.dev>",
      to: [to],
      subject: "Your Provisa license",
      text:
        `Hi ${firstName},\n\nYour Provisa license is attached (license.json).\n\n` +
        "Install it with:\n  provisa license apply <file>\n\n" +
        "or upload it in Settings -> License, or place it at ~/.provisa/license.json.\n\nProvisa",
      html:
        `<p>Hi ${escapeHtml(firstName)},</p><p>Your Provisa license is attached (<code>license.json</code>).</p>` +
        `<p>Install it with:</p><pre>provisa license apply &lt;file&gt;</pre>` +
        `<p>...or upload it in Settings &rarr; License, or place it at <code>~/.provisa/license.json</code>.</p>`,
      attachments: [
        { filename: "license.json", content: btoa(licenseJson) },
      ],
    }),
  });
  if (!res.ok) throw new Error(`resend send failed: ${res.status} ${await res.text()}`);
}

function escapeHtml(s) {
  return s.replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

function page(title, body) {
  return (
    `<!doctype html><html lang="en"><head><meta charset="utf-8">` +
    `<title>${title} - Provisa</title>` +
    `<meta name="viewport" content="width=device-width, initial-scale=1">` +
    `<link rel="stylesheet" href="/styles.css"></head>` +
    `<body><main class="container" style="padding:4rem 1rem;max-width:640px;margin:0 auto;">${body}</main></body></html>`
  );
}

function successPage(message) {
  return page("Registration confirmed", `<h1>Provisa</h1><p>${message}</p><p><a href="/">Back home</a></p>`);
}

function errorPage(message) {
  return page("Registration error", `<h1>Provisa</h1><p>${message}</p><p><a href="/register">Register again</a></p>`);
}

function html(body, status = 200) {
  return new Response(body, { status, headers: { "content-type": "text/html; charset=utf-8" } });
}
