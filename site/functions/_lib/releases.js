// Copyright (c) 2026 Kenneth Stott
// Canary: 18d6ff30-099e-4ed1-b4c1-2b375dbe935f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Shared helpers for the release-email signup (REQ-2362). Not a route: it exports no onRequest*.
// A signup is stored pending in D1 and joins the Resend release list only once its address is
// proven, either by the confirmation link (api/subscribe/confirm.js) or by the license
// confirmation link when the registrant ticked the release-email box (api/register/confirm.js).
import { randomToken } from "./license.js";

const FROM = "Provisa <releases@provisa.dev>";

// Upserts the pending signup for `email` with a fresh token and returns it. `licenseToken` is the
// license registration that will confirm this signup, or null when it came from the home page form.
export async function savePendingSignup(env, email, name, licenseToken) {
  const token = randomToken();
  await env.DB.prepare(
    "INSERT INTO release_signups (token, email, name, license_token) VALUES (?, ?, ?, ?) " +
      "ON CONFLICT(email) DO UPDATE SET token = excluded.token, name = excluded.name, " +
      "license_token = excluded.license_token, created_at = datetime('now'), confirmed_at = NULL"
  )
    .bind(token, email, name, licenseToken)
    .run();
  return token;
}

export async function sendConfirmEmail(env, to, name, token) {
  const url = `https://provisa.dev/api/subscribe/confirm?token=${token}`;
  await resendOk(
    await resend(env, "POST", "/emails", {
      from: FROM,
      to: [to],
      subject: "Confirm your Provisa release emails",
      text:
        `Hi ${name},\n\n` +
        "Click the link below to get an email when a new Provisa release ships:\n\n" +
        `${url}\n\n` +
        "If you didn't ask for this, ignore this email and you won't hear from us.\n\nProvisa",
      html:
        `<p>Hi ${escapeHtml(name)},</p>` +
        "<p>Click the link below to get an email when a new Provisa release ships:</p>" +
        `<p><a href="${url}">Confirm release emails</a></p>` +
        "<p>If you didn't ask for this, ignore this email and you won't hear from us.</p><p>Provisa</p>",
    })
  );
}

// Adds the proven address to the Resend release segment, then marks the signup confirmed. Resend
// is the list Broadcasts send to and where unsubscribes land; D1 keeps the signup record.
export async function confirmSignup(env, row) {
  const segment = env.RESEND_RELEASES_SEGMENT_ID;
  if (!segment) throw new Error("RESEND_RELEASES_SEGMENT_ID is not set");
  const id = encodeURIComponent(row.email);
  const found = await resend(env, "GET", `/contacts/${id}`);
  if (found.status === 404) {
    await resendOk(
      await resend(env, "POST", "/contacts", {
        email: row.email,
        first_name: row.name,
        unsubscribed: false,
      })
    );
  } else {
    await resendOk(found);
    // They just confirmed a fresh link, so an earlier unsubscribe no longer reflects their choice.
    await resendOk(await resend(env, "PATCH", `/contacts/${id}`, { unsubscribed: false }));
  }
  await resendOk(await resend(env, "POST", `/contacts/${id}/segments/${segment}`));
  await env.DB.prepare("UPDATE release_signups SET confirmed_at = datetime('now') WHERE token = ?")
    .bind(row.token)
    .run();
}

async function resend(env, method, path, body) {
  return fetch(`https://api.resend.com${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${env.RESEND_API_KEY}`,
      "content-type": "application/json",
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

async function resendOk(res) {
  if (!res.ok) throw new Error(`resend ${res.url} failed: ${res.status} ${await res.text()}`);
}

export function escapeHtml(s) {
  return s.replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}
