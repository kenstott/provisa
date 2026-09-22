// Copyright (c) 2026 Kenneth Stott
// Canary: 5d8e2c47-9a1b-4f36-8c02-7e4d9f1a6b83
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// POST /api/register — REQ-1793 step 1 of the double-opt-in license flow. Stores the submitted
// registration as unconfirmed and emails a confirmation link; the license itself is only signed
// and sent once that link is clicked (functions/api/register/confirm.js), so an email the
// registrant doesn't control can never receive a working license.
import { isValidEmail, randomToken, sendEmail } from "../_lib/license.js";

export async function onRequestPost(context) {
  const { request, env } = context;

  let body;
  try {
    body = await request.json();
  } catch {
    return json({ error: "Invalid request." }, 400);
  }

  const fields = {
    company: String(body.company ?? "").trim(),
    position: String(body.position ?? "").trim(),
    role: String(body.role ?? "").trim(),
    first_name: String(body.first_name ?? "").trim(),
    last_name: String(body.last_name ?? "").trim(),
    email: String(body.email ?? "").trim().toLowerCase(),
    phone: String(body.phone ?? "").trim() || null,
    machine_id: String(body.machine_id ?? "").trim(),
  };
  // Honeypot, matching /api/subscribe's convention.
  if (String(body.website ?? "").trim()) return json({ ok: true });

  for (const [name, label] of [
    ["company", "company"],
    ["position", "position/title"],
    ["role", "role"],
    ["first_name", "first name"],
    ["last_name", "last name"],
    ["machine_id", "machine ID"],
  ]) {
    if (!fields[name] || fields[name].length > 200) {
      return json({ error: `Please enter your ${label}.` }, 400);
    }
  }
  if (!isValidEmail(fields.email)) {
    return json({ error: "Please enter a valid email." }, 400);
  }

  const token = randomToken();
  try {
    await env.DB.prepare(
      "INSERT INTO license_registrations " +
        "(token, email, company, position, role, first_name, last_name, phone, machine_id) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
        "ON CONFLICT(email, machine_id) DO UPDATE SET " +
        "token = excluded.token, company = excluded.company, position = excluded.position, " +
        "role = excluded.role, first_name = excluded.first_name, last_name = excluded.last_name, " +
        "phone = excluded.phone, created_at = datetime('now'), confirmed_at = NULL"
    )
      .bind(
        token,
        fields.email,
        fields.company,
        fields.position,
        fields.role,
        fields.first_name,
        fields.last_name,
        fields.phone,
        fields.machine_id
      )
      .run();
  } catch (err) {
    return json({ error: "Could not save right now. Try again later." }, 500);
  }

  const confirmUrl = `https://provisa.dev/api/register/confirm?token=${token}`;
  try {
    await sendEmail(env, {
      to: fields.email,
      subject: "Confirm your Provisa license registration",
      text:
        `Hi ${fields.first_name},\n\n` +
        "Click the link below to confirm your Provisa registration and receive your free license file:\n\n" +
        `${confirmUrl}\n\n` +
        "If you didn't request this, ignore this email.\n\nProvisa",
      html:
        `<p>Hi ${escapeHtml(fields.first_name)},</p>` +
        `<p>Click the link below to confirm your Provisa registration and receive your free license file:</p>` +
        `<p><a href="${confirmUrl}">Confirm registration</a></p>` +
        `<p>If you didn't request this, ignore this email.</p><p>Provisa</p>`,
    });
  } catch (err) {
    return json({ error: "Registered, but the confirmation email failed to send. Try again." }, 502);
  }

  return json({ ok: true });
}

function escapeHtml(s) {
  return s.replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json" },
  });
}
