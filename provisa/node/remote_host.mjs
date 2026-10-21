// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2098: a Node source's remote host -- the host program (host-core.mjs) served over mutual TLS on
// one port, in the image Provisa builds for the source. The same as Python's remote_host.py:
//
//   node remote_host.mjs <port> <state directory> <bundle entry module>
//
// Provisa dials it: every Provisa process keeps a connection of its own, each served as the local
// worker's socket is, with the code's hello. The host's identity is an ECDSA P-256 key and a
// self-signed certificate made on its first start and kept in the state directory, with the
// certificate of the Provisa it is paired to. Node has no API that makes a certificate, so the
// certificate's DER is written here (x509), signed with node:crypto: no dependency.
//
// PAIRING. On its first start the host prints a one-time pairing code: its certificate's SHA-256
// fingerprint and a secret. Until it is paired it takes one message on a connection, {op: "pair",
// secret, certificate}: with the code's secret, it keeps the certificate as the one Provisa
// presents, answers {op: "paired"}, and from then requires that certificate of every connection.
// Another secret is refused, and the host is not paired.

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import tls from "node:tls";
import { Connection, encode, frame, loadBundle } from "./host-core.mjs";

const KEY = "host.key";
const CERTIFICATE = "host.crt";
const TRUSTED = "provisa.crt"; // the certificate of the Provisa the host is paired to
const SECRET = "pairing.secret"; // kept until the host is paired

// -- a self-signed certificate, as DER ------------------------------------------------------

function length(n) {
  if (n < 0x80) return Buffer.from([n]);
  const bytes = [];
  for (let x = n; x > 0; x = Math.floor(x / 256)) bytes.unshift(x & 0xff);
  return Buffer.from([0x80 | bytes.length, ...bytes]);
}
const tlv = (tag, content) => Buffer.concat([Buffer.from([tag]), length(content.length), content]);
const sequence = (...parts) => tlv(0x30, Buffer.concat(parts));
const set = (...parts) => tlv(0x31, Buffer.concat(parts));
const utf8 = (text) => tlv(0x0c, Buffer.from(text, "utf8"));
// An INTEGER in its one DER form: no leading 0x00 the next byte does not need (a random serial
// may begin with one, and a certificate carrying it is refused as "illegal padding"), and one
// added where the value would read as negative.
function integer(bytes) {
  let start = 0;
  while (start < bytes.length - 1 && bytes[start] === 0 && !(bytes[start + 1] & 0x80)) start++;
  const value = bytes.subarray(start);
  return tlv(0x02, value[0] & 0x80 ? Buffer.concat([Buffer.from([0]), value]) : value);
}
function oid(dotted) {
  const parts = dotted.split(".").map(Number);
  const out = [40 * parts[0] + parts[1]];
  for (const value of parts.slice(2)) {
    const groups = [value & 0x7f];
    for (let x = Math.floor(value / 128); x > 0; x = Math.floor(x / 128)) groups.unshift((x & 0x7f) | 0x80);
    out.push(...groups);
  }
  return tlv(0x06, Buffer.from(out));
}
// UTCTime: YYMMDDHHMMSSZ (a date before 2050).
const utcTime = (date) => tlv(0x17, Buffer.from(`${date.toISOString().replace(/[-:T]/g, "").slice(2, 14)}Z`));

const ECDSA_WITH_SHA256 = "1.2.840.10045.4.3.2";
const COMMON_NAME = "2.5.4.3";

/** A key and a self-signed certificate for `commonName`, both PEM, valid ten years. */
export function selfSigned(commonName) {
  const { privateKey, publicKey } = crypto.generateKeyPairSync("ec", { namedCurve: "P-256" });
  const name = sequence(set(sequence(oid(COMMON_NAME), utf8(commonName))));
  const algorithm = sequence(oid(ECDSA_WITH_SHA256));
  const serial = crypto.randomBytes(16);
  serial[0] &= 0x7f; // a positive serial
  const now = Date.now();
  const tbs = sequence(
    tlv(0xa0, integer(Buffer.from([2]))), // v3
    integer(serial),
    algorithm,
    name,
    sequence(utcTime(new Date(now - 5 * 60 * 1000)), utcTime(new Date(now + 3650 * 24 * 3600 * 1000))),
    name,
    publicKey.export({ type: "spki", format: "der" }),
  );
  const signature = crypto.sign("sha256", tbs, privateKey); // DER, as X.509 carries it
  const der = sequence(tbs, algorithm, tlv(0x03, Buffer.concat([Buffer.from([0]), signature])));
  const lines = der.toString("base64").match(/.{1,64}/g).join("\n");
  return {
    key: privateKey.export({ type: "pkcs8", format: "pem" }),
    certificate: `-----BEGIN CERTIFICATE-----\n${lines}\n-----END CERTIFICATE-----\n`,
  };
}

/** A certificate's SHA-256 fingerprint, as a pairing code carries it. */
export function fingerprint(pem) {
  return crypto.createHash("sha256").update(new crypto.X509Certificate(pem).raw).digest("hex");
}

// -- the remote host ------------------------------------------------------------------------

export class RemoteHost {
  constructor(state, bundle) {
    this.state = state;
    this.bundle = bundle;
    if (!fs.existsSync(this.file(KEY))) {
      const made = selfSigned("provisa remote host");
      fs.writeFileSync(this.file(KEY), made.key, { mode: 0o600 });
      fs.writeFileSync(this.file(CERTIFICATE), made.certificate);
    }
    if (!this.paired && !fs.existsSync(this.file(SECRET))) {
      fs.writeFileSync(this.file(SECRET), crypto.randomBytes(18).toString("base64url"), { mode: 0o600 });
    }
    this.key = fs.readFileSync(this.file(KEY));
    this.certificate = fs.readFileSync(this.file(CERTIFICATE), "utf8");
  }

  file(name) {
    return path.join(this.state, name);
  }

  get paired() {
    return fs.existsSync(this.file(TRUSTED));
  }

  /** The pairing code, while the host is not paired. */
  code() {
    if (this.paired) return null;
    return `${fingerprint(this.certificate)}.${fs.readFileSync(this.file(SECRET), "utf8")}`;
  }

  /** What the TLS server presents and trusts: Provisa's certificate, once paired. */
  context() {
    return {
      key: this.key,
      cert: this.certificate,
      ca: this.paired ? [fs.readFileSync(this.file(TRUSTED), "utf8")] : undefined,
      minVersion: "TLSv1.3",
    };
  }

  listen(port) {
    // A connection may present no certificate only to pair; once paired, every one is checked
    // against the paired Provisa's (serve).
    this.server = tls.createServer({ ...this.context(), requestCert: true, rejectUnauthorized: false }, (socket) =>
      this.serve(socket),
    );
    this.server.listen(port);
    return this.server;
  }

  serve(socket) {
    socket.on("error", () => socket.destroy()); // the connection ended: Provisa redials
    if (!this.paired) {
      this.pair(socket);
      return;
    }
    if (!socket.authorized) {
      socket.destroy(); // no certificate, or not the paired Provisa's
      return;
    }
    const connection = new Connection(
      this.bundle,
      (bytes) => socket.write(bytes),
      (message) => {
        process.stderr.write(`provisa refused this host: ${message}\n`);
        socket.end();
      },
    );
    socket.on("data", (chunk) => connection.data(chunk));
    socket.on("close", () => connection.close());
    connection.hello();
  }

  pair(socket) {
    let pending = Buffer.alloc(0);
    const answer = (message) => socket.end(frame(Buffer.concat([Buffer.from("J"), Buffer.from(encode(message))])));
    socket.on("data", (chunk) => {
      pending = Buffer.concat([pending, chunk]);
      if (pending.length < 4 || pending.length < 4 + pending.readUInt32BE(0)) return;
      const message = JSON.parse(pending.subarray(5, 4 + pending.readUInt32BE(0)).toString("utf8"));
      const held = fs.existsSync(this.file(SECRET)) ? Buffer.from(fs.readFileSync(this.file(SECRET), "utf8")) : null;
      const given = Buffer.from(String(message.secret ?? ""));
      if (message.op !== "pair" || !held || held.length !== given.length || !crypto.timingSafeEqual(held, given)) {
        answer({ op: "refused", message: "not this host's pairing code" });
        return;
      }
      const certificate = String(message.certificate);
      new crypto.X509Certificate(certificate); // a certificate, or refused here
      fs.writeFileSync(this.file(TRUSTED), certificate);
      fs.unlinkSync(this.file(SECRET));
      this.server.setSecureContext(this.context());
      answer({ op: "paired" });
      process.stderr.write("paired with Provisa\n");
    });
  }
}

async function main(argv) {
  const [port, state, entry] = argv;
  fs.mkdirSync(state, { recursive: true });
  const remote = new RemoteHost(state, await loadBundle(entry));
  const code = remote.code();
  if (code !== null) process.stderr.write(`pairing code: ${code}\n`);
  remote.listen(Number(port));
  process.stderr.write(`remote host listening on ${port}\n`);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  // What the code prints goes to the container's log, never to a connection.
  console.log = (...args) => process.stderr.write(`${args.join(" ")}\n`);
  await main(process.argv.slice(2));
}
