// Copyright (c) 2026 Kenneth Stott
// Canary: 181e1388-e9fa-4884-bca4-68d5e4896e4b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923: the entry point of source-sign-in.html, the page an issuer returns to. See
// lib/sourceSignIn.ts. The first statement takes the issuer's code out of the address; nothing
// here, and nothing this module imports, asks any server for anything.

import { answerOpener, takeIssuerAnswer } from "./lib/sourceSignIn";

const answer = takeIssuerAnswer(window.location, window.history);
answerOpener(answer);
