// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-2071: what the Data Science area's Python reads as: the page's own identity, from the same
// sources the request interceptor attaches (authFetch.requestIdentity), on this page's origin.
import { requestIdentity } from "../lib/authFetch";
import type { ProvisaAuth } from "./protocol";

export async function bridgeAuth(): Promise<ProvisaAuth> {
  const { bearer, org, env, roles } = await requestIdentity();
  return { baseUrl: "", role: roles, bearer, org, env };
}
