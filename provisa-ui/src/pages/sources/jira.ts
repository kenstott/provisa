// Copyright (c) 2026 Kenneth Stott
// Canary: 19ff6a4c-dfd0-48ec-8758-5129d90c97d8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2069: a Jira source — the site's address, the Atlassian account's email and its API token,
// riding in the source's host, username and password.

/** Whether a site address is one the server takes: the full https:// address, or a
 * ${secret:…} / ${env:…} reference to it (checked when the source is used). */
export function jiraSiteUrlValid(url: string): boolean {
  const value = url.trim();
  return value.startsWith("${") || /^https:\/\/\S+$/.test(value);
}
