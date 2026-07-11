// Copyright (c) 2026 Kenneth Stott
// Canary: 55ecea78-4131-4bb4-8a93-71e1bdacea84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/** Reusable auth fields for username/password */
export function AuthUserPass({
  authFields,
  setAuthFields,
}: {
  authFields: Record<string, string>;
  setAuthFields: (f: Record<string, string>) => void;
}) {
  return (
    <>
      <label>
        Username{" "}
        <input
          required
          value={authFields.username ?? ""}
          onChange={(e) => setAuthFields({ ...authFields, username: e.target.value })}
        />
      </label>
      <label>
        Password{" "}
        <input
          type="password"
          required
          value={authFields.password ?? ""}
          onChange={(e) => setAuthFields({ ...authFields, password: e.target.value })}
        />
      </label>
    </>
  );
}
