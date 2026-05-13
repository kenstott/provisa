// Copyright (c) 2026 Kenneth Stott
// Canary: a1b2c3d4-e5f6-7890-abcd-ef1234567890
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the COPYRIGHT holder.

import { test, expect } from "./coverage";

/**
 * Data shape tests: ps__pets → assignment → employee cardinality.
 *
 * pets-to-shelter-assignments is many-to-one, so each pet has at most one
 * assignment. When assignment.employee is many-to-one, the assignment object
 * has a single employee. Pets must NOT repeat per employee.
 */
test.describe("ps__pets assignment shape", () => {
  test.describe.configure({ timeout: 30000 });

  test("each pet appears exactly once (no cross-join explosion)", async ({
    request,
  }) => {
    const resp = await request.post("http://localhost:8000/data/graphql", {
      headers: {
        "Content-Type": "application/json",
        "X-Role": "admin",
      },
      data: {
        query: `{
          ps__pets {
            assignment { breedName employee { lastName } }
            name
            breedName
          }
        }`,
      },
    });

    expect(resp.ok()).toBeTruthy();
    const body = await resp.json();
    expect(body.errors).toBeUndefined();

    const pets: Array<{ name: string; breedName: string }> =
      body.data.ps__pets ?? [];
    expect(pets.length).toBeGreaterThan(0);

    // Deduplicate by name+breedName — if pets repeat, uniqueCount < total
    const keys = pets.map((p) => `${p.name}::${p.breedName}`);
    const uniqueCount = new Set(keys).size;
    expect(uniqueCount).toBe(
      pets.length,
      `Pets are duplicated: ${pets.length} rows but only ${uniqueCount} unique name+breedName combos`
    );
  });

  test("assignment.employee is a single object, not an array", async ({
    request,
  }) => {
    const resp = await request.post("http://localhost:8000/data/graphql", {
      headers: {
        "Content-Type": "application/json",
        "X-Role": "admin",
      },
      data: {
        query: `{
          ps__pets {
            assignment { breedName employee { lastName } }
            name
            breedName
          }
        }`,
      },
    });

    expect(resp.ok()).toBeTruthy();
    const body = await resp.json();
    expect(body.errors).toBeUndefined();

    const pets: Array<{
      name: string;
      breedName: string;
      assignment: { breedName: string; employee: { lastName: string } | null } | null;
    }> = body.data.ps__pets ?? [];

    for (const pet of pets) {
      if (pet.assignment?.employee != null) {
        expect(
          Array.isArray(pet.assignment.employee),
          `pet "${pet.name}" assignment.employee should be an object, got an array`
        ).toBe(false);
      }
    }
  });
});
