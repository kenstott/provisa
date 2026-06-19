// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

const QUERY = `
  query MyQuery {
    ps__inquiries {
      id
      inquiryType
      status
      submittedAt
      pet {
        name
        price
        species
        assignment {
          employee {
            firstName
          }
          breed {
            name
            species
            careLevel
          }
        }
      }
    }
  }
`;

test("ps__inquiries with nested breed resolves without error", async ({ request }) => {
  const resp = await request.post("http://localhost:8000/data/graphql", {
    data: { query: QUERY },
    headers: { "Content-Type": "application/json" },
  });

  const body = await resp.json();
  expect(resp.status(), JSON.stringify(body)).toBe(200);

  expect(body.errors, JSON.stringify(body.errors ?? [])).toBeUndefined();
  expect(body.data).toHaveProperty("ps__inquiries");
  expect(Array.isArray(body.data.ps__inquiries)).toBe(true);
});
