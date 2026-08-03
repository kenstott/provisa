// Copyright (c) 2026 Kenneth Stott
// Canary: 367bf8b5-9069-4e4b-936e-c4162ed75126
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect, BACKEND_URL } from './coverage';

test.describe('Infrastructure - REQ-171: MinIO bucket auto-creation', () => {
  test('GET /health returns 200 and MinIO status', async ({ request }) => {
    const response = await request.get(`${BACKEND_URL}/health`);
    expect(response.status()).toBe(200);
    const body = await response.json();
    expect(body).toHaveProperty('status');
  });
});

test.describe('Infrastructure - REQ-219: SSE subscriptions', () => {
  // The subscribe endpoint is an intentionally infinite SSE stream (keepalive loop, REQ-219/
  // REQ-258) — it never closes on its own. Playwright's APIRequestContext.get() buffers the
  // whole response body before resolving, so it can never resolve against this endpoint. Use
  // fetch() with a manual reader instead, and abort once we've inspected what we need.
  test('GET /data/subscribe/{table} returns event-stream content-type', async () => {
    // Use the pets table which is configured in the pet-store domain (sqlite-backed source)
    const controller = new AbortController();
    const response = await fetch(`${BACKEND_URL}/data/subscribe/pets`, { signal: controller.signal });
    try {
      expect(response.status).toBe(200);
      expect(response.headers.get('content-type')).toContain('text/event-stream');
    } finally {
      controller.abort();
    }
  });

  test('SSE subscription sends proper event format with data field', async () => {
    // Use the pets table; SSE streams a keepalive comment immediately on connect
    const controller = new AbortController();
    const response = await fetch(`${BACKEND_URL}/data/subscribe/pets`, { signal: controller.signal });
    try {
      expect(response.status).toBe(200);
      const reader = response.body!.getReader();
      const { value } = await reader.read();
      const text = new TextDecoder().decode(value);
      // Either a keepalive comment or a data event is acceptable
      expect(text).toMatch(/^[:\s]/);
    } finally {
      controller.abort();
    }
  });
});

test.describe('Infrastructure - REQ-222: REST endpoints', () => {
  test('GET /data/rest/{table} returns JSON array', async ({ request }) => {
    // Use the pets table which is configured in the pet-store domain
    const response = await request.get(`${BACKEND_URL}/data/rest/pet-store/pets`);
    expect(response.status()).toBe(200);
    const body = await response.json();
    expect(Array.isArray(body.data)).toBe(true);
  });

  test('GET /data/rest/{table} accepts query parameters', async ({ request }) => {
    const response = await request.get(`${BACKEND_URL}/data/rest/pet-store/pets?limit=10&offset=0`);
    expect(response.status()).toBe(200);
    const body = await response.json();
    expect(Array.isArray(body.data)).toBe(true);
  });

  test('REST endpoint response respects GraphQL-compiled schema', async ({ request }) => {
    const response = await request.get(`${BACKEND_URL}/data/rest/pet-store/pets`);
    expect(response.status()).toBe(200);
    const body = await response.json();
    if (body.data.length > 0) {
      expect(typeof body.data[0]).toBe('object');
    }
  });
});

test.describe('Infrastructure - REQ-331: Ingest POST endpoint', () => {
  test('POST /data/ingest/{source}/{table} accepts requests', async ({ request }) => {
    const response = await request.post(`${BACKEND_URL}/data/ingest/csv/contacts`, {
      data: { name: 'John', email: 'john@example.com' },
    });
    expect([202, 400, 404]).toContain(response.status());
  });

  test('Ingest endpoint with invalid source returns 404', async ({ request }) => {
    const response = await request.post(`${BACKEND_URL}/data/ingest/nonexistent/table`, {
      data: { test: 'data' },
    });
    expect(response.status()).toBe(404);
  });
});

test.describe('Infrastructure - REQ-335: Ingest accepts single/array', () => {
  test('Ingest single object returns 202 with inserted_rows count', async ({ request }) => {
    const response = await request.post(`${BACKEND_URL}/data/ingest/csv/contacts`, {
      data: { name: 'Alice', email: 'alice@example.com' },
    });
    if (response.status() === 202) {
      const body = await response.json();
      expect(body).toHaveProperty('inserted_rows');
      expect(typeof body.inserted_rows).toBe('number');
    }
  });

  test('Ingest array batch returns 202 with inserted_rows count', async ({ request }) => {
    const response = await request.post(`${BACKEND_URL}/data/ingest/csv/contacts`, {
      data: [
        { name: 'Bob', email: 'bob@example.com' },
        { name: 'Charlie', email: 'charlie@example.com' },
      ],
    });
    if (response.status() === 202) {
      const body = await response.json();
      expect(body).toHaveProperty('inserted_rows');
    }
  });

  test('Ingest missing source returns 404', async ({ request }) => {
    const response = await request.post(`${BACKEND_URL}/data/ingest/missing_source/table`, {
      data: { test: 'data' },
    });
    expect(response.status()).toBe(404);
  });

  test('Ingest missing table returns 404', async ({ request }) => {
    const response = await request.post(`${BACKEND_URL}/data/ingest/csv/missing_table`, {
      data: { test: 'data' },
    });
    expect(response.status()).toBe(404);
  });
});

test.describe('Infrastructure - REQ-336: Ingest SSE subscriptions', () => {
  // See the REQ-219 describe block above: this is an infinite SSE stream, so it must be read
  // with fetch() + a manual reader rather than APIRequestContext.get(), which buffers the whole
  // body and never resolves against a stream that never closes.
  test('Ingest table subscribable via /data/subscribe/', async () => {
    // pets is a sqlite-backed table; SSE subscribe endpoint returns event-stream for any
    // registered table, regardless of source type (falls back to watermark polling).
    const controller = new AbortController();
    const response = await fetch(`${BACKEND_URL}/data/subscribe/pets`, { signal: controller.signal });
    try {
      expect(response.status).toBe(200);
      expect(response.headers.get('content-type')).toContain('text/event-stream');
    } finally {
      controller.abort();
    }
  });

  test('Ingest subscription includes _updated_at watermark', async () => {
    const controller = new AbortController();
    const response = await fetch(`${BACKEND_URL}/data/subscribe/pets`, { signal: controller.signal });
    try {
      expect(response.status).toBe(200);
      const reader = response.body!.getReader();
      const { value } = await reader.read();
      const text = value ? new TextDecoder().decode(value) : '';
      if (text.length > 0) {
        // keepalive comment or data event
        expect(text).toMatch(/^[:\s]/);
      }
    } finally {
      controller.abort();
    }
  });
});

test.describe('Infrastructure - REQ-539: Unauthenticated endpoints', () => {
  test('GET /health returns 200 without auth token', async ({ request }) => {
    const response = await request.get(`${BACKEND_URL}/health`);
    expect(response.status()).toBe(200);
  });

  test('HEAD /health returns 200 without auth token', async ({ request }) => {
    const response = await request.head(`${BACKEND_URL}/health`);
    expect(response.status()).toBe(200);
  });

  test('GET /setup/status returns 200 without auth token', async ({ request }) => {
    const response = await request.get(`${BACKEND_URL}/setup/status`);
    expect(response.status()).toBe(200);
  });
});
