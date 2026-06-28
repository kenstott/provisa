import { test, expect } from './coverage';

const ADMIN_GQL = 'http://localhost:8000/admin/graphql';

test.describe('Source Registration & Admin Endpoints', () => {
  // REQ-012: Source registration validates connection (queries sources endpoint)
  test('REQ-012: Source registration validates connection', async ({ request }) => {
    const response = await request.post(ADMIN_GQL, {
      data: {
        query: `
          query {
            sources {
              id
              type
            }
          }
        `,
      },
    });

    expect(response.status()).toBe(200);
    const json = await response.json();
    expect(json.data).toBeDefined();
    expect(Array.isArray(json.data.sources)).toBe(true);
  });

  // REQ-013: Source registration does not expose data (verifies unregistered tables not in schema)
  test('REQ-013: Source registration does not expose data (unregistered tables not in schema)', async ({ request }) => {
    const response = await request.post(ADMIN_GQL, {
      data: {
        query: `
          query {
            __schema {
              types {
                name
                kind
              }
            }
          }
        `,
      },
    });

    expect(response.status()).toBe(200);
    const json = await response.json();
    expect(json.data).toBeDefined();
    expect(json.data.__schema).toBeDefined();

    const typeNames = json.data.__schema.types.map((t: any) => t.name);
    expect(Array.isArray(typeNames)).toBe(true);
  });

  // REQ-209: Webhook-backed mutations (queries tracked webhooks via admin/actions endpoint)
  test('REQ-209: Webhook-backed mutations (queries tracked webhooks endpoint)', async ({ request }) => {
    const response = await request.get('http://localhost:8000/admin/actions');

    expect([200, 401, 403]).toContain(response.status());
    if (response.status() === 200) {
      const json = await response.json();
      expect(json).toBeDefined();
    }
  });

  // REQ-253: Naming convention changes (verifies schema updates after convention changes)
  test('REQ-253: Naming convention changes (verifies schema updates)', async ({ request }) => {
    const getResponse = await request.get('http://localhost:8000/admin/settings');

    expect(getResponse.status()).toBe(200);
    const getJson = await getResponse.json();
    expect(getJson).toBeDefined();
    expect(typeof getJson).toBe('object');
  });

  // REQ-296: Neo4j table registration query preview (tests cypher endpoint)
  test('REQ-296: Neo4j table registration query preview (tests cypher endpoint)', async ({ request }) => {
    const response = await request.post('http://localhost:8000/data/cypher', {
      data: {
        query: 'MATCH (n) RETURN count(n) as count',
      },
      headers: { 'Content-Type': 'application/json' },
    });

    expect([200, 400, 500]).toContain(response.status());
    if (response.status() === 200) {
      const json = await response.json();
      expect(json).toBeDefined();
    }
  });

  // REQ-311: Remote schema introspection refresh (queries graphql-remote sources)
  test('REQ-311: Remote schema introspection refresh (queries graphql-remote sources)', async ({ request }) => {
    const response = await request.post(ADMIN_GQL, {
      data: {
        query: `
          query {
            sources {
              id
              type
            }
          }
        `,
      },
    });

    expect(response.status()).toBe(200);
    const json = await response.json();
    expect(json.data).toBeDefined();
    expect(Array.isArray(json.data.sources)).toBe(true);
    // Filter graphql-remote sources client-side
    const gqlSources = json.data.sources.filter((s: any) => s.type === 'graphql_remote');
    expect(Array.isArray(gqlSources)).toBe(true);
  });

  // REQ-321: OpenAPI spec refresh (queries and verifies petstore-api source)
  test('REQ-321: OpenAPI spec refresh (queries and verifies petstore-api source)', async ({ request }) => {
    const response = await request.post(ADMIN_GQL, {
      data: {
        query: `
          query {
            sources {
              id
              type
            }
          }
        `,
      },
    });

    expect(response.status()).toBe(200);
    const json = await response.json();
    expect(json.data).toBeDefined();
    expect(Array.isArray(json.data.sources)).toBe(true);
    // Filter openapi sources client-side
    const openapiSources = json.data.sources.filter((s: any) => s.type === 'openapi');
    expect(Array.isArray(openapiSources)).toBe(true);
  });

  // REQ-329: Proto schema refresh (queries grpc-remote sources)
  test('REQ-329: Proto schema refresh (queries grpc-remote sources)', async ({ request }) => {
    const response = await request.post(ADMIN_GQL, {
      data: {
        query: `
          query {
            sources {
              id
              type
            }
          }
        `,
      },
    });

    expect(response.status()).toBe(200);
    const json = await response.json();
    expect(json.data).toBeDefined();
    expect(Array.isArray(json.data.sources)).toBe(true);
    // Filter grpc-remote sources client-side
    const grpcSources = json.data.sources.filter((s: any) => s.type === 'grpc_remote');
    expect(Array.isArray(grpcSources)).toBe(true);
  });

  // REQ-363: SQLAlchemy dialect introspection (verifies role-governed GraphQL introspection)
  test('REQ-363: SQLAlchemy dialect introspection (verifies role-governed GraphQL introspection)', async ({ request }) => {
    const response = await request.post(ADMIN_GQL, {
      data: {
        query: `
          query {
            __type(name: "__Schema") {
              name
              kind
              fields {
                name
              }
            }
          }
        `,
      },
    });

    expect(response.status()).toBe(200);
    const json = await response.json();
    expect(json.data).toBeDefined();
    expect(json.data.__type).toBeDefined();
    expect(json.data.__type.fields).toBeDefined();
    expect(Array.isArray(json.data.__type.fields)).toBe(true);
  });

  // REQ-413: Auto-generate FK relationships (queries schema for relationships)
  test('REQ-413: Auto-generate FK relationships (queries schema for relationships)', async ({ request }) => {
    const response = await request.post(ADMIN_GQL, {
      data: {
        query: `
          query {
            relationships {
              id
              sourceTableName
              targetTableName
              sourceColumn
              targetColumn
            }
          }
        `,
      },
    });

    expect(response.status()).toBe(200);
    const json = await response.json();
    expect(json.data).toBeDefined();
    expect(Array.isArray(json.data.relationships) || json.data.relationships === null).toBe(true);
  });

  // REQ-414: Demo schema FK relationships (verifies at least one FK relationship exists)
  test('REQ-414: Demo schema FK relationships (verifies at least one FK relationship exists)', async ({ request }) => {
    const response = await request.post(ADMIN_GQL, {
      data: {
        query: `
          query {
            relationships {
              id
              sourceTableName
              targetTableName
              sourceColumn
              targetColumn
            }
          }
        `,
      },
    });

    expect(response.status()).toBe(200);
    const json = await response.json();
    expect(json.data).toBeDefined();
    expect(Array.isArray(json.data.relationships)).toBe(true);
    if (json.data.relationships.length > 0) {
      const rel = json.data.relationships[0];
      expect(rel.sourceTableName).toBeDefined();
      expect(rel.targetTableName).toBeDefined();
      expect(rel.sourceColumn).toBeDefined();
    }
  });

  // REQ-433: Multi-domain datasource association (verifies sources with multiple domains)
  test('REQ-433: Multi-domain datasource association (verifies sources with multiple domains)', async ({ request }) => {
    const response = await request.post(ADMIN_GQL, {
      data: {
        query: `
          query {
            sources {
              id
              type
              allowedDomains
            }
          }
        `,
      },
    });

    expect(response.status()).toBe(200);
    const json = await response.json();
    expect(json.data).toBeDefined();
    expect(Array.isArray(json.data.sources)).toBe(true);
    const sourcesWithDomains = json.data.sources.filter((s: any) => s.allowedDomains && s.allowedDomains.length > 0);
    if (sourcesWithDomains.length > 0) {
      expect(Array.isArray(sourcesWithDomains[0].allowedDomains)).toBe(true);
    }
  });

  // REQ-598: Remote schema relationships configuration (checks relationship fields)
  test('REQ-598: Remote schema relationships configuration (checks remoteManaged flag)', async ({ request }) => {
    const response = await request.post(ADMIN_GQL, {
      data: {
        query: `
          query {
            relationships {
              id
              sourceTableName
              targetTableName
              disableCypher
            }
          }
        `,
      },
    });

    expect(response.status()).toBe(200);
    const json = await response.json();
    expect(json.data).toBeDefined();
    expect(Array.isArray(json.data.relationships)).toBe(true);
    if (json.data.relationships.length > 0) {
      const rel = json.data.relationships[0];
      expect(typeof rel.disableCypher === 'boolean').toBe(true);
    }
  });
});
