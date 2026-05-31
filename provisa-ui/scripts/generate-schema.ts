import { getIntrospectionQuery, buildClientSchema, printSchema } from 'graphql';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const API_BASE = process.env.VITE_API_BASE || 'http://127.0.0.1:8000';
const SCHEMA_OUTPUT = path.join(__dirname, '../schema.graphql');

async function generateSchema() {
  try {
    console.log(`Fetching schema from ${API_BASE}/admin/graphql...`);
    const response = await fetch(`${API_BASE}/admin/graphql`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query: getIntrospectionQuery() }),
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const { data, errors } = await response.json();
    if (errors) {
      throw new Error(`GraphQL errors: ${JSON.stringify(errors)}`);
    }

    const schema = buildClientSchema(data);
    const sdl = printSchema(schema);

    fs.writeFileSync(SCHEMA_OUTPUT, sdl);
    console.log(`✓ Schema written to ${SCHEMA_OUTPUT}`);
  } catch (error) {
    console.error('Failed to generate schema:', error);
    process.exit(1);
  }
}

generateSchema();
