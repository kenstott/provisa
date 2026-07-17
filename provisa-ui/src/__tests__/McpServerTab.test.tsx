// Copyright (c) 2026 Kenneth Stott
// Canary: 363b84fc-259c-4ee0-ab19-a60226ed2586
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '../test-utils/render';
import { McpServerTab } from '../components/admin/McpServerTab';
import type { McpServerStatus } from '../api/admin';

vi.mock('../api/admin', () => ({
  fetchMcpServer: vi.fn(),
}));

import { fetchMcpServer } from '../api/admin';
const mockFetch = vi.mocked(fetchMcpServer);

const TOOLS = [
  { name: 'list_schemas', description: 'List catalog schemas.' },
  { name: 'list_tables', description: 'List tables.' },
  { name: 'describe_table', description: 'Describe a table.' },
  { name: 'run_sql', description: 'Execute SQL.' },
  { name: 'explain_sql', description: 'Explain SQL.' },
];

function status(overrides: Partial<McpServerStatus>): McpServerStatus {
  return {
    enabled: false,
    port: null,
    transport: null,
    stdio_role: null,
    max_rows: 1000,
    tools: TOOLS,
    enable_env_var: 'PROVISA_MCP_PORT',
    role_env_var: 'PROVISA_MCP_ROLE',
    ...overrides,
  };
}

describe('McpServerTab', () => {
  beforeEach(() => mockFetch.mockReset());

  it('renders enabled state with endpoint, transport, and bound role', async () => {
    mockFetch.mockResolvedValue(
      status({ enabled: true, port: 9100, transport: 'streamable-http', stdio_role: 'analyst' }),
    );
    render(<McpServerTab />);

    await waitFor(() => expect(screen.getByTestId('mcp-status')).toHaveTextContent('Enabled'));
    expect(screen.getByTestId('mcp-endpoint')).toHaveTextContent('9100');
    expect(screen.getByTestId('mcp-role')).toHaveTextContent('analyst');
    expect(screen.queryByTestId('mcp-enable-hint')).not.toBeInTheDocument();
  });

  it('renders disabled state with the enable hint and env var', async () => {
    mockFetch.mockResolvedValue(status({ enabled: false }));
    render(<McpServerTab />);

    await waitFor(() => expect(screen.getByTestId('mcp-status')).toHaveTextContent('Disabled'));
    expect(screen.getByTestId('mcp-enable-hint')).toHaveTextContent('PROVISA_MCP_PORT');
    expect(screen.queryByTestId('mcp-endpoint')).not.toBeInTheDocument();
  });

  it('lists all five exposed tools', async () => {
    mockFetch.mockResolvedValue(status({ enabled: true, port: 9100 }));
    render(<McpServerTab />);

    await waitFor(() => expect(screen.getByTestId('mcp-tools')).toBeInTheDocument());
    for (const t of ['list_schemas', 'list_tables', 'describe_table', 'run_sql', 'explain_sql']) {
      expect(screen.getByText(t)).toBeInTheDocument();
    }
  });
});
