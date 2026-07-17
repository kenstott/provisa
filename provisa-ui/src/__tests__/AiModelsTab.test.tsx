// Copyright (c) 2026 Kenneth Stott
// Canary: 67464879-2052-4866-ba5b-572ab38c24a3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor, fireEvent } from '../test-utils/render';
import { AiModelsTab } from '../components/admin/AiModelsTab';
import type { AiModelsState } from '../api/aiModels';

vi.mock('../api/aiModels', () => ({
  fetchAiModels: vi.fn(),
  setAiModels: vi.fn(),
}));

import { fetchAiModels, setAiModels } from '../api/aiModels';
const mockFetch = vi.mocked(fetchAiModels);
const mockSet = vi.mocked(setAiModels);

function state(overrides: Partial<AiModelsState> = {}): AiModelsState {
  return {
    ai_models: {
      table_description: 'claude-haiku-4-5-20251001',
      column_description: 'claude-haiku-4-5-20251001',
      relationship_inference: 'claude-haiku-4-5-20251001',
      sql_generation: 'claude-opus-4-6',
      table_selection: 'claude-haiku-4-5-20251001',
    },
    vector_models: [
      {
        id: 'text-embedding-3-small',
        provider: 'openai',
        dimensions: 1536,
        api_key_env: 'OPENAI_API_KEY',
        base_url: null,
        enabled: true,
      },
    ],
    nl: { rate_limit: 60 },
    restart_required_note: 'AI model settings take effect after a service restart.',
    ...overrides,
  };
}

describe('AiModelsTab', () => {
  beforeEach(() => {
    mockFetch.mockReset();
    mockSet.mockReset();
  });

  it('renders the model-role fields with loaded values', async () => {
    mockFetch.mockResolvedValue(state());
    render(<AiModelsTab />);

    await waitFor(() =>
      expect(screen.getByTestId('ai-model-sql_generation')).toBeInTheDocument(),
    );
    expect(screen.getByTestId('ai-model-sql_generation')).toHaveValue('claude-opus-4-6');
    expect(screen.getByTestId('ai-model-table_description')).toHaveValue(
      'claude-haiku-4-5-20251001',
    );
  });

  it('saves edited values via setAiModels', async () => {
    mockFetch.mockResolvedValue(state());
    mockSet.mockResolvedValue({ success: true, updated: ['ai_models.sql_generation'], restart_required: true });
    render(<AiModelsTab />);

    await waitFor(() =>
      expect(screen.getByTestId('ai-model-sql_generation')).toBeInTheDocument(),
    );

    fireEvent.change(screen.getByTestId('ai-model-sql_generation'), {
      target: { value: 'claude-opus-4-8' },
    });
    fireEvent.click(screen.getByTestId('ai-models-save'));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    const arg = mockSet.mock.calls[0][0];
    expect(arg.ai_models?.sql_generation).toBe('claude-opus-4-8');
    expect(arg.nl).toEqual({ rate_limit: 60 });
    expect(arg.vector_models?.[0].id).toBe('text-embedding-3-small');
  });
});
