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
import { render, screen, waitFor, fireEvent, within } from '../test-utils/render';
import { AiModelsTab } from '../components/admin/AiModelsTab';
import type { AiModelsState } from '../api/aiModels';

vi.mock('../api/aiModels', () => ({
  fetchAiModels: vi.fn(),
  fetchVendorModels: vi.fn(),
  setAiModels: vi.fn(),
  LLM_VENDORS: ['anthropic', 'openai', 'cohere', 'groq', 'mistral', 'xai', 'deepseek', 'together', 'fireworks', 'nebius', 'sambanova', 'inception'],
}));

import { fetchAiModels, fetchVendorModels, setAiModels } from '../api/aiModels';
const mockFetch = vi.mocked(fetchAiModels);
const mockVendorModels = vi.mocked(fetchVendorModels);
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
    api_keys_set: { anthropic: false },
    restart_required_note: 'AI model settings take effect after a service restart.',
    ...overrides,
  };
}

describe('AiModelsTab', () => {
  beforeEach(() => {
    mockFetch.mockReset();
    mockSet.mockReset();
    mockVendorModels.mockReset();
    mockVendorModels.mockResolvedValue([]);
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

  it('changing a role vendor away from anthropic saves the full vendor/model object', async () => {
    mockFetch.mockResolvedValue(state());
    mockSet.mockResolvedValue({ success: true, updated: ['ai_models.table_selection'], restart_required: true });
    render(<AiModelsTab />);

    await waitFor(() =>
      expect(screen.getByTestId('ai-model-table_selection-vendor')).toBeInTheDocument(),
    );

    fireEvent.change(screen.getByTestId('ai-model-table_selection-vendor'), {
      target: { value: 'ollama' },
    });
    fireEvent.change(screen.getByTestId('ai-model-table_selection'), {
      target: { value: 'llama3' },
    });
    fireEvent.click(screen.getByTestId('ai-models-save'));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    const arg = mockSet.mock.calls[0][0];
    expect(arg.ai_models?.table_selection).toEqual({ vendor: 'ollama', model: 'llama3' });
    // Untouched roles still round-trip as plain strings.
    expect(arg.ai_models?.sql_generation).toBe('claude-opus-4-6');
  });

  it('shows "no key set" status and omits api_keys when the field is untouched', async () => {
    mockFetch.mockResolvedValue(state({ api_keys_set: { anthropic: false } }));
    mockSet.mockResolvedValue({ success: true, updated: [], restart_required: false });
    render(<AiModelsTab />);

    await waitFor(() =>
      expect(screen.getByTestId('ai-models-anthropic-key-status')).toBeInTheDocument(),
    );
    expect(screen.getByTestId('ai-models-anthropic-key-status')).toHaveTextContent(
      'No key set',
    );
    expect(screen.queryByTestId('ai-models-anthropic-key-clear')).not.toBeInTheDocument();

    fireEvent.click(screen.getByTestId('ai-models-save'));
    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    expect(mockSet.mock.calls[0][0].api_keys).toBeUndefined();
  });

  it('sends api_keys.anthropic when a new key is entered', async () => {
    mockFetch.mockResolvedValue(state({ api_keys_set: { anthropic: false } }));
    mockSet.mockResolvedValue({ success: true, updated: ['api_keys.anthropic'], restart_required: false });
    render(<AiModelsTab />);

    await waitFor(() =>
      expect(screen.getByTestId('ai-models-anthropic-key-input')).toBeInTheDocument(),
    );
    fireEvent.change(screen.getByTestId('ai-models-anthropic-key-input'), {
      target: { value: 'sk-ant-new-key' },
    });
    fireEvent.click(screen.getByTestId('ai-models-save'));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    expect(mockSet.mock.calls[0][0].api_keys?.anthropic).toBe('sk-ant-new-key');
  });

  it('shows "key is set" status with a clear option, and clearing sends an empty key', async () => {
    mockFetch.mockResolvedValue(state({ api_keys_set: { anthropic: true } }));
    mockSet.mockResolvedValue({ success: true, updated: ['api_keys.anthropic'], restart_required: false });
    render(<AiModelsTab />);

    await waitFor(() =>
      expect(screen.getByTestId('ai-models-anthropic-key-status')).toBeInTheDocument(),
    );
    expect(screen.getByTestId('ai-models-anthropic-key-status')).toHaveTextContent('Key is set');

    fireEvent.click(screen.getByTestId('ai-models-anthropic-key-clear'));
    fireEvent.click(screen.getByTestId('ai-models-save'));

    await waitFor(() => expect(mockSet).toHaveBeenCalledTimes(1));
    expect(mockSet.mock.calls[0][0].api_keys?.anthropic).toBe('');
  });

  it('offers the vendor\'s live model list as the model field\'s options, once per vendor', async () => {
    mockFetch.mockResolvedValue(state());
    mockVendorModels.mockResolvedValue(['claude-haiku-4-5-20251001', 'claude-opus-4-6']);
    render(<AiModelsTab />);

    await waitFor(() => expect(mockVendorModels).toHaveBeenCalledWith('anthropic'));
    // All five roles name anthropic, so its catalog is fetched once, not five times.
    expect(mockVendorModels).toHaveBeenCalledTimes(1);

    // Mantine filters the dropdown by the current input value, so clear it before opening.
    const input = screen.getByTestId('ai-model-sql_generation');
    fireEvent.change(input, { target: { value: '' } });
    fireEvent.click(input);
    // floating-ui hides the detached dropdown in jsdom (all rects are 0), so scope by the
    // input's aria-controls listbox and query with hidden: true.
    await waitFor(() => {
      if (!input.getAttribute('aria-controls')) throw new Error('dropdown not open');
    });
    const listbox = document.getElementById(input.getAttribute('aria-controls') as string);
    expect(
      within(listbox as HTMLElement)
        .getAllByRole('option', { hidden: true })
        .map((o) => o.textContent),
    ).toEqual(['claude-haiku-4-5-20251001', 'claude-opus-4-6']);
  });

  it('switching a role vendor loads that vendor\'s catalog', async () => {
    mockFetch.mockResolvedValue(state());
    mockVendorModels.mockResolvedValue([]);
    render(<AiModelsTab />);

    await waitFor(() => expect(mockVendorModels).toHaveBeenCalledWith('anthropic'));
    fireEvent.change(screen.getByTestId('ai-model-table_selection-vendor'), {
      target: { value: 'openai' },
    });
    await waitFor(() => expect(mockVendorModels).toHaveBeenCalledWith('openai'));
  });

  it('a vendor whose listing fails leaves the model field typeable and states why', async () => {
    mockFetch.mockResolvedValue(state());
    mockVendorModels.mockRejectedValue(new Error('set an API key for anthropic to list its models'));
    render(<AiModelsTab />);

    await waitFor(() =>
      expect(screen.getAllByText(/set an API key for anthropic/).length).toBe(5),
    );
    fireEvent.change(screen.getByTestId('ai-model-sql_generation'), {
      target: { value: 'some-custom-model' },
    });
    expect(screen.getByTestId('ai-model-sql_generation')).toHaveValue('some-custom-model');
  });
});
