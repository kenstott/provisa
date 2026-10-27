// Copyright (c) 2026 Kenneth Stott
// Canary: c259a6ef-3056-4f2e-9637-89f26f5f259f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The live model the e2e backends' chat reaches: Haiku through OpenRouter, an OpenAI-style
// endpoint named in ai_endpoints and chosen for mcp_chat in ai_models (REQ-1790, REQ-1797). It is
// the e2e config's, appended to the install config each backend boots from and is reloaded with;
// the product's own defaults are not changed. The key is the runner's OPENROUTER_API_KEY.
export const LIVE_MODEL_YAML = `
ai_endpoints:
  - id: openrouter
    style: openai
    base_url: https://openrouter.ai/api/v1
    api_key_env: OPENROUTER_API_KEY
    enabled: true
ai_models:
  mcp_chat:
    vendor: openrouter
    model: anthropic/claude-haiku-5.5
`;
