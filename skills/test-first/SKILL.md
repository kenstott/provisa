---
name: test-first
description: Red-green-refactor TDD discipline. Auto-triggers when implementing new features or fixing bugs.
---

# Test-First Development

## Red-Green-Refactor Cycle
1. **RED** — Write a failing test that defines the desired behavior
2. **GREEN** — Write the minimum code to make it pass
3. **REFACTOR** — Clean up while keeping tests green

## Rules
- Never skip the red step. If the test passes immediately, it's not testing anything new.
- Write the test BEFORE the implementation.
- Each cycle should take minutes, not hours.
- Commit after each green step.

## When to Apply
- New features: write acceptance test first
- Bug fixes: write a test that reproduces the bug first
- Refactoring: ensure characterization tests exist before changing structure

## Anti-Patterns
- Writing tests after implementation (tests rubber-stamp existing behavior)
- Writing too many tests at once (lose the feedback loop)
- Making the test pass by hardcoding the expected value
- Skipping refactor step (accumulates tech debt)
