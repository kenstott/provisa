---
name: skill-design-template
description: Template and rules for writing new SKILL.md files in this project. Auto-triggers when creating or reviewing skill definitions.
---

# Skill Design Template

## File Location

```
.claude/skills/<skill-name>/SKILL.md
```

One skill per directory. Directory name = skill name (kebab-case).

## Required Frontmatter

```markdown
---
name: skill-name
description: One sentence — what this skill contains and when it auto-triggers.
---
```

The `description` field is what Claude reads to decide whether to load the skill. Make it specific: name the domain, name the trigger condition. Vague descriptions ("general Python help") are never selected.

**Auto-trigger phrasing** (use one):
- `Auto-triggers when working with <domain>.`
- `Auto-triggers when writing or reviewing <type> code.`
- `Auto-triggers when <agent type> is invoked.`

## Content Rules

**What belongs in a skill:**
- Domain facts the agent can't derive from reading code (protocols, invariants, naming conventions, design decisions)
- Repeatable workflows (step sequences that are always the same)
- Anti-patterns specific to this codebase
- Concrete examples with real module paths

**What does NOT belong:**
- General Python knowledge (agents already know this)
- Anything already in `CLAUDE.md`
- Implementation code (link to the file instead)
- Anything derivable by reading the current source

## Length Budget

| Skill type | Target length |
|------------|--------------|
| Protocol/contract | 300–500 words |
| Workflow/process | 400–600 words |
| Domain model | 500–800 words |

Skills over 800 words are usually two skills. Split on the axis of "which agent needs this."

## Format

- Use `##` sections with descriptive headers
- Code blocks with language tags for all code
- Tables for comparisons (good/bad, option A/B)
- Bullet lists for enumerated rules
- No prose paragraphs longer than 3 sentences

## Naming Conventions

| Skill name | Use for |
|------------|---------|
| `<domain>-design` | Architecture and design constraints |
| `<domain>-patterns` | Repeatable recipes and anti-patterns |
| `<domain>-debugging` | Diagnostic techniques for a domain |
| `<domain>-test-patterns` | Test strategies for a domain |
| `<protocol>-compliance` | What correct implementations look like |
| `<process>-template` | Workflow or document templates |

## Agent Mapping

Indicate which agent(s) should use this skill in the description. Skills used by multiple agents should be general; skills for a single agent can be opinionated.

## Verification Before Publishing

- [ ] Frontmatter is valid YAML
- [ ] Description is specific enough to trigger correctly
- [ ] All module paths verified to exist in current codebase
- [ ] No implementation code (code *examples* are fine; don't paste entire functions)
- [ ] Under 800 words
