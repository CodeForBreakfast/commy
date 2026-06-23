import { expect, test } from 'bun:test'

/**
 * The plugin ships substrate-general etiquette as the `using-commy`
 * skill — opt-in guidance on how to communicate *well* on commy, layered
 * on top of the always-on mechanics the MCP server echoes via its
 * `instructions:` field (comms-tonj). Lock the frontmatter contract
 * Claude Code resolves the skill by, and assert it covers the etiquette
 * the always-on block deliberately leaves out.
 */

const skillSource = await Bun.file(new URL('./skills/using-commy/SKILL.md', import.meta.url)).text()

const frontmatter = (() => {
  const block = skillSource.match(/^---\n([\s\S]*?)\n---/)?.[1]
  if (block === undefined) {
    throw new Error('using-commy SKILL.md is missing a frontmatter block')
  }
  return Bun.YAML.parse(block) as { readonly name?: string; readonly description?: string }
})()

test('using-commy skill declares the name Claude Code resolves it by', () => {
  expect(frontmatter.name).toBe('using-commy')
})

test('using-commy skill carries a non-empty description for discovery', () => {
  expect(frontmatter.description).toBeDefined()
  expect(frontmatter.description?.length ?? 0).toBeGreaterThan(0)
})

test('using-commy skill covers the etiquette the always-on instructions block does not', () => {
  expect(skillSource).toMatch(/autobiography/i)
  expect(skillSource).toMatch(/react/i)
  expect(skillSource).toMatch(/mention/i)
})
