import { expect, test } from 'bun:test'
import {
  decideRelease,
  extractVersion,
  RELEASE_TAG_PREFIX,
  releaseTagName,
} from './release-detection.ts'

// release.yml drives the B2 path off these pure functions: extractVersion reads
// the canonical version, releaseTagName builds the record tag, decideRelease is
// the release-commit gate. The workflow runs this same module (bun
// scripts/release-detection.ts), so these cover what actually fires in CI.

test('releaseTagName builds a commy-v tag from a version', () => {
  expect(releaseTagName('0.16.0')).toBe('commy-v0.16.0')
  expect(releaseTagName('1.2.3')).toBe(`${RELEASE_TAG_PREFIX}1.2.3`)
})

test('extractVersion reads the version from a plugin.json string', () => {
  expect(extractVersion('{"version":"0.16.0"}')).toBe('0.16.0')
  expect(extractVersion('{ "name": "commy", "version": "1.20.300" }')).toBe('1.20.300')
})

test('extractVersion rejects a non-semver version', () => {
  expect(() => extractVersion('{"version":"0.16"}')).toThrow()
  expect(() => extractVersion('{"version":"v0.16.0"}')).toThrow()
  expect(() => extractVersion('{"version":"latest"}')).toThrow()
})

test('extractVersion rejects a missing or non-string version', () => {
  expect(() => extractVersion('{}')).toThrow()
  expect(() => extractVersion('{"version":160}')).toThrow()
})

test('decideRelease releases a release commit: notes present, not yet tagged', () => {
  const decision = decideRelease({ notesFileExists: true, tagExists: false, force: false })
  expect(decision.release).toBe(true)
})

test('decideRelease skips when there is no notes file (an ordinary main push)', () => {
  const decision = decideRelease({ notesFileExists: false, tagExists: false, force: false })
  expect(decision.release).toBe(false)
  expect(decision.reason).toContain('not an automated release')
})

test('decideRelease skips an already-tagged version (idempotent re-runs on later pushes)', () => {
  const decision = decideRelease({ notesFileExists: true, tagExists: true, force: false })
  expect(decision.release).toBe(false)
  expect(decision.reason).toContain('already released')
})

test('decideRelease force re-releases an already-tagged version (partial-failure recovery)', () => {
  const decision = decideRelease({ notesFileExists: true, tagExists: true, force: true })
  expect(decision.release).toBe(true)
})

test('decideRelease force still requires the notes file', () => {
  const decision = decideRelease({ notesFileExists: false, tagExists: true, force: true })
  expect(decision.release).toBe(false)
})
