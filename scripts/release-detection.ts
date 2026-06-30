import { appendFileSync, existsSync, readFileSync } from 'node:fs'
import { join } from 'node:path'

// The tag that records a published release. `commy-vX.Y.Z` is what
// release.yml's verify step matches against plugin.json and what the npm
// Trusted Publisher run is anchored to. Keep the prefix in lockstep with
// the `commy-v*` glob the workflow and the existing tag history use.
export const RELEASE_TAG_PREFIX = 'commy-v'

const SEMVER_SHAPE = /^\d+\.\d+\.\d+$/

export function releaseTagName(version: string): string {
  return `${RELEASE_TAG_PREFIX}${version}`
}

// The canonical version source — `clients/claude-code/.claude-plugin/plugin.json`,
// the head of the seven-site lockstep (clients/claude-code/manifests.test.ts).
// Reading the version from it here means the tag, the npm artifact, and the
// GitHub Release all derive from the one truth the lockstep test guards.
export function extractVersion(pluginManifestText: string): string {
  const manifest = JSON.parse(pluginManifestText) as { readonly version?: unknown }
  const version = manifest.version
  if (typeof version !== 'string' || !SEMVER_SHAPE.test(version)) {
    throw new Error(
      `plugin.json version is not a MAJOR.MINOR.PATCH string: ${JSON.stringify(version)}`,
    )
  }
  return version
}

export interface ReleaseDecisionInputs {
  readonly notesFileExists: boolean
  readonly tagExists: boolean
  readonly force: boolean
}

export interface ReleaseDecision {
  readonly release: boolean
  readonly reason: string
}

// B2 release detection. A push to main is a release iff the bump PR wrote a
// checked-in RELEASE-NOTES/<version>.md (the worker's opt-in to automation)
// and the version isn't already tagged (idempotency — every later push to main
// re-runs detection, and the existing tag is what stops a re-publish). `force`
// (workflow_dispatch) bypasses the already-tagged guard to recover a release
// that tagged but failed before publishing; it still requires the notes file.
export function decideRelease({
  notesFileExists,
  tagExists,
  force,
}: ReleaseDecisionInputs): ReleaseDecision {
  if (!notesFileExists) {
    return {
      release: false,
      reason:
        'no RELEASE-NOTES/<version>.md for this version — not an automated release (or the bump PR omitted the notes file)',
    }
  }
  if (tagExists && !force) {
    return {
      release: false,
      reason: 'version is already tagged — already released (re-run with force to recover)',
    }
  }
  return {
    release: true,
    reason: force && tagExists ? 'forced re-run of an already-tagged version' : 'release commit',
  }
}

const PLUGIN_MANIFEST_PATH = join('clients', 'claude-code', '.claude-plugin', 'plugin.json')

function notesFilePath(version: string): string {
  return join('RELEASE-NOTES', `${version}.md`)
}

function tagExistsOnRemote(tag: string): boolean {
  const result = Bun.spawnSync(['git', 'ls-remote', '--tags', 'origin', `refs/tags/${tag}`])
  if (result.exitCode !== 0) {
    throw new Error(`git ls-remote failed: ${result.stderr.toString()}`)
  }
  return result.stdout.toString().trim().length > 0
}

if (import.meta.main) {
  const version = extractVersion(readFileSync(PLUGIN_MANIFEST_PATH, 'utf8'))
  const tag = releaseTagName(version)
  const decision = decideRelease({
    notesFileExists: existsSync(notesFilePath(version)),
    tagExists: tagExistsOnRemote(tag),
    force: process.env['FORCE'] === 'true',
  })

  console.error(`release-detection: version=${version} tag=${tag} -> ${decision.reason}`)

  const githubOutput = process.env['GITHUB_OUTPUT']
  if (githubOutput) {
    appendFileSync(githubOutput, `release=${decision.release}\nversion=${version}\ntag=${tag}\n`)
  }
}
