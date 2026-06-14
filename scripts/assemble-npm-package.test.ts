import { expect, test } from 'bun:test'
import { mkdtempSync, readFileSync, rmSync, statSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { assembleNpmPackage, NPM_PACKAGE_NAME } from './assemble-npm-package.ts'

// comms-iw8w.4: the publishable npm artifact for `npx @codeforbreakfast/commy-mcp`.
// These lock the repo-side contract the (fleet-local) release-plugin skill calls
// before `npm publish`; the publish itself stays Graeme's local, authed step.

const REPO_ROOT = join(import.meta.dir, '..')

const PLUGIN_VERSION = (
  JSON.parse(
    readFileSync(
      join(REPO_ROOT, 'clients', 'claude-code', '.claude-plugin', 'plugin.json'),
      'utf8',
    ),
  ) as { readonly version: string }
).version

test('assembles a dependency-free @codeforbreakfast/commy-mcp whose version tracks plugin.json', () => {
  const out = mkdtempSync(join(tmpdir(), 'commy-npm-'))
  try {
    const manifest = assembleNpmPackage(REPO_ROOT, out)

    // The 'commy' brand is unavailable as an npm scope, so the one published
    // surface carries @codeforbreakfast (Graeme's naming ruling).
    expect(manifest.name).toBe('@codeforbreakfast/commy-mcp')
    expect(NPM_PACKAGE_NAME).toBe('@codeforbreakfast/commy-mcp')
    // Version is generated from plugin.json — the head of the six-site
    // lockstep (manifests.test.ts) — so it can never drift from a release.
    expect(manifest.version).toBe(PLUGIN_VERSION)

    const onDisk = JSON.parse(readFileSync(join(out, 'package.json'), 'utf8')) as Record<
      string,
      unknown
    >
    expect(onDisk['name']).toBe('@codeforbreakfast/commy-mcp')
    expect(onDisk['version']).toBe(PLUGIN_VERSION)
    expect(onDisk['type']).toBe('module')
    // `bin` as a string names the binary after the unscoped package
    // (`commy-mcp`), so `npx @codeforbreakfast/commy-mcp` runs the server.
    expect(onDisk['bin']).toBe('./server.js')
    expect(onDisk['files']).toEqual(['server.js'])
    expect(onDisk['publishConfig']).toEqual({ access: 'public' })
    // The bundle inlines every dependency — the published package has none.
    expect(onDisk['dependencies']).toBeUndefined()
    expect(onDisk['devDependencies']).toBeUndefined()
  } finally {
    rmSync(out, { recursive: true, force: true })
  }
})

test('the staged server.js is a node-shebang bin that loads and runs under node', () => {
  const out = mkdtempSync(join(tmpdir(), 'commy-npm-'))
  try {
    assembleNpmPackage(REPO_ROOT, out)
    const serverOut = join(out, 'server.js')

    const bundle = readFileSync(serverOut, 'utf8')
    // A node shebang makes the file runnable as the package bin.
    expect(bundle.startsWith('#!/usr/bin/env node\n')).toBe(true)
    expect(statSync(serverOut).size).toBeGreaterThan(500_000)

    // Run it under node with no ZULIP_* env: it loads the whole bundle and
    // reaches env validation — proving node runs the published artifact. The
    // sibling `type: module` package.json means no MODULE_TYPELESS reparse.
    const run = Bun.spawnSync(['node', serverOut], {
      env: { PATH: process.env['PATH'] ?? '' },
      stdin: Buffer.from(''),
      stdout: 'pipe',
      stderr: 'pipe',
    })
    const output = run.stdout.toString() + run.stderr.toString()
    expect(run.exitCode).not.toBe(0)
    expect(output).toContain('EnvConfigError')
    expect(run.stderr.toString()).not.toContain('MODULE_TYPELESS_PACKAGE_JSON')
  } finally {
    rmSync(out, { recursive: true, force: true })
  }
})
