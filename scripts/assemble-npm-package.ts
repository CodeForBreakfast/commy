import { execFileSync } from 'node:child_process'
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'

// The published npm package name. The 'commy' brand is the default everywhere
// except where it clashes — npm is the one such surface, so the published
// package alone carries the @codeforbreakfast scope (Graeme's naming ruling).
export const NPM_PACKAGE_NAME = '@codeforbreakfast/commy-mcp'

// A node shebang on the bundle so `npx @codeforbreakfast/commy-mcp` (which runs
// the package bin) executes it under node directly.
const NODE_SHEBANG = '#!/usr/bin/env node\n'

export interface PublishManifest {
  readonly name: string
  readonly version: string
  readonly description: string
  readonly license: string
  readonly type: 'module'
  readonly bin: string
  readonly files: ReadonlyArray<string>
  readonly publishConfig: { readonly access: 'public' }
  readonly author: { readonly name: string; readonly email: string }
  readonly repository: { readonly type: 'git'; readonly url: string }
}

// The canonical version source — the same plugin.json release.yml verifies the
// pushed tag against, and the head of the six-site lockstep
// (clients/claude-code/manifests.test.ts). Generating the published version
// from it means the registry artifact can never drift from a release.
function pluginVersion(repoRoot: string): string {
  const manifest = JSON.parse(
    readFileSync(join(repoRoot, 'clients', 'claude-code', '.claude-plugin', 'plugin.json'), 'utf8'),
  ) as { readonly version: string }
  return manifest.version
}

function publishManifest(version: string): PublishManifest {
  return {
    name: NPM_PACKAGE_NAME,
    version,
    description:
      'commy MCP server — the inter-agent communication substrate, runnable on node via npx.',
    license: 'Apache-2.0',
    type: 'module',
    bin: './server.js',
    files: ['server.js'],
    publishConfig: { access: 'public' },
    author: { name: 'Code For Breakfast', email: 'info@codeforbreakfast.co' },
    repository: { type: 'git', url: 'https://github.com/CodeForBreakfast/commy.git' },
  }
}

// Assemble a publishable npm package at outDir: the node-target server bundle
// (deps inlined, prefixed with a node shebang so it runs as the
// package bin) plus a generated, dependency-free package.json whose version
// tracks plugin.json. `npm publish <outDir>` ships it. bun is the build tool
// here, not a runtime the consumer needs.
export function assembleNpmPackage(repoRoot: string, outDir: string): PublishManifest {
  mkdirSync(outDir, { recursive: true })

  const serverOut = join(outDir, 'server.js')
  execFileSync(
    process.execPath,
    [
      'build',
      join(repoRoot, 'packages', 'mcp', 'server.ts'),
      '--target=node',
      '--outfile',
      serverOut,
    ],
    { cwd: repoRoot, stdio: 'inherit' },
  )

  const bundled = readFileSync(serverOut, 'utf8')
  if (!bundled.startsWith('#!')) {
    writeFileSync(serverOut, NODE_SHEBANG + bundled)
  }

  const manifest = publishManifest(pluginVersion(repoRoot))
  writeFileSync(join(outDir, 'package.json'), `${JSON.stringify(manifest, null, 2)}\n`)
  return manifest
}

if (import.meta.main) {
  const repoRoot = process.argv[2] ?? join(import.meta.dir, '..')
  const outDir = process.argv[3] ?? join(repoRoot, 'packages', 'mcp', 'dist')
  console.error(`assemble-npm-package: ${repoRoot} -> ${outDir}`)
  const manifest = assembleNpmPackage(repoRoot, outDir)
  console.error(`assemble-npm-package: staged ${manifest.name}@${manifest.version} at ${outDir}`)
  console.error(`assemble-npm-package: publish with \`npm publish ${outDir}\``)
}
