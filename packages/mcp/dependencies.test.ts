import { expect, test } from 'bun:test'
import { readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'

// The mcp server is published inside a frozen whole-workspace tree
// (scripts/publish-marketplace.ts runs `bun install --frozen-lockfile` at the
// frozen workspace root). At runtime a package is present in the frozen
// node_modules iff it is declared either in this package's own dependencies
// (the `@commy/*` workspace siblings) or in the workspace-root
// dependencies (the hoisted third-party packages — effect, @effect/*,
// @modelcontextprotocol/sdk). Any runtime import declared in neither place
// resolves in dev but is absent from the frozen node_modules, crashing the
// server on launch. This guards that gap.

const mcpDir = import.meta.dir
const workspaceRoot = join(mcpDir, '..', '..')
const transpiler = new Bun.Transpiler({ loader: 'ts' })

function packageNameOf(specifier: string): string | null {
  if (specifier.startsWith('node:') || specifier.startsWith('bun:') || specifier.startsWith('.')) {
    return null
  }
  const [first, second] = specifier.split('/')
  if (first === undefined) return null
  return first.startsWith('@') ? `${first}/${second ?? ''}` : first
}

function externalPackagesImportedBy(source: string): ReadonlySet<string> {
  const packages = new Set<string>()
  for (const { path } of transpiler.scanImports(source)) {
    const name = packageNameOf(path)
    if (name !== null) packages.add(name)
  }
  return packages
}

const runtimeSources = readdirSync(mcpDir).filter(
  (name) => name.endsWith('.ts') && !name.endsWith('.test.ts'),
)

function declaredDependenciesOf(packageJsonPath: string): ReadonlySet<string> {
  return new Set(
    Object.keys(
      (
        JSON.parse(readFileSync(packageJsonPath, 'utf8')) as {
          dependencies?: Record<string, string>
        }
      ).dependencies ?? {},
    ),
  )
}

const resolvableDependencies = new Set([
  ...declaredDependenciesOf(join(mcpDir, 'package.json')),
  ...declaredDependenciesOf(join(workspaceRoot, 'package.json')),
])

test('every external package the runtime imports is a declared dependency', () => {
  const undeclared = new Set<string>()
  for (const file of runtimeSources) {
    for (const pkg of externalPackagesImportedBy(readFileSync(join(mcpDir, file), 'utf8'))) {
      if (!resolvableDependencies.has(pkg)) undeclared.add(pkg)
    }
  }
  expect([...undeclared].sort()).toEqual([])
})
