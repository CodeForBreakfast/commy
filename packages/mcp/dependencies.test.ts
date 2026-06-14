import { expect, test } from 'bun:test'
import { readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'

// The mcp server is published as a self-contained node bundle: `bun build
// --target=node` (scripts/assemble-npm-package.ts) inlines every import into a
// single server.js. The bundler can only inline an import it can resolve from
// the dev workspace, and an import resolves there iff it is declared either in
// this package's own dependencies (the `@commy/*` workspace siblings) or in the
// workspace-root dependencies (the third-party packages — effect, @effect/*,
// @modelcontextprotocol/sdk). An import declared in neither place might resolve
// transitively by luck today and vanish from the bundle tomorrow, so this guards
// that every runtime import is a first-class declared dependency.

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
