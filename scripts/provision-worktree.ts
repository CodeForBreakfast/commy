import { dirname, resolve } from 'node:path'

// Every fleet seat works in `.worktrees/<bead-id>`, and setting that up by hand
// has three failure modes that are all invisible until much later: branching
// off a local `main` that has gone stale behind `origin/main`, skipping `bun
// install` so `@commy/*` resolves up to the main checkout's `node_modules`, and
// doing either of those from the wrong directory. This module is the whole
// setup as one command — `bun run worktree <bead-id>` — so it stops being a
// list of things a brief has to remember to say.

// Bead ids look like `comms-uejm` or `comms-gh88.4b`. Anything else is a typo
// or an injection, and either way it must not reach a git command line or
// become a path segment.
const BEAD_ID_SHAPE = /^[a-z0-9]+(?:[-.][a-z0-9]+)*$/

export type StepDirectory = 'main-checkout' | 'worktree'

export interface ProvisionStep {
  readonly command: readonly string[]
  readonly cwd: StepDirectory
}

export interface WorktreeProvisionPlan {
  readonly branch: string
  readonly worktreePath: string
  readonly steps: readonly ProvisionStep[]
}

export function worktreeProvisionPlan(beadId: string): WorktreeProvisionPlan {
  if (!BEAD_ID_SHAPE.test(beadId)) {
    throw new Error(
      `not a bead id: ${JSON.stringify(beadId)} — expected lowercase alphanumerics separated by '-' or '.', e.g. comms-uejm`,
    )
  }

  const worktreePath = `.worktrees/${beadId}`

  return {
    branch: beadId,
    worktreePath,
    steps: [
      // Without this the next step branches off whatever `origin/main` pointed
      // at the last time anyone fetched, which is routinely several merges old.
      { command: ['git', 'fetch', 'origin'], cwd: 'main-checkout' },
      {
        command: ['git', 'worktree', 'add', worktreePath, '-b', beadId, 'origin/main'],
        cwd: 'main-checkout',
      },
      // A worktree starts with no `node_modules`, so bun walks up and resolves
      // the workspace packages against the main checkout — the same names
      // bound to another branch's source.
      { command: ['bun', 'install', '--no-summary'], cwd: 'worktree' },
    ],
  }
}

// `git rev-parse --git-common-dir` reports the main checkout's `.git` — absolute
// from inside a worktree, relative from inside the main checkout itself. Both
// have to land on the checkout that owns `.worktrees/`, so that provisioning
// works whichever of the two the seat has already moved into.
export function mainCheckoutRoot(commonGitDir: string, reportedFrom: string): string {
  return dirname(resolve(reportedFrom, commonGitDir))
}

export interface RunnableStep extends ProvisionStep {
  readonly directory: string
}

export interface ProvisionOutcome {
  readonly exitCode: number
  readonly worktreeDirectory: string
  readonly failedStep?: ProvisionStep
}

export function provisionWorktree(
  beadId: string,
  mainCheckout: string,
  runStep: (step: RunnableStep) => number,
): ProvisionOutcome {
  const plan = worktreeProvisionPlan(beadId)
  const worktreeDirectory = resolve(mainCheckout, plan.worktreePath)

  for (const step of plan.steps) {
    const directory = step.cwd === 'worktree' ? worktreeDirectory : mainCheckout
    const exitCode = runStep({ ...step, directory })
    if (exitCode !== 0) {
      return { exitCode, worktreeDirectory, failedStep: step }
    }
  }

  return { exitCode: 0, worktreeDirectory }
}

function locateMainCheckout(): string {
  const result = Bun.spawnSync(['git', 'rev-parse', '--git-common-dir'])
  if (result.exitCode !== 0) {
    throw new Error(`not inside a git checkout: ${result.stderr.toString().trim()}`)
  }
  return mainCheckoutRoot(result.stdout.toString().trim(), process.cwd())
}

if (import.meta.main) {
  const beadId = process.argv[2]
  if (!beadId) {
    console.error('usage: bun run worktree <bead-id>')
    process.exit(2)
  }

  const mainCheckout = locateMainCheckout()
  const outcome = provisionWorktree(beadId, mainCheckout, (step) => {
    console.error(`+ ${step.command.join(' ')}`)
    return (
      Bun.spawnSync([...step.command], {
        cwd: step.directory,
        stdout: 'inherit',
        stderr: 'inherit',
      }).exitCode ?? 1
    )
  })

  if (outcome.failedStep) {
    console.error(`worktree provisioning failed at: ${outcome.failedStep.command.join(' ')}`)
    process.exit(outcome.exitCode)
  }

  console.error(`worktree ready — cd ${outcome.worktreeDirectory}`)
}
