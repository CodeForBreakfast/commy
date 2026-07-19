import { expect, test } from 'bun:test'
import { mainCheckoutRoot, provisionWorktree, worktreeProvisionPlan } from './provision-worktree.ts'

// Three things went wrong every time a fleet seat set a worktree up by hand:
// it branched off a stale local main, it skipped `bun install` (so `@commy/*`
// silently resolved up to the main checkout's node_modules), and it ran the
// whole thing from wherever it happened to be. The plan below is the whole
// surface — these tests pin the parts a brief used to have to remember to say.

test('the plan fetches origin before it branches, so the branch point is never stale', () => {
  const plan = worktreeProvisionPlan('comms-uejm')

  expect(plan.steps[0]?.command).toEqual(['git', 'fetch', 'origin'])
})

test('the plan branches off origin/main, not the local main that goes stale', () => {
  const plan = worktreeProvisionPlan('comms-uejm')
  const add = plan.steps[1]

  expect(add?.command).toEqual([
    'git',
    'worktree',
    'add',
    '.worktrees/comms-uejm',
    '-b',
    'comms-uejm',
    'origin/main',
  ])
  expect(add?.cwd).toBe('main-checkout')
})

test('the plan installs inside the new worktree, so @commy/* resolves there', () => {
  const plan = worktreeProvisionPlan('comms-uejm')
  const install = plan.steps[2]

  expect(install?.command).toEqual(['bun', 'install', '--no-summary'])
  expect(install?.cwd).toBe('worktree')
})

test('the plan names the branch and the worktree path after the bead', () => {
  const plan = worktreeProvisionPlan('comms-gh88.4b')

  expect(plan.branch).toBe('comms-gh88.4b')
  expect(plan.worktreePath).toBe('.worktrees/comms-gh88.4b')
})

test('the plan is exactly these three steps — nothing a seat has to add by hand', () => {
  expect(worktreeProvisionPlan('comms-uejm').steps).toHaveLength(3)
})

test('a bead id that is not a bead id is rejected rather than pasted into a git command', () => {
  expect(() => worktreeProvisionPlan('')).toThrow()
  expect(() => worktreeProvisionPlan('../escape')).toThrow()
  expect(() => worktreeProvisionPlan('comms uejm')).toThrow()
  expect(() => worktreeProvisionPlan('comms-uejm; rm -rf /')).toThrow()
  expect(() => worktreeProvisionPlan('--force')).toThrow()
  expect(() => worktreeProvisionPlan('COMMS-UEJM')).toThrow()
})

// A seat is born in the main checkout and only ever `cd`s into a worktree
// afterwards (the launch cwd carries the Claude config and cannot be re-run).
// Provisioning still has to work once the seat has moved, so the main checkout
// is derived from git's common dir rather than assumed to be the cwd.
// `git rev-parse --git-common-dir` answers absolutely from inside a worktree
// and relatively from inside the main checkout, so both shapes have to land on
// the same place.
test('the main checkout is the parent of the common git dir, so it holds from inside a worktree', () => {
  expect(mainCheckoutRoot('/home/graeme/Development/commy/.git', '/anywhere')).toBe(
    '/home/graeme/Development/commy',
  )
})

test('a relative common git dir resolves against the directory it was reported from', () => {
  expect(mainCheckoutRoot('.git', '/home/graeme/Development/commy')).toBe(
    '/home/graeme/Development/commy',
  )
  expect(mainCheckoutRoot('../.git', '/home/graeme/Development/commy/scripts')).toBe(
    '/home/graeme/Development/commy',
  )
})

test('provisioning runs every step in order and stops at the first failure', () => {
  const ran: string[] = []
  const outcome = provisionWorktree('comms-uejm', '/repo', (step) => {
    ran.push(`${step.cwd}:${step.command.join(' ')}`)
    return step.command[0] === 'git' && step.command[1] === 'worktree' ? 128 : 0
  })

  expect(ran).toEqual([
    'main-checkout:git fetch origin',
    'main-checkout:git worktree add .worktrees/comms-uejm -b comms-uejm origin/main',
  ])
  expect(outcome.exitCode).toBe(128)
  expect(outcome.failedStep?.command).toContain('worktree')
})

test('a successful provision reports the worktree the seat should cd into', () => {
  const outcome = provisionWorktree('comms-uejm', '/repo', () => 0)

  expect(outcome.exitCode).toBe(0)
  expect(outcome.failedStep).toBeUndefined()
  expect(outcome.worktreeDirectory).toBe('/repo/.worktrees/comms-uejm')
})

test('each step is handed the absolute directory it must run in', () => {
  const directories: string[] = []
  provisionWorktree('comms-uejm', '/repo', (step) => {
    directories.push(step.directory)
    return 0
  })

  expect(directories).toEqual(['/repo', '/repo', '/repo/.worktrees/comms-uejm'])
})
