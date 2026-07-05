import { beforeAll, describe, expect, it } from 'bun:test'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'

// The model-audit workflow runs as a sandbox-wrapped function body (top-level return + injected
// globals), so it has no import seam. We exercise the REAL source by extracting the marker-delimited
// declarations (LENSES / assertPartition / splitSurvivors) and evaluating each in isolation — the
// same technique effect-native-audit-split.test.ts uses for splitSurvivors.
const WORKFLOW_PATH = join(
  import.meta.dir,
  '..',
  '.claude',
  'skills',
  'effect-model-audit',
  'effect-model-audit.workflow.js',
)

function extractBlock(source: string, begin: string, end: string): string {
  const start = source.indexOf(begin)
  const stop = source.indexOf(end)
  if (start === -1 || stop === -1) {
    throw new Error(`marker comments not found in the workflow source: ${begin} .. ${end}`)
  }
  return source.slice(start, stop)
}

type Lens = {
  id: string
  title: string
  question: string
  prompt: string
  boundary?: string
  grep?: unknown
}

type Unit = { key: string; members: string[] }
type PartitionReport = {
  ok: boolean
  missing: string[]
  duplicated: string[]
  unexpected: string[]
}

type Finding = { file: string; line: number; confidence: string; unit?: string; lens?: string }
type SplitResult = { confirmed: Finding[]; lowConfidence: Finding[] }

let source: string
let LENSES: Lens[]
let assertPartition: (exported: string[], units: Unit[]) => PartitionReport
let splitSurvivors: (survivors: Finding[]) => SplitResult

beforeAll(() => {
  source = readFileSync(WORKFLOW_PATH, 'utf8')
  LENSES = new Function(
    `${extractBlock(source, '// --- LENSES:', '// --- end LENSES ---')}\nreturn LENSES`,
  )() as Lens[]
  assertPartition = new Function(
    `${extractBlock(source, '// --- assertPartition:', '// --- end assertPartition ---')}\nreturn assertPartition`,
  )() as typeof assertPartition
  splitSurvivors = new Function(
    `${extractBlock(source, '// --- splitSurvivors:', '// --- end splitSurvivors ---')}\nreturn splitSurvivors`,
  )() as typeof splitSurvivors
})

describe('LENSES — the value-space lens catalogue', () => {
  it('holds exactly the four orthogonal lenses L1–L4', () => {
    expect(LENSES.map((l) => l.id)).toEqual(['L1', 'L2', 'L3', 'L4'])
  })

  it('gives every lens a non-empty title, question, and a substantial reasoning prompt', () => {
    for (const lens of LENSES) {
      expect(lens.title.length).toBeGreaterThan(0)
      expect(lens.question.length).toBeGreaterThan(0)
      // a REASONING prompt, not a stub — the whole divergence from the sibling is that a lens
      // reasons rather than matches a token.
      expect(lens.prompt.length).toBeGreaterThan(200)
    }
  })

  it('carries NO grep anchor on any lens — a lens is a question, not a pattern', () => {
    // The core design divergence from the anchor-driven sibling: a cell is unit×lens and the agent
    // REASONS. A `grep` field would reintroduce the lexical anchor that structurally missed ro14.
    for (const lens of LENSES) {
      expect(lens.grep).toBeUndefined()
    }
  })

  it('states an explicit boundary on the lenses that border another reasoning path', () => {
    // L3 overlaps L1 by design (census path vs isolation path); L4 borders the sibling's
    // unvalidated-boundary / internal-bridge. Both must declare it so synthesis does not double-count.
    const boundaried = new Map(LENSES.map((l) => [l.id, l.boundary]))
    expect(boundaried.get('L3')?.length ?? 0).toBeGreaterThan(0)
    expect(boundaried.get('L4')?.length ?? 0).toBeGreaterThan(0)
  })
})

describe('assertPartition — the coverage-equation invariant', () => {
  const exported = ['ChannelRef', 'ObservedThread', 'MessageRef', 'Identity']

  it('accepts a clean partition — every exported type in exactly one unit', () => {
    const units: Unit[] = [
      { key: 'address-observation', members: ['ChannelRef', 'ObservedThread', 'MessageRef'] },
      { key: 'identity', members: ['Identity'] },
    ]
    const report = assertPartition(exported, units)
    expect(report.ok).toBe(true)
    expect(report.missing).toEqual([])
    expect(report.duplicated).toEqual([])
    expect(report.unexpected).toEqual([])
  })

  it('flags an exported type assigned to no unit as missing', () => {
    const units: Unit[] = [{ key: 'refs', members: ['ChannelRef', 'ObservedThread', 'MessageRef'] }]
    const report = assertPartition(exported, units)
    expect(report.ok).toBe(false)
    expect(report.missing).toContain('Identity')
  })

  it('flags an exported type assigned to two units as duplicated', () => {
    const units: Unit[] = [
      { key: 'a', members: ['ChannelRef', 'ObservedThread', 'MessageRef', 'Identity'] },
      { key: 'b', members: ['Identity'] },
    ]
    const report = assertPartition(exported, units)
    expect(report.ok).toBe(false)
    expect(report.duplicated).toContain('Identity')
  })

  it('flags a unit member that is not an exported domain type as unexpected', () => {
    const units: Unit[] = [
      { key: 'a', members: ['ChannelRef', 'ObservedThread', 'MessageRef', 'Identity', 'Phantom'] },
    ]
    const report = assertPartition(exported, units)
    expect(report.ok).toBe(false)
    expect(report.unexpected).toContain('Phantom')
  })
})

describe('splitSurvivors — deterministic dedup + confidence partition', () => {
  it('routes high-confidence to confirmed and the rest to lowConfidence', () => {
    const high: Finding = { file: 'core/ports.ts', line: 154, confidence: 'high', lens: 'L3' }
    const low: Finding = { file: 'core/ports.ts', line: 194, confidence: 'low', lens: 'L1' }
    const { confirmed, lowConfidence } = splitSurvivors([high, low])
    expect(confirmed).toEqual([high])
    expect(lowConfidence).toEqual([low])
  })

  it('dedups by (file,line) keeping the highest-confidence copy, never duplicating across arrays', () => {
    // The L1/L3 overlap-by-design: the same site reached by both the isolation path and the census
    // path. Deduped by file:line, a high copy wins and it appears in exactly one array.
    const high: Finding = { file: 'core/ports.ts', line: 194, confidence: 'high', lens: 'L3' }
    const low: Finding = { file: 'core/ports.ts', line: 194, confidence: 'low', lens: 'L1' }
    const { confirmed, lowConfidence } = splitSurvivors([low, high])
    expect(confirmed).toEqual([high])
    expect(lowConfidence).toEqual([])
  })

  it('treats any non-high confidence value as low', () => {
    const survivors: Finding[] = [
      { file: 'core/ports.ts', line: 1, confidence: 'medium' },
      { file: 'core/ports.ts', line: 2, confidence: '' },
    ]
    const { confirmed, lowConfidence } = splitSurvivors(survivors)
    expect(confirmed).toEqual([])
    expect(lowConfidence).toEqual(survivors)
  })

  it('loses no survivor — every deduped finding lands in exactly one array', () => {
    const survivors: Finding[] = [
      { file: 'core/ports.ts', line: 154, confidence: 'high', lens: 'L4' },
      { file: 'core/ports.ts', line: 176, confidence: 'high', lens: 'L3' },
      { file: 'core/ports.ts', line: 194, confidence: 'high', lens: 'L1' },
      { file: 'core/ports.ts', line: 194, confidence: 'low', lens: 'L3' },
      { file: 'zulip/refs.ts', line: 40, confidence: 'low', lens: 'L2' },
    ]
    const { confirmed, lowConfidence } = splitSurvivors(survivors)
    expect(confirmed.length + lowConfidence.length).toBe(4)
    const seen = [...confirmed, ...lowConfidence].map((f) => `${f.file}:${f.line}`)
    expect(new Set(seen).size).toBe(4)
    expect(confirmed.map((f) => `${f.file}:${f.line}`)).toContain('core/ports.ts:194')
  })
})

describe('workflow shape — pre-pass → fan-out → verify → partition → synthesis', () => {
  it('declares the four phases in meta', () => {
    for (const title of ['Discover', 'Reason', 'Verify', 'Synthesise']) {
      expect(source).toContain(`title: '${title}'`)
    }
  })

  it('enforces the partition invariant — assertPartition is called, not merely defined', () => {
    // Definition + at least one call site: coverage is a VERIFIED equation, not a trusted list.
    const occurrences = source.split('assertPartition(').length - 1
    expect(occurrences).toBeGreaterThanOrEqual(2)
  })

  it('fans out over unit×lens cells and verifies each finding adversarially', () => {
    expect(source).toContain('lensFinderPrompt')
    expect(source).toContain('refuteLensPrompt')
    // the fan-out is the cartesian product of units and lenses
    expect(source).toMatch(/units\.flatMap|flatMap\(\s*\(?u/)
  })

  it('is standalone — does not import the sibling effect-native-audit workflow', () => {
    expect(source).not.toContain('effect-native-audit.workflow')
  })
})
