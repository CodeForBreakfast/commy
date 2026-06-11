import { beforeAll, describe, expect, it } from 'bun:test'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'

type Finding = {
  file: string
  line: number
  confidence: string
  kind?: string
  smell?: string
}

type SplitResult = { confirmed: Finding[]; lowConfidence: Finding[] }

// The audit workflow runs as a sandbox-wrapped function body (top-level return + injected globals),
// so it has no import seam. We exercise the REAL source by extracting just the splitSurvivors
// declaration — delimited by marker comments in the workflow — and evaluating it in isolation.
const WORKFLOW_PATH = join(
  import.meta.dir,
  '..',
  '.claude',
  'skills',
  'effect-native-audit',
  'effect-native-audit.workflow.js',
)

let splitSurvivors: (survivors: Finding[]) => SplitResult

beforeAll(() => {
  const source = readFileSync(WORKFLOW_PATH, 'utf8')
  const begin = '// --- splitSurvivors:'
  const end = '// --- end splitSurvivors ---'
  const start = source.indexOf(begin)
  const stop = source.indexOf(end)
  if (start === -1 || stop === -1) {
    throw new Error('splitSurvivors marker comments not found in the workflow source')
  }
  const fnSource = source.slice(start, stop)
  splitSurvivors = new Function(`${fnSource}\nreturn splitSurvivors`)() as typeof splitSurvivors
})

describe('splitSurvivors', () => {
  it('routes every high-confidence finding to confirmed and the rest to lowConfidence', () => {
    const high: Finding = { file: 'core/a.ts', line: 10, confidence: 'high', kind: 'idiom' }
    const low: Finding = { file: 'core/b.ts', line: 20, confidence: 'low', kind: 'modelling' }

    const { confirmed, lowConfidence } = splitSurvivors([high, low])

    expect(confirmed).toEqual([high])
    expect(lowConfidence).toEqual([low])
  })

  it('dedups by (file,line) keeping the highest-confidence copy, never duplicating across arrays', () => {
    // The real 2026-05-31 hazard: one site flagged twice across axes —
    // idiom/high AND data-first-helper/low at the same file:line. A naive
    // filter would put it in BOTH arrays.
    const high: Finding = {
      file: 'channels-catch-up.ts',
      line: 154,
      confidence: 'high',
      kind: 'idiom',
    }
    const low: Finding = {
      file: 'channels-catch-up.ts',
      line: 154,
      confidence: 'low',
      kind: 'modelling',
      smell: 'data-first-helper',
    }

    const { confirmed, lowConfidence } = splitSurvivors([low, high])

    expect(confirmed).toEqual([high])
    expect(lowConfidence).toEqual([])
  })

  it('treats any non-high confidence value as low', () => {
    const survivors: Finding[] = [
      { file: 'core/a.ts', line: 1, confidence: 'medium' },
      { file: 'core/b.ts', line: 2, confidence: '' },
    ]

    const { confirmed, lowConfidence } = splitSurvivors(survivors)

    expect(confirmed).toEqual([])
    expect(lowConfidence).toEqual(survivors)
  })

  it('keeps distinct lines in the same file as separate findings', () => {
    const high: Finding = { file: 'core/a.ts', line: 10, confidence: 'high' }
    const low: Finding = { file: 'core/a.ts', line: 20, confidence: 'low' }

    const { confirmed, lowConfidence } = splitSurvivors([high, low])

    expect(confirmed).toEqual([high])
    expect(lowConfidence).toEqual([low])
  })

  it('loses no survivor — every deduped finding lands in exactly one array', () => {
    const survivors: Finding[] = [
      { file: 'bootstrap.ts', line: 78, confidence: 'high', kind: 'modelling' },
      { file: 'bootstrap.ts', line: 96, confidence: 'high', kind: 'behaviour' },
      { file: 'adapter.ts', line: 1240, confidence: 'high', kind: 'structural' },
      { file: 'memory/adapter.ts', line: 124, confidence: 'high', kind: 'modelling' },
      { file: 'memory/adapter.ts', line: 124, confidence: 'low', kind: 'idiom' },
      { file: 'core/c.ts', line: 5, confidence: 'low' },
    ]

    const { confirmed, lowConfidence } = splitSurvivors(survivors)

    // 5 distinct sites; the doubled memory/adapter.ts:124 collapses to one high.
    expect(confirmed.length + lowConfidence.length).toBe(5)
    const seen = [...confirmed, ...lowConfidence].map((f) => `${f.file}:${f.line}`)
    expect(new Set(seen).size).toBe(5)
    expect(confirmed.map((f) => `${f.file}:${f.line}`)).toContain('memory/adapter.ts:124')
  })

  it('keeps the first high copy when a site is flagged high more than once', () => {
    const first: Finding = { file: 'core/a.ts', line: 10, confidence: 'high', kind: 'idiom' }
    const second: Finding = { file: 'core/a.ts', line: 10, confidence: 'high', kind: 'structural' }

    const { confirmed, lowConfidence } = splitSurvivors([first, second])

    expect(confirmed).toEqual([first])
    expect(lowConfidence).toEqual([])
  })
})
