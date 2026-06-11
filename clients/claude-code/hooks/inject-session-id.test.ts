import { expect, test } from 'bun:test'

const SCRIPT_PATH = new URL('./inject-session-id.ts', import.meta.url).pathname

interface HookOutput {
  readonly hookSpecificOutput?: {
    readonly hookEventName?: string
    readonly updatedInput?: Record<string, unknown>
  }
}

const runHook = async (stdin: string): Promise<HookOutput> => {
  const proc = Bun.spawn(['bun', SCRIPT_PATH], {
    stdin: 'pipe',
    stdout: 'pipe',
    stderr: 'pipe',
  })
  proc.stdin.write(stdin)
  await proc.stdin.end()
  const exitCode = await proc.exited
  const stdout = await new Response(proc.stdout).text()
  expect(exitCode).toBe(0)
  if (stdout.length === 0) return {}
  return JSON.parse(stdout) as HookOutput
}

test('forwards session_id from stdin into updatedInput', async () => {
  const out = await runHook(
    JSON.stringify({
      session_id: 'sess-abcdef',
      cwd: '/home/x/assistant',
      tool_input: { channel_name: 'home', body: 'hello' },
    }),
  )
  expect(out.hookSpecificOutput?.updatedInput).toMatchObject({
    channel_name: 'home',
    body: 'hello',
    session_id: 'sess-abcdef',
  })
})

test('forwards cwd from stdin into updatedInput (ass-v7b4)', async () => {
  const out = await runHook(
    JSON.stringify({
      session_id: 'sess-abcdef',
      cwd: '/home/x/brewlife',
      tool_input: { channel_name: 'home', body: 'hello' },
    }),
  )
  expect(out.hookSpecificOutput?.updatedInput).toMatchObject({
    cwd: '/home/x/brewlife',
  })
})

test('preserves the original tool_input fields alongside session_id and cwd', async () => {
  const out = await runHook(
    JSON.stringify({
      session_id: 'sess-abcdef',
      cwd: '/home/x/y',
      tool_input: {
        channel_name: 'home',
        body: 'hello',
        thread: 'general',
        mentions: ['id-1', 'id-2'],
      },
    }),
  )
  expect(out.hookSpecificOutput?.updatedInput).toEqual({
    channel_name: 'home',
    body: 'hello',
    thread: 'general',
    mentions: ['id-1', 'id-2'],
    session_id: 'sess-abcdef',
    cwd: '/home/x/y',
  })
})

test('omits cwd from updatedInput when stdin cwd is missing (still forwards session_id)', async () => {
  const out = await runHook(
    JSON.stringify({
      session_id: 'sess-abcdef',
      tool_input: { channel_name: 'home', body: 'hello' },
    }),
  )
  const updated = out.hookSpecificOutput?.updatedInput
  expect(updated).toMatchObject({ session_id: 'sess-abcdef' })
  expect(updated && 'cwd' in updated).toBe(false)
})

test('omits cwd from updatedInput when stdin cwd is an empty string', async () => {
  const out = await runHook(
    JSON.stringify({
      session_id: 'sess-abcdef',
      cwd: '',
      tool_input: {},
    }),
  )
  const updated = out.hookSpecificOutput?.updatedInput
  expect(updated && 'cwd' in updated).toBe(false)
})

test('omits cwd from updatedInput when stdin cwd is not a string', async () => {
  const out = await runHook(
    JSON.stringify({
      session_id: 'sess-abcdef',
      cwd: 42,
      tool_input: {},
    }),
  )
  const updated = out.hookSpecificOutput?.updatedInput
  expect(updated && 'cwd' in updated).toBe(false)
})

test('emits no updatedInput when session_id is missing', async () => {
  const out = await runHook(JSON.stringify({ cwd: '/home/x', tool_input: {} }))
  expect(out.hookSpecificOutput?.updatedInput).toBeUndefined()
})

test('emits no updatedInput on malformed JSON', async () => {
  const out = await runHook('not json')
  expect(out.hookSpecificOutput?.updatedInput).toBeUndefined()
})
