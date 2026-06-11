import { expect, spyOn, test } from 'bun:test'
import { Effect } from 'effect'
import { captureLogger, stderrLoggerLayer } from './logging.ts'

test('stderrLoggerLayer routes Effect.logInfo to STDERR and never to STDOUT (MCP protocol channel)', async () => {
  // commy is an MCP server: STDOUT carries JSON-RPC. A log line
  // leaking onto STDOUT corrupts the protocol stream. Effect's default
  // logger uses console.log → STDOUT, so we must prove the stderr layer
  // keeps STDOUT pristine while still emitting the diagnostic on STDERR.
  const stdout = spyOn(console, 'log').mockImplementation(() => {})
  const stderr = spyOn(console, 'error').mockImplementation(() => {})
  try {
    await Effect.runPromise(
      Effect.logInfo('commy diagnostic line').pipe(Effect.provide(stderrLoggerLayer)),
    )
    expect(stdout).not.toHaveBeenCalled()
    expect(stderr).toHaveBeenCalledTimes(1)
    expect(String(stderr.mock.calls[0]?.[0])).toContain('commy diagnostic line')
  } finally {
    stdout.mockRestore()
    stderr.mockRestore()
  }
})

test('stderrLoggerLayer routes Effect.logError to STDERR and never to STDOUT', async () => {
  const stdout = spyOn(console, 'log').mockImplementation(() => {})
  const stderr = spyOn(console, 'error').mockImplementation(() => {})
  try {
    await Effect.runPromise(
      Effect.logError('commy error line').pipe(Effect.provide(stderrLoggerLayer)),
    )
    expect(stdout).not.toHaveBeenCalled()
    expect(stderr).toHaveBeenCalledTimes(1)
    expect(String(stderr.mock.calls[0]?.[0])).toContain('commy error line')
  } finally {
    stdout.mockRestore()
    stderr.mockRestore()
  }
})

test('captureLogger records the message text of each Effect.log* call', async () => {
  const lines: string[] = []
  await Effect.runPromise(
    Effect.gen(function* () {
      yield* Effect.logInfo('first line')
      yield* Effect.logError('second line')
    }).pipe(Effect.provide(captureLogger(lines))),
  )
  expect(lines).toEqual(['first line', 'second line'])
})
