/**
 * Logging seam for the plugin's Effect chains.
 *
 * commy is an MCP server speaking JSON-RPC over stdio: STDOUT is
 * the protocol channel, so any stray byte there corrupts the stream.
 * Effect's default logger writes via `console.log` (STDOUT), so every
 * runtime edge that may run a logging Effect provides
 * {@link stderrLoggerLayer}, which routes `Effect.logInfo` / `logError`
 * to STDERR via `console.error`.
 *
 * `Logger.prettyLogger({ stderr: true })` is deliberately not used: its
 * TTY renderer calls `console.group` / `console.groupEnd`, both of which
 * write to STDOUT regardless of the `stderr` flag — that would leak onto
 * the protocol channel. `Logger.withConsoleError` wraps a single-string
 * logger so the whole line goes out via `console.error` with no group
 * framing.
 */

import type { Layer } from 'effect'
import { Logger } from 'effect'

/**
 * Routes every `Effect.log*` call to STDERR in human-readable logfmt.
 * Provided at each runtime edge so plugin diagnostics never touch the
 * MCP STDOUT protocol channel. Production default.
 */
export const stderrLoggerLayer: Layer.Layer<never> = Logger.replace(
  Logger.defaultLogger,
  Logger.withConsoleError(Logger.logfmtLogger),
)

/**
 * Test seam that captures the message text of every `Effect.log*` call into
 * `lines`, so a test can provide this layer and assert on the diagnostics an
 * Effect emitted without touching the runner's STDERR.
 */
export const captureLogger = (lines: Array<string>): Layer.Layer<never> =>
  Logger.replace(
    Logger.defaultLogger,
    Logger.make(({ message }) => {
      lines.push(Array.isArray(message) ? message.map(String).join(' ') : String(message))
    }),
  )
