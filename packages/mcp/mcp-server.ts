import { Server } from '@modelcontextprotocol/sdk/server/index.js'

/**
 * Plugin version echoed at MCP `initialize` time. Released in lockstep
 * with `.claude-plugin/plugin.json` and `package.json` — the parity is
 * enforced by manifests.test.ts. Update only via the `release-plugin`
 * skill, which edits all three sites and tags the result.
 */
export const PLUGIN_VERSION = '0.14.0'

/**
 * Echoed to every connected MCP client via the server `instructions:`
 * field. Covers seven topics chosen to keep concierge-style sessions
 * coherent across the workstation's overlapping comms substrates
 * (comms-msx): substrate preference, channel naming + discovery
 * (comms-tg6), topic discipline (comms-tg6), subscription discipline,
 * clickable-permalink rendering (comms-0lob.1), tool cheat sheet, and
 * the `session_id` contract introduced by ass-2dhb. Kept terse on
 * purpose — connected sessions already carry plenty of CLAUDE.md
 * context.
 */
const COMMY_INSTRUCTIONS = `**Canonical substrate.** commy is the inter-agent channel on this workstation. Where you also see \`claude-peers\`, \`agent-mail\`, or Discord MCP tools, prefer commy — claude-peers and Discord are retiring (comms-sxf); agent-mail is a mail-shaped substrate kept for specific flows. Don't fan the same message across substrates.

**Channels.** Each project has one channel: \`#<project-slug>\` where the slug is resolved per session as \`COMMY_PROJECT\` env > git remote \`origin\` basename > git root basename. Sessions launched outside a git repo (e.g. \`/tmp\`, \`$HOME\`) have no project channel and should post to \`#general\` instead. Don't invent a channel name from a metaphor — never post to a literal \`#home\`, \`#project\`, or \`#<project>\`. Use \`list_channels\` to enumerate what actually exists in the realm; posting to a non-existent channel throws \`UnknownChannel\` rather than silently routing to Notification Bot.

**Topics.** A topic is a logical thread of conversation within a channel; the \`post\` tool's \`thread\` argument names it (Zulip calls these "topics"). Open a new topic when the work shifts — a fresh bead, a new question, a different incident — and reply into an existing one when you're continuing the same line of work. Name topics by the work, not the speaker: \`payments-migration\`, \`comms-tg6-policy\`, \`zulip-events-queue-expiry\` — not \`bot-debugging\` or \`graeme-asked\`. Top-level channel chatter (omit \`thread\`) is for terse status pings only; anything substantive deserves a topic.

**Subscriptions.** Be on your project channel and \`#general\` only — not on other projects' channels. The workstation's boot-time defaults come from the plugin's \`COMMY_SUBSCRIBE\` userConfig (tokens: \`channel:<name>\`, \`thread:<channel>/<thread>\`, \`new-topics:<channel>\`, \`mentions\`); adjust the live set via \`subscribe\`/\`unsubscribe\` at runtime. Inbound matches arrive as \`<channel source="commy" ...>\` blocks. If one arrives from a channel you aren't subscribed to, treat it as background context and don't reply — only post into your project channel, into \`#general\`, or into threads you've explicitly joined. **Refer to peers by name** — the \`sender_name\` (and reaction \`by_name\`) in those blocks — never by a bare number; reserve numbers for message and bead ids, which otherwise collide with identity in a thread.

**Links.** Every ref the substrate hands you carries a ready-to-click \`permalink\` — on \`post\` results, \`read_channel\`/\`read_thread\` messages (message \`permalink\` plus \`channel.permalink\` and \`thread.permalink\`), \`list_channels\` (channel \`permalink\`), and inbound \`<channel source="commy">\` frames (\`permalink\` / \`channel_permalink\` / \`thread_permalink\` meta, \`target_permalink\` on reaction frames). **Whenever you show a human a message, channel, or topic reference, render it as that clickable permalink — never a bare name or numeric id.** A human can click a permalink straight to the message; a bare \`#channel > topic\` or message number makes them hunt. When you hold only a message id (e.g. one cited elsewhere, with no permalink to hand), \`message_link(message_id, channel_name?, thread?)\` returns its \`{permalink}\`.

**Tools.** \`post\` (channel; optionally thread or reply), \`react\`/\`unreact\` (emoji on a message), \`subscribe\`/\`unsubscribe\` (live target), \`read_channel\`/\`read_thread\` (history within a range), \`list_channels\` (enumerate channels in the realm), \`message_link\` (canonical permalink for a message id), \`resolve\` (identity by name), \`current_identity\` (passive — never acquires), \`download_file\` (fetch a \`/user_uploads/...\` attachment to a temp file — use Read on the returned path to view images), \`upload_file\` (upload a local file by absolute path; returns a \`reference\` string to embed in a \`post\` body).

**session_id.** Pass your conversation's session id as the optional argument on \`post\`, \`edit_message\`, \`react\`, \`unreact\`, and \`current_identity\`. **Must be a UUID** (e.g. \`crypto.randomUUID()\`); anything else is rejected as malformed and the call routes to the "missing session_id" error rather than silently minting a \`cc-<garbage>\` identity (comms-uqf). In Claude Code the plugin's PreToolUse hook injects the harness session id automatically. The server uses it to derive the ephemeral \`cc-<8>\` bot identity for this conversation and to detect transitions between conversations.`

/**
 * Construct the commy MCP server with the capabilities the plugin
 * needs declared at initialize-time. Tool handlers and the inbound
 * event-pump are wired in later boot stages — this builder owns only the
 * static capability surface.
 *
 * `tools: {}` is declared up front so per-tool handlers register on the
 * same server instance without a second construction. The
 * `claude/channel` experimental capability opts in to the channel-block
 * notification stream that the event-pump publishes. We do NOT declare
 * `claude/channel/permission`: that capability asserts the plugin
 * authenticates the human replying, and commy has no
 * human-counterpart pairing concept on the port surface.
 *
 * `logging: {}` is required because the event-pump also dual-emits each
 * inbound event as the MCP-standard `notifications/message`
 * (`LoggingMessageNotification`) host-neutral carrier alongside
 * `claude/channel` — the SDK's `assertNotificationCapability` throws on the
 * send path unless the server advertises the logging capability.
 *
 * The `instructions:` field carries the canonical guidance block — see
 * `COMMY_INSTRUCTIONS` for content and `comms-msx` for rationale.
 */
export const buildMcpServer = (): Server =>
  new Server(
    { name: 'commy', version: PLUGIN_VERSION },
    {
      capabilities: {
        tools: {},
        logging: {},
        experimental: {
          'claude/channel': {},
        },
      },
      instructions: COMMY_INSTRUCTIONS,
    },
  )
