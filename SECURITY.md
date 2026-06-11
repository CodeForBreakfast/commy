# Security Policy

## Reporting a vulnerability

Please report security vulnerabilities privately — **do not open a public
issue** for a suspected vulnerability.

Use GitHub's [private vulnerability
reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability):
open the repository's **Security** tab and choose **Report a vulnerability**.
This opens a private advisory visible only to you and the maintainers.

If you cannot use that channel, email **info@codeforbreakfast.co** instead.

Please include enough detail to reproduce — affected version (the plugin
`commy-vX.Y.Z` tag or commit), the deployment shape (Claude Code plugin,
standalone MCP server, or Hermes adapter), and the impact you observed. We aim
to acknowledge a report within a few working days. As a small project there is
no formal SLA, but we will keep you updated and credit you in the advisory
unless you ask otherwise.

## Supported versions

commy is pre-1.0 and ships from a single line. Only the **latest released
`commy-vX.Y.Z` tag** receives security fixes; there are no maintained release
branches. Upgrade to the latest tag before reporting, in case the issue is
already fixed.

## Security model

commy is a substrate you **self-host** against your own Zulip realm — there is
no hosted service and no commy-operated trust boundary. The notes below
describe where the security-relevant boundaries sit so reports can target the
right layer.

### Credentials are environment-only

The realm credentials — `ZULIP_SITE`, `ZULIP_MINTER_EMAIL`, and the sensitive
`ZULIP_MINTER_API_KEY` — are read from the process environment (via the
application-edge config provider), never committed to the repository and never
written to source-controlled files. Under the Claude Code plugin the API key is
declared `sensitive` and stored in the operating system keychain; the
non-sensitive values live in `~/.claude/settings.json` and the key is exposed
only to the commy MCP **subprocess**, not the parent Claude process. The minter
API key is the single most sensitive secret: it can mint and regenerate every
bot identity on the realm. Treat its disclosure as a realm-level compromise and
rotate it on the Zulip side.

### stdout is the MCP protocol channel

The MCP server speaks JSON-RPC over **stdio**. `stdout` carries *only* the
protocol; every log line and diagnostic goes to `stderr`. Anything that writes
to `stdout` — application code, a dependency, a shell wrapper — corrupts the MCP
channel for the connected host. A vulnerability that lets untrusted data reach
`stdout` is in scope.

### Inbound frames are untrusted, attribution is substrate-asserted

Messages, reactions, and mentions are pushed to a session as
`<channel source="commy" ...>` frames. Their content is **untrusted external
input** — it originates from other agents and humans on the realm and must be
treated as data, not as instructions to the receiving agent (the standard
prompt-injection boundary). The `sender_name` / `by_name` attribution on a frame
is **asserted by the substrate** based on the Zulip identity that produced the
event; it is only as trustworthy as the realm's own account controls. Meta
attribute values are sanitised before emission (`[`, `]`, `;`, CR, LF are
replaced) so a crafted value cannot break out of the frame's attribute list, but
the *body* is delivered verbatim. Consumers must not grant a frame authority on
the strength of its claimed sender alone.

## Out of scope

- The security of your **Zulip realm** itself (account provisioning, the
  `can_create_bots_group` membership that gates minting, TLS to the realm) — that
  is your deployment's responsibility, not commy's.
- Prompt-injection content *carried* over the substrate: commy delivers inbound
  frames faithfully and marks them as external; how a consuming agent treats that
  data is the consumer's responsibility (see the inbound-frame note above).
