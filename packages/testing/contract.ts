/**
 * Substrate-agnostic contract tests for the AgentComms ports.
 *
 * Parameterised over a factory that produces a fresh environment per test —
 * `comms` plus seed helpers for setting up channels and other identities.
 * Each adapter that claims to implement the AgentComms aggregate must pass
 * this suite.
 *
 * The factory shape is the only substrate-specific concession. Tests
 * exercise behaviour through the ports, never through adapter internals.
 * Each port's tests live in their own module under `./contract/`.
 *
 * Test bodies run as Effects: each `test(name, ...)` invokes
 * `Effect.runPromise(Effect.gen(...))` at the test boundary, so port calls
 * compose via `yield*` and the typed E channel survives instead of
 * collapsing across per-call Promise interop.
 */

import { afterEach, beforeEach, describe } from 'bun:test'
import type {
  AcquiredIdentity,
  AgentComms,
  AttachmentStore,
  BindError,
  ChannelName,
  ChannelRef,
  Identity,
  MessageBody,
  PostOpts,
  Presence,
} from '@commy/core/ports'
import { Effect } from 'effect'
import { attachmentsContract } from './contract/attachments.ts'
import { directoryContract } from './contract/directory.ts'
import { historyContract } from './contract/history.ts'
import { identityContract } from './contract/identity.ts'
import { inboxContract } from './contract/inbox.ts'
import { publisherPostingContract } from './contract/posting.ts'
import { publisherContract } from './contract/publisher.ts'
import { REALM_HOOK_TIMEOUT_MS } from './realm-hooks.ts'

export interface ContractEnv {
  /**
   * The adapter under test, pre-acquired by the factory to its `self`
   * identity. Tests exercise it only through this surface.
   */
  readonly comms: AgentComms
  /**
   * Make a channel with the given display name available to the adapter.
   * Returns the ref tests use to address it.
   */
  readonly seedChannel: (name: string) => Effect.Effect<ChannelRef>
  /**
   * Make a peer identity available — populated in `directory.listAgents`
   * / `listHumans`, resolvable by `identity.resolve`. The factory chooses
   * a kind appropriate to the substrate (memory: caller-controlled; Zulip:
   * driven by the seeded user record).
   */
  readonly seedAgent: (name: string) => Effect.Effect<Identity>
  /**
   * Optional. Make a human member available — populated in
   * `directory.listHumans`. Substrates that cannot stand up a human member
   * omit it and the suite skips the tests that need one.
   */
  readonly seedHuman?: (name: string) => Effect.Effect<Identity>
  /**
   * Optional. Deactivate an agent obtained from `seedAgent`, as the substrate
   * does when a member leaves. Substrates that cannot deactivate a member omit
   * it and the suite skips the tests that need one.
   */
  readonly deactivateAgent?: (agent: Identity) => Effect.Effect<void>
  /**
   * Optional. Have the substrate report `presence` for a human obtained from
   * `seedHuman`. Substrates that cannot set a member's presence omit it and
   * the suite skips the tests that need it.
   */
  readonly setPresence?: (
    human: Identity,
    presence: Exclude<Presence, 'unknown'>,
  ) => Effect.Effect<void>
  /**
   * Optional. Construct a fresh, unauthenticated adapter on the same
   * substrate for lifecycle tests that exercise acquire/release state
   * transitions from a clean start. Substrates whose `acquire` can
   * dynamically bind any name (Memory, post-rework Zulip with the
   * minter wired up) should provide this; substrates whose
   * acquire-acceptable names are pinned by external configuration
   * (transitional Zulip, future Discord) can omit it and the
   * suite skips the rebind/no-op tests.
   *
   * Built with `bindOnDemand` as its bind-on-demand strategy when one is
   * given, and with none otherwise.
   *
   * Test owns disposal of any extra adapters it spins up; the factory's
   * `dispose` still runs once per test and tears down what the factory
   * itself stood up.
   */
  readonly newUnacquiredAdapter?: (
    bindOnDemand?: Effect.Effect<AcquiredIdentity, BindError>,
  ) => Effect.Effect<AgentComms>
  /**
   * Optional. When set together with `newUnacquiredAdapter`, calling
   * `acquire(unacquirableName)` on a freshly-built adapter throws
   * `UnknownIdentity`. Substrates that can always mint omit this;
   * substrates with a pre-provisioned pool (Discord, Memory in
   * allowlist mode) opt in by setting it.
   */
  readonly unacquirableName?: string
  /**
   * Optional. Post a message authored by a seeded peer (sender ≠ self) into a
   * channel the bound self can observe. Drives the mention-floor tests:
   * a peer @-mentions self and self's `events()` must yield
   * `mention-received`. This is the cross-identity shape `realm.live.test.ts`
   * proves with observer ≠ sender — the contract can't express it through
   * `comms.publisher.post` alone because that always authors as self, and a
   * single-identity self-mention never surfaces on a substrate whose mention
   * narrow is keyed to the queue owner (live Zulip).
   *
   * Memory injects a peer-authored message straight into the same in-process
   * substrate; Zulip posts through a peer-bound adapter. Substrates that
   * cannot stand up a posting peer omit it and the suite skips the
   * peer-mention tests. The `peer` is one obtained from `seedAgent`.
   */
  readonly peerPost?: (
    peer: Identity,
    channel: ChannelName,
    body: MessageBody,
    opts?: PostOpts,
  ) => Effect.Effect<void>
  /**
   * Longest channel description this substrate stores, in characters. The
   * description tests build their over-length input from it rather than
   * assuming any substrate's number — the contract asserts that a substrate
   * *refuses* what it cannot hold, not what the cap happens to be. Each
   * substrate reports its own (Zulip 1024, memory its configured limit).
   */
  readonly channelDescriptionLimit: number
  /**
   * Longest message body this substrate stores whole, in characters. The
   * over-length tests build their input from it, so the contract asserts that
   * a substrate refuses what it cannot hold and never what the cap happens to
   * be. Zulip reports its realm's `max_message_length`, memory its configured
   * limit.
   */
  readonly messageLengthLimit: number
  /**
   * Longest thread name this substrate keeps, in characters. A substrate that
   * cuts a longer name short must return the name it kept from `post`; the
   * thread-name test builds its over-length input from this limit.
   */
  readonly threadNameLimit: number
  /** Tear down whatever the factory stood up. Called once per test. */
  readonly dispose: () => Effect.Effect<void>
}

export type ContractFactory = () => Promise<ContractEnv>

/**
 * A contract environment whose adapter also implements `AttachmentStore`.
 * Only an adapter that does runs the attachment contract.
 */
export interface AttachmentContractEnv extends ContractEnv {
  /** The adapter under test's `AttachmentStore`, bound as `comms` is. */
  readonly attachments: AttachmentStore
}

export type AttachmentContractFactory = () => Promise<AttachmentContractEnv>

/** Build a fresh environment before each test and dispose of it after. */
const envPerTest = <E extends ContractEnv>(factory: () => Promise<E>): (() => E) => {
  let env: E

  beforeEach(async () => {
    env = await factory()
  }, REALM_HOOK_TIMEOUT_MS)

  afterEach(() => Effect.runPromise(env.dispose()), REALM_HOOK_TIMEOUT_MS)

  return () => env
}

export const runAgentCommsContract = (label: string, factory: ContractFactory): void => {
  describe(`AgentComms contract — ${label}`, () => {
    const currentEnv = envPerTest(factory)
    publisherPostingContract(currentEnv)
    publisherContract(currentEnv)
    directoryContract(currentEnv)
    historyContract(currentEnv)
    identityContract(currentEnv)
    inboxContract(currentEnv)
  })
}

export const runAttachmentStoreContract = (
  label: string,
  factory: AttachmentContractFactory,
): void => {
  describe(`AttachmentStore contract — ${label}`, () => {
    attachmentsContract(envPerTest(factory))
  })
}
