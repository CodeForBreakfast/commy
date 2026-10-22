/** The shared contract's tests for MessagePublisher.post. */

import { expect, test } from 'bun:test'
import {
  type AcquiredIdentity,
  type AgentComms,
  type BindError,
  decodeBotNameSync,
  decodeChannelNameSync,
  decodeMessageBodySync,
  decodeThreadNameSync,
  Mention,
  type MessageRef,
  mentionedIdentities,
  UnboundEphemeralSession,
  UnknownChannel,
  UnresolvedMention,
} from '@commy/core/ports'
import { Array as Arr, Effect, Option, Ref } from 'effect'
import type { ContractEnv } from '../contract.ts'

const findByBody = (
  messages: ReadonlyArray<{ readonly body: string }>,
  body: string,
): { readonly body: string } | undefined => messages.find((m) => m.body === body)

/**
 * A bind-on-demand strategy that counts how often it is asked and binds
 * through the very adapter it is built into. The adapter does not exist until
 * its construction returns, so the strategy is wired to it straight after.
 */
const makeCountingBinder = (
  name: string,
): Effect.Effect<{
  readonly bind: Effect.Effect<AcquiredIdentity, BindError>
  readonly calls: Effect.Effect<number>
  readonly wire: (adapter: AgentComms) => Effect.Effect<void>
}> =>
  Effect.gen(function* () {
    const callsRef = yield* Ref.make(0)
    const targetRef = yield* Ref.make<Option.Option<AgentComms>>(Option.none())
    return {
      bind: Ref.update(callsRef, (n) => n + 1).pipe(
        Effect.zipRight(Ref.get(targetRef)),
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.dieMessage('binder ran before the adapter was wired'),
            onSome: (adapter: AgentComms) => adapter.identity.acquire(decodeBotNameSync(name)),
          }),
        ),
      ),
      calls: Ref.get(callsRef),
      wire: (adapter) => Ref.set(targetRef, Option.some(adapter)),
    }
  })

export const publisherPostingContract = (env: () => ContractEnv): void => {
  test('publisher.post returns a MessageRef whose channel matches the destination', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const ref = yield* env().comms.publisher.post(
          channel.name,
          decodeMessageBodySync('hello world'),
        )
        // Channel identity, not the whole ref: a substrate may decorate the
        // returned ChannelRef (e.g. Zulip hangs a permalink off it)
        // beyond the bare {id,name} the seed factory hands back.
        expect(ref.channel.id).toEqual(channel.id)
        expect(ref.channel.name).toEqual(channel.name)
      }),
    ))
  // Substrates must surface unknown-channel failures explicitly. Zulip in
  // particular routes the failure to Notification Bot DMs and returns a
  // success-shaped response — the adapter's job is to pre-flight the channel
  // and throw before the substrate has a chance to swallow the error.
  test('publisher.post on an unknown channel fails with UnknownChannel', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const phantom = decodeChannelNameSync('phantom-channel-never-seeded')
        const error = yield* Effect.flip(
          env().comms.publisher.post(phantom, decodeMessageBodySync('should fail')),
        )
        expect(error).toBeInstanceOf(UnknownChannel)
        if (error instanceof UnknownChannel) expect(error.channel).toEqual(phantom)
      }),
    ))
  test('publisher.post without a thread returns a MessageRef with no thread', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const ref = yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('bare'))
        expect(ref.thread).toEqual(Option.none())
      }),
    ))
  test('publisher.post with thread returns a MessageRef naming that thread, unresolved', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const thread = decodeThreadNameSync('planning')
        const ref = yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('hi'), {
          thread,
        })
        expect(Option.map(ref.thread, (t) => t.name)).toEqual(Option.some(thread))
        expect(Option.map(ref.thread, (t) => t.resolved)).toEqual(Option.some(false))
      }),
    ))
  // A substrate that resolves by renaming the thread must address the renamed
  // form, or the post mints a sibling thread under the clean name and the
  // read below finds the new message in an unresolved thread. The substrate
  // may add its own notice of the resolve to the thread, so the read looks for
  // the post rather than the whole thread.
  test('publisher.post into a resolved thread appends to it and leaves it resolved', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const thread = decodeThreadNameSync('planning')
        yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('settled'), {
          thread,
        })
        yield* env().comms.publisher.resolveThread(channel.name, thread)
        const ref = yield* env().comms.publisher.post(
          channel.name,
          decodeMessageBodySync('one more thing'),
          { thread },
        )
        expect(Option.map(ref.thread, (t) => t.name)).toEqual(Option.some(thread))
        expect(Option.map(ref.thread, (t) => t.resolved)).toEqual(Option.some(true))
        const messages = yield* env().comms.history.readThread(channel.name, thread, {})
        const appended = messages.find((m) => m.body === 'one more thing')
        if (appended === undefined) throw new Error('expected the post in the resolved thread')
        expect(Option.map(appended.ref.thread, (t) => t.resolved)).toEqual(Option.some(true))
      }),
    ))
  test('publisher.post refuses a body that mentions nobody in the realm, and posts nothing', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const body = decodeMessageBodySync('decision for @**Nobody Example** to make')
        const error = yield* Effect.flip(env().comms.publisher.post(channel.name, body))
        expect(error).toBeInstanceOf(UnresolvedMention)
        if (error instanceof UnresolvedMention) {
          expect(error.operation).toBe('post')
          expect(error.tokens).toEqual(['Nobody Example'])
        }
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        expect(findByBody(messages, body)).toBeUndefined()
      }),
    ))
  test('publisher.post refuses a body that mentions a deactivated agent, and posts nothing', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const deactivateAgent = env().deactivateAgent
        if (deactivateAgent === undefined) return
        const gone = yield* env().seedAgent('alice')
        yield* deactivateAgent(gone)
        const channel = yield* env().seedChannel('lobby')
        const body = decodeMessageBodySync(`decision for @**${gone.name}** to make`)
        const error = yield* Effect.flip(env().comms.publisher.post(channel.name, body))
        expect(error).toBeInstanceOf(UnresolvedMention)
        if (error instanceof UnresolvedMention) {
          expect(error.operation).toBe('post')
          expect(error.tokens).toEqual([String(gone.name)])
        }
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        expect(findByBody(messages, body)).toBeUndefined()
      }),
    ))
  test('publisher.post accepts a name|id mention of an agent and reports that agent', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const alice = yield* env().seedAgent('alice')
        const body = decodeMessageBodySync(`@**${alice.name}|${alice.id}** wake up`)
        yield* env().comms.publisher.post(channel.name, body)
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        const found = messages.find((m) => m.body === body)
        expect(mentionedIdentities(found?.mentions ?? []).map((m) => m.id)).toEqual([alice.id])
      }),
    ))
  test('publisher.post refuses a name|id mention of a deactivated agent, and posts nothing', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const deactivateAgent = env().deactivateAgent
        if (deactivateAgent === undefined) return
        const gone = yield* env().seedAgent('alice')
        yield* deactivateAgent(gone)
        const channel = yield* env().seedChannel('lobby')
        const token = `${gone.name}|${gone.id}`
        const body = decodeMessageBodySync(`decision for @**${token}** to make`)
        const error = yield* Effect.flip(env().comms.publisher.post(channel.name, body))
        expect(error).toBeInstanceOf(UnresolvedMention)
        if (error instanceof UnresolvedMention) expect(error.tokens).toEqual([token])
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        expect(findByBody(messages, body)).toBeUndefined()
      }),
    ))
  test('publisher.post refuses a name|id mention whose name is not the named agent', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const alice = yield* env().seedAgent('alice')
        const bob = yield* env().seedAgent('bob')
        const token = `${bob.name}|${alice.id}`
        const error = yield* Effect.flip(
          env().comms.publisher.post(channel.name, decodeMessageBodySync(`@**${token}** hi`)),
        )
        expect(error).toBeInstanceOf(UnresolvedMention)
        if (error instanceof UnresolvedMention) expect(error.tokens).toEqual([token])
      }),
    ))
  test('publisher.post refuses a name|id mention whose name differs from the agent only by case', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const alice = yield* env().seedAgent('alice')
        const token = `${String(alice.name).toUpperCase()}|${alice.id}`
        const error = yield* Effect.flip(
          env().comms.publisher.post(channel.name, decodeMessageBodySync(`@**${token}** hi`)),
        )
        expect(error).toBeInstanceOf(UnresolvedMention)
        if (error instanceof UnresolvedMention) expect(error.tokens).toEqual([token])
      }),
    ))
  test('publisher.post refusing dead mentions names each dead token once and no live name', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const alice = yield* env().seedAgent('alice')
        const body = decodeMessageBodySync(
          `@**Nobody One** and @**${alice.name}** and @**Nobody Two** and @**Nobody One**`,
        )
        const error = yield* Effect.flip(env().comms.publisher.post(channel.name, body))
        expect(error).toBeInstanceOf(UnresolvedMention)
        if (error instanceof UnresolvedMention) {
          expect(error.tokens).toEqual(['Nobody One', 'Nobody Two'])
        }
      }),
    ))
  // A mention form inside a code span is literal text, not a failed delivery,
  // so it never triggers UnresolvedMention.
  test('publisher.post delivers a mention form naming nobody verbatim when it sits in a code span', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const body = decodeMessageBodySync('the dead form `@**Nobody Example**` names nobody')
        yield* env().comms.publisher.post(channel.name, body)
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        expect(findByBody(messages, body)).toBeDefined()
      }),
    ))
  test('publisher.post with thread is visible via history.readThread', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const thread = decodeThreadNameSync('design')
        yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('in thread'), {
          thread,
        })
        const messages = yield* env().comms.history.readThread(channel.name, thread, {})
        const found = findByBody(messages, 'in thread')
        expect(found).toBeDefined()
      }),
    ))
  // A substrate may cut an over-long thread name short. The ref `post` returns
  // must carry the name that was kept, or the caller cannot address the thread.
  test('publisher.post returns the thread name the substrate kept, which addresses the thread', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const ref = yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('long'), {
          thread: decodeThreadNameSync('x'.repeat(env().threadNameLimit + 10)),
        })
        const kept = yield* Effect.fromNullable(Option.getOrUndefined(ref.thread)).pipe(
          Effect.orDie,
        )
        const messages = yield* env().comms.history.readThread(channel.name, kept.name, {})
        expect(findByBody(messages, 'long')).toBeDefined()
      }),
    ))
  test('publisher.post with thread is also visible via history.readChannel (channel reads include all topics)', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('topic-a'), {
          thread: decodeThreadNameSync('alpha'),
        })
        yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('topic-b'), {
          thread: decodeThreadNameSync('bravo'),
        })
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        const bodies = messages.map((m) => m.body)
        expect(bodies).toContain(decodeMessageBodySync('topic-a'))
        expect(bodies).toContain(decodeMessageBodySync('topic-b'))
      }),
    ))
  test('publisher.post reports who its body @-mentions via history.readChannel', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const alice = yield* env().seedAgent('alice')
        const body = decodeMessageBodySync(`@**${alice.name}** wake up`)
        yield* env().comms.publisher.post(channel.name, body)
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        const found = messages.find((m) => m.body === body)
        expect(found).toBeDefined()
        expect(mentionedIdentities(found?.mentions ?? []).map((m) => m.id)).toEqual([alice.id])
      }),
    ))
  test('publisher.post reports a repeated mention once, in the order first written', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const alice = yield* env().seedAgent('alice')
        const bob = yield* env().seedAgent('bob')
        const body = decodeMessageBodySync(
          `@**${bob.name}** then @**${alice.name}** then @**${bob.name}** again`,
        )
        yield* env().comms.publisher.post(channel.name, body)
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        const found = messages.find((m) => m.body === body)
        expect(mentionedIdentities(found?.mentions ?? []).map((m) => m.id)).toEqual([
          bob.id,
          alice.id,
        ])
      }),
    ))
  // A silent mention links the name but notifies nobody.
  test('publisher.post reports no mention for a silent mention', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const alice = yield* env().seedAgent('alice')
        const body = decodeMessageBodySync(`as @_**${alice.name}** said`)
        yield* env().comms.publisher.post(channel.name, body)
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        const found = messages.find((m) => m.body === body)
        expect(found).toBeDefined()
        expect(found?.mentions).toEqual([])
      }),
    ))
  test('publisher.post accepts a channel wildcard and reports it as a channel-wide mention', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const body = decodeMessageBodySync('heads up @**all**, deploying')
        yield* env().comms.publisher.post(channel.name, body)
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        const found = messages.find((m) => m.body === body)
        expect(found?.mentions).toEqual([Mention.ChannelWildcardMention()])
      }),
    ))
  test('publisher.post accepts a topic wildcard and reports it as a topic-wide mention', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const thread = decodeThreadNameSync('deploy')
        const body = decodeMessageBodySync('heads up @**topic**, deploying')
        yield* env().comms.publisher.post(channel.name, body, { thread })
        const messages = yield* env().comms.history.readThread(channel.name, thread, {})
        const found = messages.find((m) => m.body === body)
        expect(found?.mentions).toEqual([Mention.TopicWildcardMention()])
      }),
    ))
  // Bounded, because the contract also runs live against a rate-limited realm.
  test('publisher.post made concurrently returns a distinct message ref for each post', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const refs = yield* Effect.all(
          Arr.makeBy(5, (i) =>
            env().comms.publisher.post(channel.name, decodeMessageBodySync(`concurrent ${i}`)),
          ),
          { concurrency: 3 },
        )
        expect(new Set(refs.map((r) => r.id)).size).toBe(5)
      }),
    ))
  // A token quoted in code is literal text: the substrate renders it as code
  // and notifies nobody.
  test('publisher.post reports no mention for a token inside a code span or block', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const alice = yield* env().seedAgent('alice')
        const body = decodeMessageBodySync(
          `write \`@**${alice.name}**\` to ping\n\n\`\`\`\n@**${alice.name}**\n\`\`\``,
        )
        yield* env().comms.publisher.post(channel.name, body)
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        const found = messages.find((m) => m.body === body)
        expect(found).toBeDefined()
        expect(found?.mentions).toEqual([])
      }),
    ))
  test('publisher.post reports a real mention beside the same name in a code span, once', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const alice = yield* env().seedAgent('alice')
        const body = decodeMessageBodySync(
          `@**${alice.name}** never write \`@**${alice.name}**\` in a code span`,
        )
        yield* env().comms.publisher.post(channel.name, body)
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        const found = messages.find((m) => m.body === body)
        expect(found?.mentions).toHaveLength(1)
        expect(mentionedIdentities(found?.mentions ?? []).map((m) => m.id)).toEqual([alice.id])
      }),
    ))
  test('publisher.post with replyTo does not throw (substrate may quote-block or drop)', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const parentRef = yield* env().comms.publisher.post(
          channel.name,
          decodeMessageBodySync('parent'),
        )
        const followUp: MessageRef = yield* env().comms.publisher.post(
          channel.name,
          decodeMessageBodySync('child'),
          { replyTo: parentRef },
        )
        expect(followUp.channel.id).toEqual(channel.id)
        expect(followUp.channel.name).toEqual(channel.name)
      }),
    ))
  // The factory's unacquired adapter is built with no bind-on-demand strategy,
  // so nothing can bind it before the write.
  test('publisher.post on an adapter that has not acquired an identity fails with UnboundEphemeralSession', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const newUnacquiredAdapter = env().newUnacquiredAdapter
        if (newUnacquiredAdapter === undefined) return
        const channel = yield* env().seedChannel('lobby')
        const fresh = yield* newUnacquiredAdapter()
        const error = yield* Effect.flip(
          fresh.publisher.post(channel.name, decodeMessageBodySync('nobody to send it')),
        )
        expect(error).toBeInstanceOf(UnboundEphemeralSession)
      }),
    ))
  test('publisher.post on an adapter that has never acquired binds through its bind-on-demand strategy', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const newUnacquiredAdapter = env().newUnacquiredAdapter
        if (newUnacquiredAdapter === undefined) return
        const channel = yield* env().seedChannel('lobby')
        const current = yield* env().comms.identity.currentIdentity()
        const binder = yield* makeCountingBinder(`${current.name}-cycle`)
        const fresh = yield* newUnacquiredAdapter(binder.bind)
        yield* binder.wire(fresh)
        yield* fresh.publisher.post(channel.name, decodeMessageBodySync('bound on demand'))
        expect(yield* binder.calls).toBe(1)
        const bound = yield* fresh.identity.currentIdentity()
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        const found = messages.find((m) => m.body === 'bound on demand')
        expect(found?.sender.id).toEqual(bound.id)
      }),
    ))
  // "Is something bound?" and "may this caller use it?" differ, and only the
  // strategy can answer the second, so a bound seat does not skip it.
  test('publisher.post asks the bind-on-demand strategy on every write, bound or not', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const newUnacquiredAdapter = env().newUnacquiredAdapter
        if (newUnacquiredAdapter === undefined) return
        const channel = yield* env().seedChannel('lobby')
        const current = yield* env().comms.identity.currentIdentity()
        const binder = yield* makeCountingBinder(`${current.name}-cycle`)
        const fresh = yield* newUnacquiredAdapter(binder.bind)
        yield* binder.wire(fresh)
        yield* fresh.publisher.post(channel.name, decodeMessageBodySync('one'))
        yield* fresh.publisher.post(channel.name, decodeMessageBodySync('two'))
        expect(yield* binder.calls).toBe(2)
      }),
    ))
  test('publisher.post fails with the strategy refusal and posts nothing, even on a seat that has acquired', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const newUnacquiredAdapter = env().newUnacquiredAdapter
        if (newUnacquiredAdapter === undefined) return
        const channel = yield* env().seedChannel('lobby')
        const current = yield* env().comms.identity.currentIdentity()
        const fresh = yield* newUnacquiredAdapter(
          Effect.fail(new UnboundEphemeralSession({ message: 'this caller may not bind' })),
        )
        yield* fresh.identity.acquire(decodeBotNameSync(`${current.name}-cycle`))
        const error = yield* Effect.flip(
          fresh.publisher.post(channel.name, decodeMessageBodySync('refused')),
        )
        expect(error).toBeInstanceOf(UnboundEphemeralSession)
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        expect(findByBody(messages, 'refused')).toBeUndefined()
      }),
    ))
}
