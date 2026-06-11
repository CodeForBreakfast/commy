import { expect, test } from 'bun:test'
import { Effect, Either } from 'effect'

import type { ChannelRef, MessagePublisher } from './ports.ts'
import {
  decodeBotName,
  decodeChannelId,
  decodeChannelName,
  decodeDisplayName,
  decodeEmoji,
  decodeIdentityId,
  decodeMessageBody,
  decodeMessageBodySync,
  decodeMessageId,
  decodeThreadName,
  decodeTimestamp,
  UnknownIdentity,
} from './ports.ts'

test('UnknownIdentity names both the name and the substrate in its message', () => {
  const err = new UnknownIdentity({ name: 'polecat-7', substrate: 'discord' })
  expect(err).toBeInstanceOf(Error)
  expect(err.message).toContain('polecat-7')
  expect(err.message).toContain('discord')
  expect(err._tag).toBe('UnknownIdentity')
})

const runEither = <A, E>(effect: Effect.Effect<A, E>): Either.Either<A, E> =>
  Effect.runSync(Effect.either(effect))

const expectDecodes = <A, E>(effect: Effect.Effect<A, E>, expected: unknown): void => {
  const result = runEither(effect)
  expect(Either.isRight(result)).toBe(true)
  if (Either.isRight(result)) {
    expect(result.right).toBe(expected as A)
  }
}

const expectParseError = <A, E>(effect: Effect.Effect<A, E>): void => {
  const result = runEither(effect)
  expect(Either.isLeft(result)).toBe(true)
  if (Either.isLeft(result)) {
    expect((result.left as { readonly _tag: string })._tag).toBe('ParseError')
  }
}

test('decodeIdentityId succeeds on a non-empty input', () => {
  expectDecodes(decodeIdentityId('value'), 'value')
})

test('decodeIdentityId fails with a ParseError on the empty string', () => {
  expectParseError(decodeIdentityId(''))
})

test('decodeChannelId succeeds on a non-empty input', () => {
  expectDecodes(decodeChannelId('value'), 'value')
})

test('decodeChannelId fails with a ParseError on the empty string', () => {
  expectParseError(decodeChannelId(''))
})

test('decodeChannelName succeeds on a non-empty input', () => {
  expectDecodes(decodeChannelName('value'), 'value')
})

test('decodeChannelName fails with a ParseError on the empty string', () => {
  expectParseError(decodeChannelName(''))
})

test('decodeMessageId succeeds on a non-empty input', () => {
  expectDecodes(decodeMessageId('value'), 'value')
})

test('decodeMessageId fails with a ParseError on the empty string', () => {
  expectParseError(decodeMessageId(''))
})

test('decodeThreadName succeeds on a non-empty input', () => {
  expectDecodes(decodeThreadName('value'), 'value')
})

test('decodeThreadName fails with a ParseError on the empty string', () => {
  expectParseError(decodeThreadName(''))
})

test('decodeMessageBody succeeds on a non-empty input', () => {
  expectDecodes(decodeMessageBody('value'), 'value')
})

test('decodeMessageBody fails with a ParseError on the empty string', () => {
  expectParseError(decodeMessageBody(''))
})

test('decodeDisplayName succeeds on a non-empty input', () => {
  expectDecodes(decodeDisplayName('value'), 'value')
})

test('decodeDisplayName fails with a ParseError on the empty string', () => {
  expectParseError(decodeDisplayName(''))
})

test('decodeBotName succeeds on a non-empty input', () => {
  expectDecodes(decodeBotName('value'), 'value')
})

test('decodeBotName fails with a ParseError on the empty string', () => {
  expectParseError(decodeBotName(''))
})

test('decodeTimestamp succeeds on 0', () => {
  expectDecodes(decodeTimestamp(0), 0)
})

test('decodeTimestamp fails with a ParseError on a negative number', () => {
  expectParseError(decodeTimestamp(-1))
})

test('decodeEmoji accepts a single-character shortcode', () => {
  expectDecodes(decodeEmoji('a'), 'a')
})

test('decodeEmoji accepts a shortcode containing a control char / newline', () => {
  expectDecodes(decodeEmoji('sm\nile'), 'sm\nile')
})

test('decodeEmoji fails with a ParseError on values wrapped in surrounding colons', () => {
  expectParseError(decodeEmoji(':smile:'))
})

test('decodeEmoji fails with a ParseError on a leading colon', () => {
  expectParseError(decodeEmoji(':smile'))
})

test('decodeEmoji fails with a ParseError on a trailing colon', () => {
  expectParseError(decodeEmoji('smile:'))
})

test('decodeEmoji fails with a ParseError on the empty string', () => {
  expectParseError(decodeEmoji(''))
})

test('MessagePublisher.post rejects an unbranded body at the type level (comms-m1y)', () => {
  // A bare `string` for the MessageBody-branded `body` param must not compile.
  // Method-vs-arrow syntax is irrelevant: strictFunctionTypes bivariance only
  // loosens function-type *assignability*, never a direct call-site argument
  // check — so the brand bites even though `post` is declared with method
  // shorthand. comms-m1y was filed believing the opposite (that bivariance
  // let a bare string through here); this proof is the disproof. If it stops
  // firing, the MessageBody brand has genuinely been weakened.
  const proof = (publisher: MessagePublisher, channel: ChannelRef): void => {
    // @ts-expect-error — body must be MessageBody, not string
    void publisher.post(channel, 'raw-unbranded-body')
    void publisher.post(channel, decodeMessageBodySync('branded-ok'))
  }
  expect(proof).toBeTypeOf('function')
})
