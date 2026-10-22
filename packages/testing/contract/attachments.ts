/** The shared contract's tests for AttachmentStore. */

import { expect, test } from 'bun:test'
import { decodeAttachmentRefSync } from '@commy/core/ports'
import { Array as Arr, Effect } from 'effect'
import type { AttachmentContractEnv } from '../contract.ts'

// Every byte value, so a store that decodes the payload as text or trims it
// cannot pass.
const EVERY_BYTE = Uint8Array.from(Arr.range(0, 255))

export const attachmentsContract = (env: () => AttachmentContractEnv): void => {
  test('attachments.downloadFile returns the bytes uploadFile stored', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const uploaded = yield* env().attachments.uploadFile('payload.bin', EVERY_BYTE)
        const downloaded = yield* env().attachments.downloadFile(uploaded.ref)
        expect(downloaded.data).toEqual(EVERY_BYTE)
      }),
    ))
  test('attachments.downloadFile names the file as uploadFile reported it', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const uploaded = yield* env().attachments.uploadFile(
          'notes.txt',
          new TextEncoder().encode('hello'),
        )
        const downloaded = yield* env().attachments.downloadFile(uploaded.ref)
        expect(downloaded.filename).toBe(uploaded.filename)
      }),
    ))
  test('attachments.downloadFile returns a file whose name is not ASCII', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const data = new TextEncoder().encode('bonjour')
        const uploaded = yield* env().attachments.uploadFile('café.txt', data)
        const downloaded = yield* env().attachments.downloadFile(uploaded.ref)
        expect(downloaded.data).toEqual(data)
      }),
    ))
  // A ref is opaque above the port, so a caller can hand over one the realm
  // never issued. Two shapes: one built from nothing, and one that looks like
  // an issued ref but names no file.
  test('attachments.downloadFile fails with a download AttachmentError on a ref the realm never issued', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const uploaded = yield* env().attachments.uploadFile(
          'real.txt',
          new TextEncoder().encode('real'),
        )
        const neverIssued = [
          decodeAttachmentRefSync('never-issued'),
          decodeAttachmentRefSync(`${uploaded.ref}-never-issued`),
        ]
        for (const ref of neverIssued) {
          const error = yield* Effect.flip(env().attachments.downloadFile(ref))
          expect(error._tag).toBe('AttachmentError')
          expect(error.operation).toBe('download')
        }
      }),
    ))
  // Eight climbs are more than any issued ref is deep, so each ref climbs to
  // the realm's root.
  test('attachments.downloadFile fails with a download AttachmentError on a ref that climbs out of an issued one', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const uploaded = yield* env().attachments.uploadFile(
          'real.txt',
          new TextEncoder().encode('real'),
        )
        const climbs = ['..', '%2e%2e', '%2E%2E', '.%2e', '..%2f..', '..%5c..']
        for (const climb of climbs) {
          const ref = decodeAttachmentRefSync(
            `${uploaded.ref}/${`${climb}/`.repeat(8)}api/v1/users`,
          )
          const error = yield* Effect.flip(env().attachments.downloadFile(ref))
          expect(error._tag).toBe('AttachmentError')
          expect(error.operation).toBe('download')
        }
      }),
    ))
}
