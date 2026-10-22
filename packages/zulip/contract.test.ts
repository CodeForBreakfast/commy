import { runAgentCommsContract, runAttachmentStoreContract } from '@commy/testing/contract'
import { inProcessContractFactory } from './in-process-contract.ts'

runAgentCommsContract('zulip adapter (in-process realm)', inProcessContractFactory)
runAttachmentStoreContract('zulip adapter (in-process realm)', inProcessContractFactory)
