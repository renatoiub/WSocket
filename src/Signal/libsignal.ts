import * as libsignal from 'libsignal'
import type { CacheStore, SignalAuthState, SignalKeyStoreWithTransaction, SignedKeyPair } from '../Types'
import { SignalRepository } from '../Types/Signal'
import { generateSignalPubKey } from '../Utils'
import logger from '../Utils/logger'
import { FullJid, isHostedLidUser, isHostedPnUser, isLidUser, isPnUser, jidDecode, transferDevice } from '../WABinary'
import { SenderKeyName } from './Group/sender-key-name'
import { SenderKeyRecord } from './Group/sender-key-record'
import { GroupCipher, GroupSessionBuilder, SenderKeyDistributionMessage } from './Group'
import { EncryptionResult, LIDMappingStore, SessionValidationResult } from './lid-mapping'
import { SenderKeyStore } from './Group/group_cipher'
import NodeCache from '@cacheable/node-cache'
import { DEFAULT_CACHE_TTLS } from '../Defaults'

const SIGNAL_CONSTANTS = {
	PREKEY_MESSAGE_TYPE: 3,
	WHATSAPP_DOMAIN: '@s.whatsapp.net',
	LID_DOMAIN: '@lid',
	DEFAULT_DEVICE: 0
} as const

export function makeLibSignalRepository(auth: SignalAuthState): SignalRepository {
	const parsedKeys = auth.keys as SignalKeyStoreWithTransaction
	const lidMapping = new LIDMappingStore(parsedKeys, logger)
	const storage: SenderKeyStore & Record<string, unknown> = signalStorage(auth, lidMapping)

	const migratedSessionCache = new NodeCache({
		stdTTL: DEFAULT_CACHE_TTLS.SESSION_MIGRATION_CACHE, // 1 hour
		useClones: false
	})
	const sessionValidationCache: CacheStore = new NodeCache({
		stdTTL: DEFAULT_CACHE_TTLS.SESSION_VALIDATION_CACHE, // 1 hour
		useClones: false
	})

	/**
	 * Utility function to validate JID format and decode
	 */
	const validateAndDecodeJid = (jid: string): { user: string; device: number } | null => {
		try {
			const decoded: FullJid | undefined = jidDecode(jid)
			if (!decoded?.user) {
				logger.warn({ jid }, 'Invalid JID format')
				return null
			}

			return {
				user: decoded.user,
				device: decoded.device || SIGNAL_CONSTANTS.DEFAULT_DEVICE
			}
		} catch (error) {
			logger.error({ error, jid }, 'Failed to decode JID')
			return null
		}
	}

	/**
	 * Check if JID should use LID for encryption
	 */
	const shouldUseLID = (jid: string): boolean => {
		return jid.includes(SIGNAL_CONSTANTS.WHATSAPP_DOMAIN)
	}

	/**
	 * Get the optimal encryption JID (prefers LID if available)
	 */
	const getOptimalEncryptionJid = async (jid: string): Promise<string> => {
		if (!shouldUseLID(jid)) {
			return jid
		}

		try {
			const lidForPN: string | null = await lidMapping.getLIDForPN(jid)
			if (!lidForPN?.includes(SIGNAL_CONSTANTS.LID_DOMAIN)) {
				return jid
			}

			const lidAddr = jidToSignalProtocolAddress(lidForPN)
			const { [lidAddr.toString()]: lidSession } = await parsedKeys.get('session', [lidAddr.toString()])

			if (lidSession) {
				return lidForPN
			}

			const pnAddr = jidToSignalProtocolAddress(jid)
			const { [pnAddr.toString()]: pnSession } = await parsedKeys.get('session', [pnAddr.toString()])

			if (pnSession) {
				await repository.migrateSession(jid, lidForPN)
				return lidForPN
			}

			return jid
		} catch (error) {
			logger.error({ error, jid }, 'Failed to get optimal encryption JID')
			return jid
		}
	}

	const repository: SignalRepository = {
		decryptGroupMessage({ group, authorJid, msg }) {
			const senderName: SenderKeyName = jidToSignalSenderKeyName(group, authorJid)
			const cipher = new GroupCipher(storage, senderName)

			try {
				return cipher.decrypt(msg)
			} catch (error) {
				throw error
			}
		},
		async processSenderKeyDistributionMessage({ item, authorJid }) {
			const builder = new GroupSessionBuilder(storage)
			if (!item.groupId) {
				throw new Error('Group ID is required for sender key distribution message')
			}

			const senderName: SenderKeyName = jidToSignalSenderKeyName(item.groupId, authorJid)

			const senderMsg = new SenderKeyDistributionMessage(
				null,
				null,
				null,
				null,
				item.axolotlSenderKeyDistributionMessage
			)
			const senderNameStr: string = senderName.toString()
			const { [senderNameStr]: senderKey } = await parsedKeys.get('sender-key', [senderNameStr])
			if (!senderKey) {
				await storage.storeSenderKey(senderName, new SenderKeyRecord())
			}

			await builder.process(senderName, senderMsg)
		},
		async decryptMessage({ jid, type, ciphertext }) {
			const addr = jidToSignalProtocolAddress(jid)
			const session = new libsignal.SessionCipher(storage, addr)
			let result: Buffer

			try {
				switch (type) {
					case 'pkmsg':
						result = await session.decryptPreKeyWhisperMessage(ciphertext)
						break
					case 'msg':
						result = await session.decryptWhisperMessage(ciphertext)
						break
					default:
						throw new Error(`Unknown message type: ${type}`)
				}
			} catch (error) {
				throw error
			}

			return result
		},
		async encryptMessage({ jid, data }): Promise<EncryptionResult> {
			const originalJid = jid
			try {
				const decoded = validateAndDecodeJid(jid)
				if (!decoded) {
					throw new Error(`Invalid JID format: ${jid}`)
				}

				const encryptionJid: string = await getOptimalEncryptionJid(jid)
				logger.trace({ originalJid: jid, encryptionJid }, 'Encryption JID selected')

				const addr = jidToSignalProtocolAddress(encryptionJid)

				const sessionValidation = await repository.validateSession(encryptionJid)
				if (!sessionValidation.exists) {
					logger.warn(
						{ jid: encryptionJid, reason: sessionValidation.reason, originalJid },
						'No valid session for encryption'
					)
					throw new Error(`No valid session for ${encryptionJid}: ${sessionValidation.reason}`)
				}

				const cipher = new libsignal.SessionCipher(storage, addr)

				const { type: sigType, body } = await cipher.encrypt(data)
				const type: 'pkmsg' | 'msg' = sigType === SIGNAL_CONSTANTS.PREKEY_MESSAGE_TYPE ? 'pkmsg' : 'msg'

				logger.trace({ jid: encryptionJid, type, originalJid }, 'Message encrypted successfully')

				return {
					type,
					ciphertext: Buffer.from(body, 'binary')
				}
			} catch (error) {
				logger.error(
					{
						error,
						jid: originalJid,
						errorName: error?.name,
						errorMessage: error?.message
					},
					'Failed to encrypt message'
				)
				throw error
			}
		},

		async encryptGroupMessage({ group, meId, data }) {
			const senderName: SenderKeyName = jidToSignalSenderKeyName(group, meId)
			const builder = new GroupSessionBuilder(storage)
			const senderNameStr: string = senderName.toString()
			const { [senderNameStr]: senderKey } = await parsedKeys.get('sender-key', [senderNameStr])

			if (!senderKey) {
				await storage.storeSenderKey(senderName, new SenderKeyRecord())
			}

			const senderKeyDistributionMessage = await builder.create(senderName)
			const session = new GroupCipher(storage, senderName)
			const ciphertext: Uint8Array = await session.encrypt(data)

			return {
				ciphertext,
				senderKeyDistributionMessage: senderKeyDistributionMessage.serialize()
			}
		},
		async injectE2ESession({ jid, session }) {
			const cipher = new libsignal.SessionBuilder(storage, jidToSignalProtocolAddress(jid))
			await cipher.initOutgoing(session)
		},

		jidToSignalProtocolAddress(jid) {
			return jidToSignalProtocolAddress(jid).toString()
		},

		async storeLIDPNMapping(lid: string, pn: string) {
			await lidMapping.storeLIDPNMapping(lid, pn)
		},

		getLIDMappingStore() {
			return lidMapping
		},

		async validateSession(jid: string): Promise<SessionValidationResult> {
			try {
				const cacheKey = `validation:${jid}`
				const cached: SessionValidationResult | undefined =
					sessionValidationCache.get<SessionValidationResult>(cacheKey)
				if (cached) {
					return cached
				}

				const decoded = validateAndDecodeJid(jid)
				if (!decoded) {
					const result = { exists: false, reason: 'invalid jid format' }
					sessionValidationCache.set(cacheKey, result)
					return result
				}

				const addr = jidToSignalProtocolAddress(jid)
				const session = await (storage as any).loadSession(addr.toString())

				if (!session) {
					const result = { exists: false, reason: 'no session' }
					sessionValidationCache.set(cacheKey, result)
					return result
				}

				if (!session.haveOpenSession()) {
					const result = { exists: false, reason: 'no open session' }
					sessionValidationCache.set(cacheKey, result)
					return result
				}

				const result = { exists: true }
				sessionValidationCache.set(cacheKey, result)
				return result
			} catch (error) {
				logger.error({ error, jid }, 'Session validation error')
				const result = { exists: false, reason: 'validation error' }
				return result
			}
		},

		async deleteSession(jid: string): Promise<void> {
			try {
				const decoded = validateAndDecodeJid(jid)
				if (!decoded) {
					logger.warn({ jid }, 'Cannot delete session for invalid JID')
					return
				}

				const addr = jidToSignalProtocolAddress(jid)

				await parsedKeys.transaction(async () => {
					await parsedKeys.set({ session: { [addr.toString()]: null } })
				})

				sessionValidationCache.del(`validation:${jid}`)

				logger.info({ jid }, 'Session deleted for')
			} catch (error) {
				logger.error({ error, jid }, 'Failed to delete session')
				throw error
			}
		},

		async migrateSession(
			fromJid: string,
			toJid: string
		): Promise<{ migrated: number; skipped: number; total: number }> {
			// TODO: use usync to handle this entire mess
			if (!fromJid || (!isLidUser(toJid) && !isHostedLidUser(toJid))) {
				return { migrated: 0, skipped: 0, total: 0 }
			}

			// Only support PN to LID migration
			if (!isPnUser(fromJid) && !isHostedPnUser(fromJid)) {
				return { migrated: 0, skipped: 0, total: 1 }
			}

			const { user } = jidDecode(fromJid)!

			logger.debug({ fromJid }, 'bulk device migration - loading all user devices')

			// Get user's device list from storage
			const { [user]: userDevices } = await parsedKeys.get('device-list', [user])
			if (!userDevices) {
				return { migrated: 0, skipped: 0, total: 0 }
			}

			const { device: fromDevice } = jidDecode(fromJid)!
			const fromDeviceStr = fromDevice?.toString() || '0'
			if (!userDevices.includes(fromDeviceStr)) {
				userDevices.push(fromDeviceStr)
			}

			// Filter out cached devices before database fetch
			const uncachedDevices = userDevices.filter(device => {
				const deviceKey = `${user}.${device}`
				return !migratedSessionCache.get(deviceKey)
			})

			// Bulk check session existence only for uncached devices
			const deviceSessionKeys = uncachedDevices.map(device => `${user}.${device}`)
			const existingSessions = await parsedKeys.get('session', deviceSessionKeys)

			// Step 3: Convert existing sessions to JIDs (only migrate sessions that exist)
			const deviceJids: string[] = []
			for (const [sessionKey, sessionData] of Object.entries(existingSessions)) {
				if (sessionData) {
					// Session exists in storage
					const deviceStr = sessionKey.split('.')[1]
					if (!deviceStr) {
						continue
					}

					const deviceNum = parseInt(deviceStr)
					let jid = deviceNum === 0 ? `${user}@s.whatsapp.net` : `${user}:${deviceNum}@s.whatsapp.net`
					if (deviceNum === 99) {
						jid = `${user}:99@hosted`
					}

					deviceJids.push(jid)
				}
			}

			logger.debug(
				{
					fromJid,
					totalDevices: userDevices.length,
					devicesWithSessions: deviceJids.length,
					devices: deviceJids
				},
				'bulk device migration complete - all user devices processed'
			)

			// Single transaction for all migrations
			return parsedKeys.transaction(async (): Promise<{ migrated: number; skipped: number; total: number }> => {
				// Prepare migration operations with addressing metadata
				type MigrationOp = {
					fromJid: string
					toJid: string
					pnUser: string
					lidUser: string
					deviceId: number
					fromAddr: libsignal.ProtocolAddress
					toAddr: libsignal.ProtocolAddress
				}

				const migrationOps: MigrationOp[] = deviceJids.map(jid => {
					const lidWithDevice = transferDevice(jid, toJid)
					const fromDecoded: FullJid = jidDecode(jid)!
					const toDecoded: FullJid = jidDecode(lidWithDevice)!

					return {
						fromJid: jid,
						toJid: lidWithDevice,
						pnUser: fromDecoded.user,
						lidUser: toDecoded.user,
						deviceId: fromDecoded.device || 0,
						fromAddr: jidToSignalProtocolAddress(jid),
						toAddr: jidToSignalProtocolAddress(lidWithDevice)
					}
				})

				const totalOps = migrationOps.length
				let migratedCount = 0

				// Bulk fetch PN sessions - already exist (verified during device discovery)
				const pnAddrStrings = Array.from(new Set(migrationOps.map(op => op.fromAddr.toString())))
				const pnSessions = await parsedKeys.get('session', pnAddrStrings)

				// Prepare bulk session updates (PN → LID migration + deletion)
				const sessionUpdates: { [key: string]: Uint8Array | null } = {}

				for (const op of migrationOps) {
					const pnAddrStr = op.fromAddr.toString()
					const lidAddrStr = op.toAddr.toString()

					const pnSession = pnSessions[pnAddrStr]
					if (pnSession) {
						// Session exists (guaranteed from device discovery)
						const fromSession = libsignal.SessionRecord.deserialize(pnSession)
						if (fromSession.haveOpenSession()) {
							// Queue for bulk update: copy to LID, delete from PN
							sessionUpdates[lidAddrStr] = fromSession.serialize()
							sessionUpdates[pnAddrStr] = null

							migratedCount++
						}
					}
				}

				// Single bulk session update for all migrations
				if (Object.keys(sessionUpdates).length > 0) {
					await parsedKeys.set({ session: sessionUpdates })
					logger.debug({ migratedSessions: migratedCount }, 'bulk session migration complete')

					// Cache device-level migrations
					for (const op of migrationOps) {
						if (sessionUpdates[op.toAddr.toString()]) {
							const deviceKey = `${op.pnUser}.${op.deviceId}`
							migratedSessionCache.set(deviceKey, true)
						}
					}
				}

				const skippedCount = totalOps - migratedCount
				return { migrated: migratedCount, skipped: skippedCount, total: totalOps }
			})
		},

		async encryptMessageWithWire({ encryptionJid, wireJid, data }) {
			const result = await repository.encryptMessage({ jid: encryptionJid, data })
			return { ...result, wireJid }
		},

		destroy() {
			try {
				migratedSessionCache.flushAll()
				sessionValidationCache.flushAll()

				logger.trace({}, 'LibSignal repository destroyed and caches cleared')
			} catch (error) {
				logger.error({ error }, 'Error during repository destruction')
			}
		}
	}

	return repository
}

const jidToSignalProtocolAddress = (jid: string) => {
	const { user, device } = jidDecode(jid)!
	return new libsignal.ProtocolAddress(user, device || 0)
}

const jidToSignalSenderKeyName = (group: string, user: string): SenderKeyName => {
	return new SenderKeyName(group, jidToSignalProtocolAddress(user))
}

function signalStorage(
	{ creds, keys }: SignalAuthState,
	lidMapping: LIDMappingStore
): SenderKeyStore & Record<string, unknown> {
	/**
	 * Enhanced session loading with LID preference
	 */
	const loadSessionWithLIDPreference = async (id: string): Promise<any> => {
		try {
			let actualId: string = id

			if (id.includes('.') && !id.includes('_1')) {
				const parts: string[] = id.split('.')
				const device: string = parts[1] || '0'
				const pnJid: string =
					device === '0'
						? `${parts[0]}${SIGNAL_CONSTANTS.WHATSAPP_DOMAIN}`
						: `${parts[0]}:${device}${SIGNAL_CONSTANTS.WHATSAPP_DOMAIN}`

				const lidForPN: string | null = await lidMapping.getLIDForPN(pnJid)
				if (lidForPN?.includes(SIGNAL_CONSTANTS.LID_DOMAIN)) {
					const lidAddr = jidToSignalProtocolAddress(lidForPN)
					const lidId = lidAddr.toString()

					const { [lidId]: lidSession } = await keys.get('session', [lidId])
					if (lidSession) {
						actualId = lidId
					}
				}
			}

			const { [actualId]: sess } = await keys.get('session', [actualId])
			if (sess) {
				return libsignal.SessionRecord.deserialize(sess)
			}

			return null
		} catch (error) {
			logger.error({ error, id }, 'Failed to load session')
			return null
		}
	}

	return {
		loadSession: loadSessionWithLIDPreference,

		storeSession: async (id: string, session: libsignal.SessionRecord): Promise<void> => {
			try {
				await keys.set({ session: { [id]: session.serialize() } })
				logger.trace({ id }, 'Session stored for')
			} catch (error) {
				logger.error({ error, id }, 'Failed to store session')
				throw error
			}
		},

		isTrustedIdentity: (): boolean => {
			return true
		},

		loadPreKey: async (id: number | string): Promise<{ privKey: Buffer; pubKey: Buffer } | undefined> => {
			try {
				const keyId: string = id.toString()
				const { [keyId]: key } = await keys.get('pre-key', [keyId])
				if (key) {
					return {
						privKey: Buffer.from(key.private),
						pubKey: Buffer.from(key.public)
					}
				}

				return undefined
			} catch (error) {
				logger.error({ error, id }, 'Failed to load pre-key')
				return undefined
			}
		},

		removePreKey: async (id: number): Promise<void> => {
			try {
				await keys.set({ 'pre-key': { [id]: null } })
				logger.trace({ id }, 'Pre-key removed')
			} catch (error) {
				logger.error({ error, id }, 'Failed to remove pre-key')
				throw error
			}
		},

		loadSignedPreKey: (): { privKey: Buffer; pubKey: Buffer } => {
			const key: SignedKeyPair = creds.signedPreKey
			return {
				privKey: Buffer.from(key.keyPair.private),
				pubKey: Buffer.from(key.keyPair.public)
			}
		},

		loadSenderKey: async (senderKeyName: SenderKeyName): Promise<SenderKeyRecord> => {
			try {
				const keyId: string = senderKeyName.toString()
				const { [keyId]: key } = await keys.get('sender-key', [keyId])
				if (key) {
					return SenderKeyRecord.deserialize(key)
				}

				return new SenderKeyRecord()
			} catch (error) {
				logger.error({ error, senderKeyName: senderKeyName.toString() }, 'Failed to load sender key')
				return new SenderKeyRecord()
			}
		},

		storeSenderKey: async (senderKeyName: SenderKeyName, key: SenderKeyRecord): Promise<void> => {
			try {
				const keyId: string = senderKeyName.toString()
				const serialized: string = JSON.stringify(key.serialize())
				await keys.set({
					'sender-key': {
						[keyId]: Buffer.from(serialized, 'utf-8')
					}
				})
				logger.trace({ keyId }, 'Sender key stored')
			} catch (error) {
				logger.error({ error, senderKeyName: senderKeyName.toString() }, 'Failed to store sender key')
				throw error
			}
		},

		getOurRegistrationId: (): number => {
			return creds.registrationId
		},

		getOurIdentity: (): { privKey: Buffer; pubKey: Buffer } => {
			const { signedIdentityKey } = creds
			const pubKey: Uint8Array = generateSignalPubKey(signedIdentityKey.public)
			return {
				privKey: Buffer.from(signedIdentityKey.private),
				pubKey: Buffer.isBuffer(pubKey) ? pubKey : Buffer.from(pubKey)
			}
		}
	}
}
