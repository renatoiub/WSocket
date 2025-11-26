import { Boom } from '@hapi/boom'
import { randomBytes } from 'crypto'
import { URL } from 'url'
import { promisify } from 'util'
import { proto } from '../../WAProto'
import {
	DEF_CALLBACK_PREFIX,
	DEF_TAG_PREFIX,
	INITIAL_PREKEY_COUNT,
	MIN_PREKEY_COUNT,
	MIN_UPLOAD_INTERVAL,
	NOISE_WA_HEADER,
	UPLOAD_TIMEOUT
} from '../Defaults'
import { DisconnectReason, SocketConfig } from '../Types'
import {
	addTransactionCapability,
	aesEncryptCTR,
	bindWaitForConnectionUpdate,
	bytesToCrockford,
	configureSuccessfulPairing,
	Curve,
	derivePairingCodeKey,
	generateLoginNode,
	generateMdTagPrefix,
	generateRegistrationNode,
	getCodeFromWSError,
	getErrorCodeFromStreamError,
	getNextPreKeysNode,
	getPlatformId,
	makeEventBuffer,
	makeNoiseHandler,
	printQRIfNecessaryListener,
	promiseTimeout,
	promiseTimeoutEnhanced
} from '../Utils'
import {
	assertNodeErrorFree,
	BinaryNode,
	binaryNodeToString,
	encodeBinaryNode,
	getAllBinaryNodeChildren,
	getBinaryNodeChild,
	getBinaryNodeChildren,
	jidDecode,
	jidEncode,
	S_WHATSAPP_NET
} from '../WABinary'
import { WebSocketClient } from './Client'

/**
 * Connects to WA servers and performs:
 * - simple queries (no retry mechanism, wait for connection establishment)
 * - listen to messages and emit events
 * - query phone connection
 */

export const makeSocket = (config: SocketConfig) => {
	const {
		waWebSocketUrl,
		connectTimeoutMs,
		logger,
		keepAliveIntervalMs,
		browser,
		auth: authState,
		printQRInTerminal,
		defaultQueryTimeoutMs,
		transactionOpts,
		qrTimeout,
		makeSignalRepository
	} = config

	const url = typeof waWebSocketUrl === 'string' ? new URL(waWebSocketUrl) : waWebSocketUrl

	if (config.mobile || url.protocol === 'tcp:') {
		throw new Boom('Mobile API is not supported anymore', { statusCode: DisconnectReason.loggedOut })
	}

	if (url.protocol === 'wss' && authState?.creds?.routingInfo) {
		url.searchParams.append('ED', authState.creds.routingInfo.toString('base64url'))
	}

	const ws = new WebSocketClient(url, config)

	ws.connect()

	const ev = makeEventBuffer(logger)
	const ephemeralKeyPair = Curve.generateKeyPair()
	const noise = makeNoiseHandler({
		keyPair: ephemeralKeyPair,
		NOISE_HEADER: NOISE_WA_HEADER,
		logger,
		routingInfo: authState?.creds?.routingInfo
	})

	const { creds } = authState
	const keys = addTransactionCapability(authState.keys, logger, transactionOpts)
	const signalRepository = makeSignalRepository({ creds, keys })

	let lastDateRecv: Date
	let epoch = 1
	let keepAliveReq: NodeJS.Timeout
	let qrTimer: NodeJS.Timeout
	let closed = false

	const uqTagId = generateMdTagPrefix()
	const generateMessageTag = () => `${uqTagId}${epoch++}`

	const sendPromise = promisify(ws.send)

	const sendRawMessage = async (data: Uint8Array | Buffer) => {
		if (!ws.isOpen) {
			throw new Boom('Connection Closed', { statusCode: DisconnectReason.connectionClosed })
		}

		const bytes = noise.encodeFrame(data)
		await promiseTimeout<void>(connectTimeoutMs, async (resolve, reject) => {
			try {
				await sendPromise.call(ws, bytes)
				resolve()
			} catch (error) {
				reject(error)
			}
		})
	}

	const sendNode = (frame: BinaryNode) => {
		if (logger.level === 'trace') {
			logger.trace({ xml: binaryNodeToString(frame), msg: 'xml send' })
		}

		const buff = encodeBinaryNode(frame)
		return sendRawMessage(buff)
	}

	const onUnexpectedError = (err: Error | Boom, msg: string) => {
		logger.error({ err }, `unexpected error in '${msg}'`)
	}

	const awaitNextMessage = async <T>(sendMsg?: Uint8Array) => {
		if (!ws.isOpen) {
			throw new Boom('Connection Closed', {
				statusCode: DisconnectReason.connectionClosed
			})
		}

		let onOpen: (data: T) => void
		let onClose: (err: Error) => void

		const result = promiseTimeout<T>(connectTimeoutMs, (resolve, reject) => {
			onOpen = resolve
			onClose = mapWebSocketError(reject)
			ws.on('frame', onOpen)
			ws.on('close', onClose)
			ws.on('error', onClose)
		}).finally(() => {
			ws.off('frame', onOpen)
			ws.off('close', onClose)
			ws.off('error', onClose)
		})

		if (sendMsg) {
			sendRawMessage(sendMsg).catch(onClose!)
		}

		return result
	}

	/**
	 * Wait for a message with a certain tag to be received
	 * @param msgId the message tag to await
	 * @param timeoutMs timeout after which the promise will reject
	 */
	const waitForMessage = async <T>(msgId: string, timeoutMs = defaultQueryTimeoutMs) => {
		let onRecv: (json) => void
		let onErr: (err) => void
		try {
			return await promiseTimeout<T>(timeoutMs, (resolve, reject) => {
				onRecv = resolve
				onErr = err => {
					reject(err || new Boom('Connection Closed', { statusCode: DisconnectReason.connectionClosed }))
				}

				ws.on(`TAG:${msgId}`, onRecv)
				ws.on('close', onErr)
				ws.off('error', onErr)
			})
		} finally {
			ws.off(`TAG:${msgId}`, onRecv!)
			ws.off('close', onErr!)
			ws.off('error', onErr!)
		}
	}

	/**
	 * Enhanced wait for message with adaptive timeouts
	 * @param msgId the message tag to await
	 * @param operationType the type of operation for adaptive timeout
	 * @param customTimeoutMs custom timeout, overrides adaptive timeout
	 */
	const waitForMessageEnhanced = async <T>(
		msgId: string,
		operationType: 'group-metadata' | 'send-message' | 'query' | 'default' = 'default',
		customTimeoutMs?: number
	) => {
		let onRecv: (json) => void
		let onErr: (err) => void
		try {
			return await promiseTimeoutEnhanced<T>(
				operationType,
				(resolve, reject) => {
					onRecv = resolve
					onErr = err => {
						reject(err || new Boom('Connection Closed', { statusCode: DisconnectReason.connectionClosed }))
					}

					ws.on(`TAG:${msgId}`, onRecv)
					ws.on('close', onErr)
					ws.off('error', onErr)
				},
				customTimeoutMs
			)
		} finally {
			ws.off(`TAG:${msgId}`, onRecv!)
			ws.off('close', onErr!)
			ws.off('error', onErr!)
		}
	}

	const query = async (node: BinaryNode, timeoutMs?: number) => {
		if (!node.attrs.id) {
			node.attrs.id = generateMessageTag()
		}

		const msgId = node.attrs.id
		const wait = waitForMessage(msgId, timeoutMs)

		await sendNode(node)

		const result = await (wait as Promise<BinaryNode>)
		if ('tag' in result) {
			assertNodeErrorFree(result)
		}

		return result
	}

	const queryEnhanced = async (
		node: BinaryNode,
		operationType: 'group-metadata' | 'send-message' | 'query' | 'default' = 'query',
		customTimeoutMs?: number
	) => {
		if (!node.attrs.id) {
			node.attrs.id = generateMessageTag()
		}

		const msgId = node.attrs.id
		const wait = waitForMessageEnhanced(msgId, operationType, customTimeoutMs)

		await sendNode(node)

		const result = await (wait as Promise<BinaryNode>)
		if ('tag' in result) {
			assertNodeErrorFree(result)
		}

		return result
	}

	const validateConnection = async () => {
		let helloMsg: proto.IHandshakeMessage = {
			clientHello: { ephemeral: ephemeralKeyPair.public }
		}
		helloMsg = proto.HandshakeMessage.fromObject(helloMsg)

		logger.trace({ browser, helloMsg }, 'connected to WA')

		const init: Uint8Array = proto.HandshakeMessage.encode(helloMsg).finish()

		const result: Uint8Array = await awaitNextMessage<Uint8Array>(init)
		const handshake = proto.HandshakeMessage.decode(result)

		logger.trace({ handshake }, 'handshake recv from WA')

		const keyEnc: Buffer = await noise.processHandshake(handshake, creds.noiseKey)

		let node: proto.IClientPayload
		if (!creds.me) {
			node = generateRegistrationNode(creds, config)
			logger.trace({ node }, 'not logged in, attempting registration...')
		} else {
			node = generateLoginNode(creds.me.id, config)
			logger.trace({ node }, 'logging in...')
		}

		const payloadEnc: Buffer = noise.encrypt(proto.ClientPayload.encode(node).finish())
		await sendRawMessage(
			proto.HandshakeMessage.encode({
				clientFinish: {
					static: keyEnc,
					payload: payloadEnc
				}
			}).finish()
		)
		noise.finishInit()
		startKeepAliveRequest()
	}

	const getAvailablePreKeysOnServer = async () => {
		const result = await query({
			tag: 'iq',
			attrs: {
				id: generateMessageTag(),
				xmlns: 'encrypt',
				type: 'get',
				to: S_WHATSAPP_NET
			},
			content: [{ tag: 'count', attrs: {} }]
		})
		const countChild = getBinaryNodeChild(result, 'count')
		return +countChild!.attrs.value
	}

	let uploadPreKeysPromise: Promise<void> | null = null
	let lastUploadTime = 0
	let uploadDebounceTimer: NodeJS.Timeout | null = null

	const uploadPreKeys = async (count = INITIAL_PREKEY_COUNT, retryCount = 0) => {
		if (uploadDebounceTimer) {
			clearTimeout(uploadDebounceTimer)
		}

		return new Promise<void>((resolve, reject) => {
			uploadDebounceTimer = setTimeout(async () => {
				if (retryCount === 0) {
					const timeSinceLastUpload = Date.now() - lastUploadTime
					if (timeSinceLastUpload < MIN_UPLOAD_INTERVAL) {
						logger.debug({}, `Skipping upload, only ${timeSinceLastUpload}ms since last upload`)
						resolve()
						return
					}
				}

				if (uploadPreKeysPromise) {
					logger.debug({}, 'Pre-key upload already in progress, waiting for completion')
					try {
						await uploadPreKeysPromise
						resolve()
					} catch (error) {
						reject(error)
					}

					return
				}

				const uploadLogic = async () => {
					logger.info({ count, retryCount }, 'uploading pre-keys')

					const node = await keys.transaction(async () => {
						logger.debug({ requestedCount: count }, 'generating pre-keys with requested count')
						const { update, node } = await getNextPreKeysNode({ creds, keys }, count)
						ev.emit('creds.update', update)
						return node
					})

					try {
						await query(node)
						logger.info({ count }, 'uploaded pre-keys successfully')
						lastUploadTime = Date.now()
					} catch (uploadError) {
						logger.error({ uploadError, count }, 'Failed to upload pre-keys to server')

						if (retryCount < 3) {
							const backoffDelay = Math.min(1000 * Math.pow(2, retryCount), 10000)
							logger.info({}, `Retrying pre-key upload in ${backoffDelay}ms`)
							await new Promise(resolve => setTimeout(resolve, backoffDelay))
							return uploadPreKeys(count, retryCount + 1)
						}

						throw uploadError
					}
				}

				uploadPreKeysPromise = Promise.race([
					uploadLogic(),
					new Promise<void>((_, reject) =>
						setTimeout(() => reject(new Boom('Pre-key upload timeout', { statusCode: 408 })), UPLOAD_TIMEOUT)
					)
				])

				try {
					await uploadPreKeysPromise
					resolve()
				} catch (error) {
					reject(error)
				} finally {
					uploadPreKeysPromise = null
					uploadDebounceTimer = null
				}
			}, 100)
		})
	}

	const verifyCurrentPreKeyExists = async () => {
		const currentPreKeyId = creds.nextPreKeyId - 1
		if (currentPreKeyId <= 0) {
			return { exists: false, currentPreKeyId: 0 }
		}

		const preKeys = await keys.get('pre-key', [currentPreKeyId.toString()])
		const exists = !!preKeys[currentPreKeyId.toString()]

		return { exists, currentPreKeyId }
	}

	const uploadPreKeysToServerIfRequired = async () => {
		try {
			const preKeyCount = await getAvailablePreKeysOnServer()
			const { exists: currentPreKeyExists, currentPreKeyId } = await verifyCurrentPreKeyExists()

			logger.debug({ preKeyCount }, `${preKeyCount} pre-keys found on server`)
			logger.debug(
				{ currentPreKeyId, currentPreKeyExists },
				`Current prekey ID: ${currentPreKeyId}, exists in storage: ${currentPreKeyExists}`
			)

			const lowServerCount = preKeyCount <= MIN_PREKEY_COUNT
			const missingCurrentPreKey = !currentPreKeyExists && currentPreKeyId > 0

			const shouldUpload = lowServerCount || missingCurrentPreKey

			if (shouldUpload) {
				const reasons: string[] = []
				if (lowServerCount) {
					reasons.push(`server count low (${preKeyCount})`)
				}

				if (missingCurrentPreKey) {
					reasons.push(`current prekey ${currentPreKeyId} missing from storage`)
				}

				logger.debug({ reasons }, `Uploading PreKeys due to: ${reasons.join(', ')}`)
				await uploadPreKeys()
			} else {
				logger.debug(
					{ preKeyCount, currentPreKeyId },
					`PreKey validation passed - Server: ${preKeyCount}, Current prekey ${currentPreKeyId} exists`
				)
			}
		} catch (error) {
			logger.error({ error }, 'Failed to check/upload pre-keys during initialization')
		}
	}

	const onMessageReceived = (data: Buffer) => {
		noise.decodeFrame(data, frame => {
			lastDateRecv = new Date()

			let anyTriggered = false

			anyTriggered = ws.emit('frame', frame)
			if (!(frame instanceof Uint8Array)) {
				const msgId = frame.attrs.id

				if (logger.level === 'trace') {
					logger.trace({ xml: binaryNodeToString(frame), msg: 'recv xml' })
				}

				anyTriggered = ws.emit(`${DEF_TAG_PREFIX}${msgId}`, frame) || anyTriggered
				const l0 = frame.tag
				const l1 = frame.attrs || {}
				const l2 = Array.isArray(frame.content) ? frame.content[0]?.tag : ''

				for (const key of Object.keys(l1)) {
					anyTriggered = ws.emit(`${DEF_CALLBACK_PREFIX}${l0},${key}:${l1[key]},${l2}`, frame) || anyTriggered
					anyTriggered = ws.emit(`${DEF_CALLBACK_PREFIX}${l0},${key}:${l1[key]}`, frame) || anyTriggered
					anyTriggered = ws.emit(`${DEF_CALLBACK_PREFIX}${l0},${key}`, frame) || anyTriggered
				}

				anyTriggered = ws.emit(`${DEF_CALLBACK_PREFIX}${l0},,${l2}`, frame) || anyTriggered
				anyTriggered = ws.emit(`${DEF_CALLBACK_PREFIX}${l0}`, frame) || anyTriggered

				if (!anyTriggered && logger.level === 'debug') {
					logger.debug({ unhandled: true, msgId, fromMe: false, frame }, 'communication recv')
				}
			}
		})
	}

	const end = (error: Error | undefined) => {
		if (closed) {
			logger.trace({ trace: error?.stack }, 'connection already closed')
			return
		}

		closed = true
		logger.trace({ trace: error?.stack }, error ? 'connection errored' : 'connection closed')

		clearInterval(keepAliveReq)
		clearTimeout(qrTimer)

		ws.removeAllListeners('close')
		ws.removeAllListeners('error')
		ws.removeAllListeners('open')
		ws.removeAllListeners('message')

		if (!ws.isClosed && !ws.isClosing) {
			try {
				ws.close()
			} catch {}
		}

		ev.emit('connection.update', {
			connection: 'close',
			lastDisconnect: {
				error,
				date: new Date()
			}
		})
		ev.removeAllListeners('connection.update')
	}

	const waitForSocketOpen = async () => {
		if (ws.isOpen) {
			return
		}

		if (ws.isClosed || ws.isClosing) {
			throw new Boom('Connection Closed', { statusCode: DisconnectReason.connectionClosed })
		}

		let onOpen: () => void
		let onClose: (err: Error) => void
		await new Promise((resolve, reject) => {
			onOpen = () => resolve(undefined)
			onClose = mapWebSocketError(reject)
			ws.on('open', onOpen)
			ws.on('close', onClose)
			ws.on('error', onClose)
		}).finally(() => {
			ws.off('open', onOpen)
			ws.off('close', onClose)
			ws.off('error', onClose)
		})
	}

	const startKeepAliveRequest = () => {
		let consecutiveFailures = 0
		const MAX_FAILURES = 3

		return (keepAliveReq = setInterval(() => {
			if (!lastDateRecv) {
				lastDateRecv = new Date()
			}

			const diff = Date.now() - lastDateRecv.getTime()
			/*
				check if it's been a suspicious amount of time since the server responded with our last seen
				it could be that the network is down
			*/
			if (diff > keepAliveIntervalMs + 5000) {
				end(new Boom('Connection was lost', { statusCode: DisconnectReason.connectionLost }))
			} else if (ws.isOpen) {
				query({
					tag: 'iq',
					attrs: {
						id: generateMessageTag(),
						to: S_WHATSAPP_NET,
						type: 'get',
						xmlns: 'w:p'
					},
					content: [{ tag: 'ping', attrs: {} }]
				})
					.then(() => {
						consecutiveFailures = 0
					})
					.catch(err => {
						consecutiveFailures++
						logger.error(
							{
								trace: err.stack,
								consecutiveFailures,
								maxFailures: MAX_FAILURES
							},
							'error in sending keep alive'
						)

						if (consecutiveFailures >= MAX_FAILURES) {
							logger.warn({}, 'Too many keep-alive failures, ending connection')
							end(
								new Boom('Keep-alive failures exceeded threshold', {
									statusCode: DisconnectReason.connectionLost
								})
							)
						}
					})
			} else {
				logger.warn({}, 'keep alive called when WS not open')
			}
		}, keepAliveIntervalMs))
	}

	const sendPassiveIq = (tag: 'passive' | 'active') =>
		query({
			tag: 'iq',
			attrs: {
				to: S_WHATSAPP_NET,
				xmlns: 'passive',
				type: 'set'
			},
			content: [{ tag, attrs: {} }]
		})

	const logout = async (msg?: string) => {
		const jid = authState.creds.me?.id
		if (jid) {
			await sendNode({
				tag: 'iq',
				attrs: {
					to: S_WHATSAPP_NET,
					type: 'set',
					id: generateMessageTag(),
					xmlns: 'md'
				},
				content: [
					{
						tag: 'remove-companion-device',
						attrs: {
							jid,
							reason: 'user_initiated'
						}
					}
				]
			})
		}

		end(new Boom(msg || 'Intentional Logout', { statusCode: DisconnectReason.loggedOut }))
	}

	const requestPairingCode = async (phoneNumber: string, customPairingCode?: string): Promise<string> => {
		const pairingCode: string = customPairingCode ?? bytesToCrockford(randomBytes(5))

		if (customPairingCode && customPairingCode?.length !== 8) {
			throw new Error('Custom pairing code must be exactly 8 chars')
		}

		authState.creds.pairingCode = pairingCode
		authState.creds.me = {
			id: jidEncode(phoneNumber, 's.whatsapp.net'),
			name: '~'
		}
		ev.emit('creds.update', authState.creds)
		await sendNode({
			tag: 'iq',
			attrs: {
				to: S_WHATSAPP_NET,
				type: 'set',
				id: generateMessageTag(),
				xmlns: 'md'
			},
			content: [
				{
					tag: 'link_code_companion_reg',
					attrs: {
						jid: authState.creds.me.id,
						stage: 'companion_hello',
						should_show_push_notification: 'true'
					},
					content: [
						{
							tag: 'link_code_pairing_wrapped_companion_ephemeral_pub',
							attrs: {},
							content: await generatePairingKey()
						},
						{
							tag: 'companion_server_auth_key_pub',
							attrs: {},
							content: authState.creds.noiseKey.public
						},
						{
							tag: 'companion_platform_id',
							attrs: {},
							content: getPlatformId(browser[1])
						},
						{
							tag: 'companion_platform_display',
							attrs: {},
							content: `${browser[1]} (${browser[0]})`
						},
						{
							tag: 'link_code_pairing_nonce',
							attrs: {},
							content: '0'
						}
					]
				}
			]
		})
		return authState.creds.pairingCode
	}

	async function generatePairingKey() {
		const salt: Buffer = randomBytes(32)
		const randomIv: Buffer = randomBytes(16)
		const key: Buffer = await derivePairingCodeKey(authState.creds.pairingCode!, salt)
		const ciphered: Buffer = aesEncryptCTR(authState.creds.pairingEphemeralKeyPair.public, key, randomIv)
		return Buffer.concat([salt, randomIv, ciphered])
	}

	const sendWAMBuffer = (wamBuffer: Buffer) => {
		return query({
			tag: 'iq',
			attrs: {
				to: S_WHATSAPP_NET,
				id: generateMessageTag(),
				xmlns: 'w:stats'
			},
			content: [
				{
					tag: 'add',
					attrs: {},
					content: wamBuffer
				}
			]
		})
	}

	ws.on('message', onMessageReceived)

	ws.on('open', async () => {
		try {
			await validateConnection()
		} catch (err) {
			logger.error({ err }, 'error in validating connection')
			end(err)
		}
	})
	ws.on('error', mapWebSocketError(end))
	ws.on('close', () => end(new Boom('Connection Terminated', { statusCode: DisconnectReason.connectionClosed })))
	ws.on('CB:xmlstreamend', () =>
		end(new Boom('Connection Terminated by Server', { statusCode: DisconnectReason.connectionClosed }))
	)
	ws.on('CB:iq,type:set,pair-device', async (stanza: BinaryNode) => {
		const iq: BinaryNode = {
			tag: 'iq',
			attrs: {
				to: S_WHATSAPP_NET,
				type: 'result',
				id: stanza.attrs.id
			}
		}
		await sendNode(iq)

		const pairDeviceNode = getBinaryNodeChild(stanza, 'pair-device')
		const refNodes = getBinaryNodeChildren(pairDeviceNode, 'ref')
		const noiseKeyB64 = Buffer.from(creds.noiseKey.public).toString('base64')
		const identityKeyB64 = Buffer.from(creds.signedIdentityKey.public).toString('base64')
		const advB64 = creds.advSecretKey

		let qrMs = qrTimeout || 60_000
		const genPairQR = () => {
			if (!ws.isOpen) {
				return
			}

			const refNode = refNodes.shift()
			if (!refNode) {
				end(new Boom('QR refs attempts ended', { statusCode: DisconnectReason.timedOut }))
				return
			}

			const ref = (refNode.content as Buffer).toString('utf-8')
			const qr = [ref, noiseKeyB64, identityKeyB64, advB64].join(',')

			ev.emit('connection.update', { qr })

			qrTimer = setTimeout(genPairQR, qrMs)
			qrMs = qrTimeout || 20_000
		}

		genPairQR()
	})
	ws.on('CB:iq,,pair-success', async (stanza: BinaryNode) => {
		logger.debug({}, 'pair success recv')
		try {
			const { reply, creds: updatedCreds } = configureSuccessfulPairing(stanza, creds)

			logger.trace(
				{ me: updatedCreds.me, platform: updatedCreds.platform },
				'pairing configured successfully, expect to restart the connection...'
			)

			ev.emit('creds.update', updatedCreds)
			ev.emit('connection.update', { isNewLogin: true, qr: undefined })

			await sendNode(reply)
		} catch (error) {
			logger.error({ trace: error.stack }, 'error in pairing')
			end(error)
		}
	})

	ws.on('CB:success', async (node: BinaryNode) => {
		await uploadPreKeysToServerIfRequired()
		await sendPassiveIq('active')

		logger.trace({}, 'opened connection to WA')
		clearTimeout(qrTimer)

		ev.emit('creds.update', { me: { ...authState.creds.me!, lid: node.attrs.lid } })

		ev.emit('connection.update', { connection: 'open' })

		if (node.attrs.lid && authState.creds.me?.id) {
			const myLID = node.attrs.lid
			process.nextTick(async () => {
				try {
					const myPN = authState.creds.me!.id

					// Store our own LID-PN mapping
					await signalRepository.getLIDMappingStore().storeLIDPNMappings([{ lidUser: myLID, pnUser: myPN }])

					// Create device list for our own user (needed for bulk migration)
					const { user, device } = jidDecode(myPN)!
					await authState.keys.set({
						'device-list': {
							[user]: [device?.toString() || '0']
						}
					})

					// migrate our own session
					await signalRepository.migrateSession(myPN, myLID)

					logger.debug({ myPN, myLID }, 'Own LID session created successfully')
				} catch (error) {
					logger.error({ error, lid: myLID }, 'Failed to create own LID session')
				}
			})
		}
	})

	ws.on('CB:stream:error', (node: BinaryNode) => {
		const [reasonNode] = getAllBinaryNodeChildren(node)
		logger.error({ node }, 'stream errored out')

		const { reason, statusCode } = getErrorCodeFromStreamError(node)

		end(new Boom(`Stream Errored (${reason})`, { statusCode, data: reasonNode || node }))
	})

	ws.on('CB:failure', (node: BinaryNode) => {
		const reason = +(node.attrs.reason || 500)
		end(new Boom('Connection Failure', { statusCode: reason, data: node.attrs }))
	})

	ws.on('CB:ib,,downgrade_webclient', () => {
		end(new Boom('Multi-device beta not joined', { statusCode: DisconnectReason.multideviceMismatch }))
	})

	ws.on('CB:ib,,offline_preview', (node: BinaryNode) => {
		logger.trace(node, 'offline preview received')
		sendNode({
			tag: 'ib',
			attrs: {},
			content: [{ tag: 'offline_batch', attrs: { count: '100' } }]
		})
	})

	ws.on('CB:ib,,edge_routing', (node: BinaryNode) => {
		const edgeRoutingNode = getBinaryNodeChild(node, 'edge_routing')
		const routingInfo = getBinaryNodeChild(edgeRoutingNode, 'routing_info')
		if (routingInfo?.content) {
			authState.creds.routingInfo = Buffer.from(routingInfo?.content as Uint8Array)
			ev.emit('creds.update', authState.creds)
		}
	})

	let didStartBuffer = false
	process.nextTick(() => {
		if (creds.me?.id) {
			ev.buffer()
			didStartBuffer = true
		}

		ev.emit('connection.update', { connection: 'connecting', receivedPendingNotifications: false, qr: undefined })
	})

	ws.on('CB:ib,,offline', (node: BinaryNode) => {
		const child = getBinaryNodeChild(node, 'offline')
		const offlineNotifs = +(child?.attrs.count || 0)

		logger.trace({ offlineNotifs }, `handled ${offlineNotifs} offline messages/notifications`)
		if (didStartBuffer) {
			ev.flush()
			logger.trace({}, 'flushed events for initial buffer')
		}

		ev.emit('connection.update', { receivedPendingNotifications: true })
	})

	ev.on('creds.update', update => {
		const name = update.me?.name
		if (creds.me?.name !== name) {
			logger.debug({ name }, 'updated pushName')
			sendNode({
				tag: 'presence',
				attrs: { name: name! }
			}).catch(err => {
				logger.error({ trace: err.stack }, 'error in sending presence update on name change')
			})
		}

		Object.assign(creds, update)
	})

	if (printQRInTerminal) {
		printQRIfNecessaryListener(ev, logger)
	}

	return {
		type: 'md' as 'md',
		ws,
		ev,
		authState: { creds, keys },
		signalRepository,
		get user() {
			return authState.creds.me
		},
		generateMessageTag,
		query,
		queryEnhanced,
		waitForMessage,
		waitForMessageEnhanced,
		waitForSocketOpen,
		sendRawMessage,
		sendNode,
		logout,
		end,
		onUnexpectedError,
		uploadPreKeys,
		uploadPreKeysToServerIfRequired,
		requestPairingCode,
		waitForConnectionUpdate: bindWaitForConnectionUpdate(ev),
		sendWAMBuffer
	}
}

/**
 * map the websocket error to the right type
 * so it can be retried by the caller
 * */
function mapWebSocketError(handler: (err: Error) => void) {
	return (error: Error) => {
		handler(new Boom(`WebSocket Error (${error?.message})`, { statusCode: getCodeFromWSError(error), data: error }))
	}
}

export type Socket = ReturnType<typeof makeSocket>
