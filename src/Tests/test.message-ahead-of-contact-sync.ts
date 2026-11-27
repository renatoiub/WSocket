jest.mock('../Socket/Client', () => {
	const { EventEmitter } = require('events')

	class WebSocketClient extends EventEmitter {
		constructor(
			public url: unknown,
			public config: unknown
		) {
			super()
		}

		get isOpen() {
			return true
		}

		get isClosed() {
			return false
		}

		get isClosing() {
			return false
		}

		get isConnecting() {
			return false
		}

		async connect() {}

		async close() {
			this.emit('close')
		}

		send(_data: unknown, cb?: (err?: Error) => void) {
			cb?.()
			return true
		}
	}

	return { WebSocketClient }
})

import pino from 'pino'
import { proto } from '../../WAProto'
import { DEFAULT_CONNECTION_CONFIG } from '../Defaults'
import { makeMessagesRecvSocket } from '../Socket/messages-recv'
import { AuthenticationState } from '../Types'
import { initAuthCreds } from '../Utils/auth-utils'

const SYNC_TIMEOUT_MS = 1200
const MESSAGE_BUDGET_MS = 400

describe('mensagem recebida durante sync de contatos', () => {
	it('emite a mensagem antes do sync de contatos terminar', async () => {
		const store: Record<string, unknown> = {}
		const auth: AuthenticationState = {
			creds: {
				...initAuthCreds(),
				me: { id: '5599999999999@s.whatsapp.net', name: 'me' }
			},
			keys: {
				get: async (type, ids) => {
					const data: Record<string, any> = {}
					for (const id of ids) {
						const item = store[`${type}.${id}`]
						if (typeof item !== 'undefined') {
							data[id] = item
						}
					}

					return data
				},
				set: async data => {
					for (const type in data) {
						for (const id in data[type]) {
							store[`${type}.${id}`] = data[type]![id]
						}
					}
				}
			}
		}

		const sock = makeMessagesRecvSocket({
			...DEFAULT_CONNECTION_CONFIG,
			auth,
			logger: pino({ level: 'silent' }) as any,
			fireInitQueries: false,
			markOnlineOnConnect: false,
			defaultQueryTimeoutMs: SYNC_TIMEOUT_MS,
			connectTimeoutMs: SYNC_TIMEOUT_MS
		})

		const started = Date.now()
		let messageAt: number | undefined
		sock.ev.on('messages.upsert', () => {
			messageAt = Date.now() - started
		})

		sock.ws.emit('CB:notification', {
			tag: 'notification',
			attrs: {
				from: '@s.whatsapp.net',
				id: 'SYNC1',
				t: '1700000000',
				offline: '1',
				type: 'server_sync'
			},
			content: [{ tag: 'collection', attrs: { name: 'critical_unblock_low' } }]
		})
		sock.ws.emit('CB:message', {
			tag: 'message',
			attrs: {
				from: '5511111111111@s.whatsapp.net',
				id: 'MSG1',
				t: '1700000001',
				offline: '1'
			},
			content: [
				{
					tag: 'plaintext',
					attrs: {},
					content: proto.Message.encode({ conversation: 'oi' }).finish()
				}
			]
		})

		const messageArrived = new Promise<void>((resolve, reject) => {
			const timer = setTimeout(
				() => reject(new Error(`mensagem esperou o sync de contatos por mais de ${MESSAGE_BUDGET_MS}ms`)),
				MESSAGE_BUDGET_MS
			)
			const watch = () => {
				if (messageAt !== undefined) {
					clearTimeout(timer)
					resolve()
					return
				}

				setTimeout(watch, 15)
			}

			watch()
		})

		try {
			await messageArrived
			expect(messageAt).toBeLessThan(MESSAGE_BUDGET_MS)
		} finally {
			sock.end(undefined)
			await new Promise(resolve => setTimeout(resolve, SYNC_TIMEOUT_MS + 200))
		}
	}, 5000)
})
