import { Boom } from '@hapi/boom'
import readline from 'readline'
import { randomBytes } from 'crypto'
import makeWASocket, { AnyMessageContent, BinaryInfo, delay, DisconnectReason, encodeWAM, isJidNewsletter, makeCacheableSignalKeyStore, proto, useMultiFileAuthState, WAMessageContent, WAMessageKey, jidNormalizedUser, extractMessageContent, getContentType, downloadMediaMessage, printQRIfNecessaryListener, fetchLatestBaileysVersion } from '../src'
import fs from 'fs'
import logger from '../src/Utils/logger'

const usePairingCode = process.argv.includes('--use-pairing-code')
const rl = readline.createInterface({ input: process.stdin, output: process.stdout })
const question = (text: string) => new Promise<string>((resolve) => rl.question(text, resolve))
const startSock = async () => {
	const { state, saveCreds } = await useMultiFileAuthState('baileys_auth_info')
	const { version, isLatest } = await fetchLatestBaileysVersion()
	logger.info({ version, isLatest }, 'using WA version')
	const sock = makeWASocket({
		version,
		logger,
		auth: {
			creds: state.creds,
			keys: makeCacheableSignalKeyStore(state.keys, logger),
		},
		generateHighQualityLinkPreview: true,
		getMessage,
		syncFullHistory: true,
	})

	if (usePairingCode && !sock.authState.creds.registered) {
		const phoneNumber: string = await question('Please enter your phone number:\n')
		const code: string = await sock.requestPairingCode(phoneNumber)
		logger.info({ code }, 'Pairing code:')
	}

	const sendMessageWTyping = async (msg: AnyMessageContent, jid: string) => {
		await sock.presenceSubscribe(jid)
		await delay(500)

		await sock.sendPresenceUpdate('composing', jid)
		await delay(2000)

		await sock.sendPresenceUpdate('paused', jid)

		await sock.sendMessage(jid, msg)
	}

	sock.ev.process(
		async (events) => {
			if (events['connection.update']) {
				const update = events['connection.update']
				const { connection, lastDisconnect } = update
				if (connection === 'close') {
					if ((lastDisconnect?.error as Boom)?.output?.statusCode !== DisconnectReason.loggedOut) {
						startSock()
					} else {
						try {
							await fs.promises.unlink('baileys_auth_info/creds.json')
							logger.info({ path: 'baileys_auth_info/creds.json' }, 'Removed creds.json due to logout')
						} catch (err: any) {
							if (err && err.code === 'ENOENT') {
								logger.info({ path: 'baileys_auth_info/creds.json' }, 'creds.json not found during logout cleanup')
							} else {
								logger.warn({ err }, 'Failed to remove creds.json during logout cleanup')
							}
						}
						logger.info({}, 'Connection closed. You are logged out.')
					}
				}

				const sendWAMExample = false;
				if (connection === 'open' && sendWAMExample) {
					/// sending WAM EXAMPLE
					const {
						header: {
							wamVersion,
							eventSequenceNumber,
						},
						events,
					} = JSON.parse(await fs.promises.readFile("./boot_analytics_test.json", "utf-8"))

					const binaryInfo = new BinaryInfo({
						protocolVersion: wamVersion,
						sequence: eventSequenceNumber,
						events: events
					})

					const buffer = encodeWAM(binaryInfo);

					const result = await sock.sendWAMBuffer(buffer)
				}

				if (update.qr) {
					printQRIfNecessaryListener(sock.ev, logger)
					const website: string = "https://quickchart.io/qr?text=" + encodeURIComponent(update.qr)
					logger.info({ website }, 'QR code received, open in browser:')
				}
			}

			if (events['creds.update']) {
				await saveCreds()
			}

			if (events['labels.association']) {}


			if (events['labels.edit']) {}

			if (events.call) {}

			if (events['messaging-history.set']) {
				const { chats, contacts, messages, isLatest, progress, syncType } = events['messaging-history.set']
				if (syncType === proto.HistorySync.HistorySyncType.ON_DEMAND) {
					logger.info({ messages }, 'received on-demand history sync')
				}
				logger.info({ chats, contacts, messages, isLatest, progress, syncType }, 'received messaging history')
			}

			if (events['messages.upsert']) {
				const upsert = events['messages.upsert']
				logger.info({ upsert }, 'Message upsert event received')

				try {
					const messages = (upsert as any).messages || []
					for (const msg of messages) {
						try {
							const content = extractMessageContent(msg.message)
							const ctype = getContentType(content)
							if (ctype === 'imageMessage' || ctype === 'videoMessage' || ctype === 'audioMessage' || ctype === 'documentMessage') {
								const buffer = await downloadMediaMessage(msg as any, 'buffer', { options: {} }).catch(err => {
									logger?.warn({ err, key: msg.key }, 'Failed to download media')
									return null
								})
								if (buffer) {
									const jidSafe = (msg.key?.remoteJid || 'unknown').replace(/[@:]/g, '_')
									const id = msg.key?.id || Date.now().toString()
									const filePath = `Media/${jidSafe}_${id}_image.jpg`
									await fs.promises.mkdir('Media', { recursive: true })
									await fs.promises.writeFile(filePath, buffer)
									logger?.info({ filePath, key: msg.key }, 'Saved image from message')
								}
							}
						} catch (err) {
							logger?.trace({ err }, 'error processing message for media download')
						}
					}
				} catch (err) {
					logger?.error({ err }, 'messages.upsert handler failed')
				}

				if (upsert.type === 'notify') {
					for (const msg of upsert.messages as any[]) {
						if (msg.message?.conversation || msg.message?.extendedTextMessage?.text) {
							const text = msg.message?.conversation || msg.message?.extendedTextMessage?.text

							if (text == "!lid") {
								try {
									const lid = msg.key.sender_lid || msg.key.remoteJid!
									const me = sock.user
									await sendMessageWTyping({
										text: `Enviado pelo ${jidNormalizedUser(lid)}\n\nSeu lid: ${jidNormalizedUser(lid)}\nMeu lid: ${jidNormalizedUser(me?.lid)}`
									}, lid);
								} catch (error) {
									console.error('Erro ao processar comando "lid":', error);
									await sendMessageWTyping({
										text: `Erro ao processar comando. Usando JID original: ${msg.key.remoteJid!}`
									}, msg.key.remoteJid!);
								}
							}

							if (text == "!jid") {
								try {
									const jid = msg.key.sender_pn || msg.key.remoteJid!
									const me = sock.user
									await sendMessageWTyping({
										text: `Enviado pelo ${jidNormalizedUser(jid)}\n\nSeu jid: ${jidNormalizedUser(jid)}\nMeu jid: ${jidNormalizedUser(me?.id)}`,
									}, jid);
								} catch (error) {
									console.error('Erro ao processar comando "jid":', error);
									await sendMessageWTyping({
										text: `Erro ao processar comando. JID: ${msg.key.remoteJid!}`
									}, msg.key.remoteJid!);
								}
							}

							// TEXTO SIMPLES
							if (text === "!text") {
								await sock.readMessages([msg.key]);
								await sendMessageWTyping({
									text: "Esta é uma mensagem de texto simples!"
								}, msg.key.remoteJid!);
							}

							// TEXTO COM FORMATAÇÃO
							if (text === "!format") {
								await sock.readMessages([msg.key]);
								await sendMessageWTyping({
									text: "*Texto em negrito*\n_Texto em itálico_\n~Texto riscado~\n```Texto monoespaçado```\n> Citação"
								}, msg.key.remoteJid!);
							}

							// TEXTO COM MENÇÕES
							if (text === "!mention") {
								await sock.readMessages([msg.key]);
								await sendMessageWTyping({
									text: `Olá @${msg.key.remoteJid!.split('@')[0]}! Como você está?`,
									mentions: [msg.key.remoteJid!]
								}, msg.key.remoteJid!);
							}

							// IMAGEM
							if (text === "!image") {
								await sock.readMessages([msg.key]);
								await sendMessageWTyping({
									image: { url: 'https://picsum.photos/400/300' },
									caption: 'Esta é uma imagem de exemplo!'
								}, msg.key.remoteJid!);
							}

							// VÍDEO
							if (text === "!video") {
								await sock.readMessages([msg.key]);
								await sendMessageWTyping({
									video: { url: 'https://sample-videos.com/video321/mp4/720/big_buck_bunny_720p_1mb.mp4' },
									caption: 'Este é um vídeo de exemplo!'
								}, msg.key.remoteJid!);
							}

							// ÁUDIO
							if (text === "!audio") {
								await sock.readMessages([msg.key]);
								try {
									await sendMessageWTyping({
										audio: fs.readFileSync('./Media/sonata.mp3'),
										mimetype: 'audio/mp4'
									}, msg.key.remoteJid!);
								} catch (error) {
									await sendMessageWTyping({
										text: 'Erro: Arquivo de áudio não encontrado. Certifique-se de que ./Media/sonata.mp3 existe.'
									}, msg.key.remoteJid!);
								}
							}

							// ÁUDIO COMO NOTA DE VOZ
							if (text === "!voice") {
								await sock.readMessages([msg.key]);
								try {
									await sendMessageWTyping({
										audio: fs.readFileSync('./Media/sonata.mp3'),
										mimetype: 'audio/mp4',
										ptt: true
									}, msg.key.remoteJid!);
								} catch (error) {
									await sendMessageWTyping({
										text: 'Erro: Arquivo de áudio não encontrado. Certifique-se de que ./Media/sonata.mp3 existe.'
									}, msg.key.remoteJid!);
								}
							}

							// DOCUMENTO
							if (text === "!document") {
								await sock.readMessages([msg.key]);
								await sendMessageWTyping({
									document: Buffer.from("Conteúdo do documento de exemplo"),
									fileName: 'exemplo.txt',
									mimetype: 'text/plain',
									caption: 'Este é um documento de exemplo!'
								}, msg.key.remoteJid!);
							}

							// STICKER
							if (text === "!sticker") {
								await sock.readMessages([msg.key]);
								try {
									await sendMessageWTyping({
										sticker: fs.readFileSync('./Media/octopus.webp')
									}, msg.key.remoteJid!);
								} catch (error) {
									await sendMessageWTyping({
										text: 'Erro: Arquivo de sticker não encontrado. Certifique-se de que ./Media/octopus.webp existe.'
									}, msg.key.remoteJid!);
								}
							}

							// LOCALIZAÇÃO
							if (text === "!location") {
								await sock.readMessages([msg.key]);
								await sendMessageWTyping({
									location: {
										degreesLatitude: -23.550520,
										degreesLongitude: -46.633308,
										name: "São Paulo, SP",
										address: "São Paulo, Estado de São Paulo, Brasil"
									}
								}, msg.key.remoteJid!);
							}

							// CONTATO
							if (text === "!contact") {
								await sock.readMessages([msg.key]);
								await sendMessageWTyping({
									contacts: {
										displayName: "Contato de Exemplo",
										contacts: [{
											displayName: "João Silva",
											vcard: `BEGIN:VCARD\nVERSION:3.0\nN:Silva;João;;;\nFN:João Silva\nTEL;TYPE=CELL:+5511999999999\nEND:VCARD`
										}]
									}
								}, msg.key.remoteJid!);
							}

							// REAÇÃO
							if (text === "!react") {
								await sock.readMessages([msg.key]);
								await sendMessageWTyping({
									react: {
										text: "👍",
										key: msg.key
									}
								}, msg.key.remoteJid!);
							}

							// POLL (ENQUETE)
							if (text === "!poll") {
								await sock.readMessages([msg.key]);
								await sendMessageWTyping({
									poll: {
										name: "Qual sua cor favorita?",
										values: ["🔴 Vermelho", "🔵 Azul", "🟢 Verde", "🟡 Amarelo"],
										selectableCount: 1
									}
								}, msg.key.remoteJid!);
							}

							// EDITAR MENSAGEM
							if (text === "!edit") {
								await sock.readMessages([msg.key]);
								// Primeiro envia uma mensagem
								const sentMsg = await sock.sendMessage(msg.key.remoteJid!, {
									text: "Esta mensagem será editada em 3 segundos..."
								});
								// Aguarda 3 segundos e edita
								setTimeout(async () => {
									if (sentMsg?.key) {
										await sock.sendMessage(msg.key.remoteJid!, {
											text: "Mensagem editada! ✏️",
											edit: sentMsg.key
										});
									}
								}, 3000);
							}

							// DELETAR MENSAGEM
							if (text === "!delete") {
								await sock.readMessages([msg.key]);
								// Primeiro envia uma mensagem
								const sentMsg = await sock.sendMessage(msg.key.remoteJid!, {
									text: "Esta mensagem será deletada em 3 segundos..."
								});
								// Aguarda 3 segundos e deleta
								setTimeout(async () => {
									if (sentMsg?.key) {
										await sock.sendMessage(msg.key.remoteJid!, {
											delete: sentMsg.key
										});
									}
								}, 3000);
							}

							// VIEW ONCE (VISUALIZAÇÃO ÚNICA)
							if (text === "!viewonce") {
								await sock.readMessages([msg.key]);
								await sendMessageWTyping({
									image: { url: 'https://picsum.photos/400/300' },
									caption: 'Esta imagem só pode ser vista uma vez!',
									viewOnce: true
								}, msg.key.remoteJid!);
							}

							// FORWARD (ENCAMINHAR)
							if (text === "!forward") {
								await sock.readMessages([msg.key]);
								await sendMessageWTyping({
									forward: msg
								}, msg.key.remoteJid!);
							}

							// MENSAGENS EPHEMERAL (TEMPORÁRIAS)
							if (text === "!ephemeral") {
								await sock.readMessages([msg.key]);
								await sendMessageWTyping({
									disappearingMessagesInChat: 86400 // 24 horas
								}, msg.key.remoteJid!);
								await sendMessageWTyping({
									text: "Esta mensagem desaparecerá em 24 horas!"
								}, msg.key.remoteJid!);
							}

							// === COMANDOS AVANÇADOS ===

							// GRUPO - CONVITE
							if (text === "!groupinvite") {
								await sock.readMessages([msg.key]);
								if (msg.key.remoteJid!.endsWith('@g.us')) {
									try {
										const code = await sock.groupInviteCode(msg.key.remoteJid!);
										await sendMessageWTyping({
											groupInvite: {
												inviteCode: code!,
												inviteExpiration: Date.now() + 86400000, // 24 horas
												text: "Convite para o grupo",
												jid: msg.key.remoteJid!,
												subject: "Grupo de Exemplo"
											}
										}, msg.key.remoteJid!);
									} catch (error) {
										await sendMessageWTyping({
											text: "Erro: Não foi possível gerar convite do grupo ou não tenho permissão."
										}, msg.key.remoteJid!);
									}
								} else {
									await sendMessageWTyping({
										text: "Este comando só funciona em grupos!"
									}, msg.key.remoteJid!);
								}
							}

							// STATUS BROADCAST
							if (text === "!status") {
								await sock.readMessages([msg.key]);
								try {
									await sock.sendMessage('status@broadcast', {
										text: "Esta é uma mensagem de status! 📢"
									});
									await sendMessageWTyping({
										text: "Status enviado com sucesso! ✅"
									}, msg.key.remoteJid!);
								} catch (error) {
									await sendMessageWTyping({
										text: "Erro ao enviar status. Verifique as permissões."
									}, msg.key.remoteJid!);
								}
							}

							// NEWSLETTER (Se suportado)
							if (text === "!newsletter") {
								await sock.readMessages([msg.key]);
								try {
									if (isJidNewsletter(msg.key.remoteJid!)) {
										await sendMessageWTyping({
											text: "Esta é uma mensagem para newsletter! 📰"
										}, msg.key.remoteJid!);
									} else {
										await sendMessageWTyping({
											text: "Este comando só funciona em newsletters!"
										}, msg.key.remoteJid!);
									}
								} catch (error) {
									await sendMessageWTyping({
										text: "Newsletters podem não estar disponíveis nesta conta."
									}, msg.key.remoteJid!);
								}
							}

							// COMPARTILHAR NÚMERO DE TELEFONE
							if (text === "!sharenumber") {
								await sock.readMessages([msg.key]);
								await sendMessageWTyping({
									sharePhoneNumber: true
								}, msg.key.remoteJid!);
							}

							// SOLICITAR NÚMERO DE TELEFONE
							if (text === "!requestnumber") {
								await sock.readMessages([msg.key]);
								await sendMessageWTyping({
									requestPhoneNumber: true
								}, msg.key.remoteJid!);
							}

							// PIN MESSAGE (FIXAR MENSAGEM)
							if (text === "!pin") {
								await sock.readMessages([msg.key]);
								const sentMsg = await sock.sendMessage(msg.key.remoteJid!, {
									text: "Esta mensagem será fixada!"
								});
								if (sentMsg?.key) {
									setTimeout(async () => {
										await sock.sendMessage(msg.key.remoteJid!, {
											pin: sentMsg.key,
											type: proto.PinInChat.Type.PIN_FOR_ALL,
											time: 86400 // 24 horas
										});
									}, 2000);
								}
							}

							// POLL AVANÇADO
							if (text === "!polladvanced") {
								await sock.readMessages([msg.key]);
								await sendMessageWTyping({
									poll: {
										name: "📊 Enquete Avançada - Múltipla Escolha",
										values: [
											"🔥 Opção 1 - Muito interessante",
											"⭐ Opção 2 - Interessante",
											"👍 Opção 3 - Regular",
											"👎 Opção 4 - Não gostei",
											"❌ Opção 5 - Terrível"
										],
										selectableCount: 2, // Permite selecionar até 2 opções
										messageSecret: randomBytes(32) // Criptografia da enquete
									}
								}, msg.key.remoteJid!);
							}

							if (text === "!resyncapp") {
								await sock.readMessages([msg.key]);
								try {
									await sock.resyncAppState(["critical_block", "critical_unblock_low", "regular_high", "regular_low", "regular"], true);
								} catch (error) {
									console.error('Error resyncing app state:', error);
								}
							}

							// HELP - LISTA TODOS OS COMANDOS
							if (text === "!help" || text === "!comandos") {
								await sock.readMessages([msg.key]);
								const helpText = `
🤖 *COMANDOS DISPONÍVEIS* 🤖

📝 *TEXTO:*
!text - Texto simples
!format - Texto formatado
!mention - Texto com menção

📷 *MÍDIA:*
!image - Enviar imagem
!video - Enviar vídeo
!audio - Enviar áudio
!voice - Nota de voz
!document - Documento
!sticker - Sticker
!viewonce - Imagem visualização única

📍 *LOCALIZAÇÃO:*
!location - Localização
~!livelocation - Localização ao vivo~
~!event - Evento~
~!eventaudio -	Evento com áudio~
~!eventvideo - Evento com áudio/vídeo~

👤 *CONTATO:*
!contact - Compartilhar contato

💬 *INTERAÇÃO:*
!react - Reagir mensagem
!poll - Criar enquete
!polladvanced - Enquete avançada
~!buttons - Botões~
~!list - Lista de opções~
~!template - Template buttons~
~!interactive - Mensagem interativa~
~!allbuttons - Todos tipos de botão~

✏️ *AÇÕES:*
!edit - Editar mensagem
!delete - Deletar mensagem
!forward - Encaminhar mensagem
!ephemeral - Mensagem temporária
!pin - Fixar mensagem
!keep - Manter no chat

🔧 *SISTEMA:*
!jid - Mostrar JID
!lid - Mostrar LID
!device - Device message
!sharenumber - Compartilhar número
!requestnumber - Solicitar número

👥 *GRUPO/STATUS:*
!groupinvite - Convite do grupo
!status - Enviar status
!newsletter - Mensagem newsletter

📋 *AJUDA:*
!help - Esta ajuda
!comandos - Lista de comandos
`;
								await sendMessageWTyping({
									text: helpText
								}, msg.key.remoteJid!);
							}
						}
					}
				}
			}

			if (events['messages.update']) {
				/* logger.info(
					JSON.stringify(events['messages.update'], undefined, 2)
				) */

				for (const { key, update } of events['messages.update']) {
					if (update.pollUpdates) {
						const pollCreation: proto.IMessage = {}
						if (pollCreation) {

						}
					}
				}
			}

			if (events['chats.update']) {}

			if (events['contacts.upsert']) {
				for (const contact of events['contacts.upsert']) {}
			}

			if (events['contacts.update']) {
				for (const contact of events['contacts.update']) {}
			}

			if (events['chats.delete']) {}
		}
	)

	return sock

	async function getMessage(key: WAMessageKey): Promise<WAMessageContent | undefined> {
		// Implement a way to retreive messages that were upserted from messages.upsert
		// up to you

		// only if store is present
		return proto.Message.fromObject({})
	}
}

startSock()
