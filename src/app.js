import dotenv from 'dotenv'
dotenv.config({ override: true })

import { createBot, createProvider, createFlow, addKeyword, EVENTS, MemoryDB } from '@builderbot/bot'
import { BaileysProvider } from '@builderbot/provider-baileys'
import Groq from 'groq-sdk'
import pg from 'pg'
import fs from 'node:fs'
import path from 'node:path'
import crypto from 'node:crypto'

const { Pool } = pg

// Keep the bot alive if one message fails or times out
process.on('unhandledRejection', (err) => console.error('⚠️ Unhandled rejection:', err?.message || err))
process.on('uncaughtException', (err) => console.error('⚠️ Uncaught exception:', err?.message || err))

// =============================================================
// 🐘 1. POSTGRESQL (SUPABASE) CONFIGURATION & TABLES
// =============================================================
const dbConfig = {
    host: 'aws-0-eu-west-1.pooler.supabase.com',
    user: 'postgres.tkdofzunxwmenziqnqma',
    password: 'wU7p?+35Qd&7z%!',
    database: 'postgres',
    port: 6543,
    ssl: { rejectUnauthorized: false }
}

// Shows which database the bot is really using (password hidden)
console.log('DB config:', { ...dbConfig, password: '***' })

const pool = new Pool({
    ...dbConfig,
    max: 5,
    connectionTimeoutMillis: 10000,
    query_timeout: 15000,
    idleTimeoutMillis: 30000
})
pool.on('error', (err) => console.error('❌ Unexpected PG pool error:', err.message))

async function initDbTables() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS chat_history (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(50) NOT NULL,
                role VARCHAR(20) NOT NULL,
                content TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        `)

        await pool.query(`
            CREATE TABLE IF NOT EXISTS customer_orders (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(50) NOT NULL,
                order_details TEXT NOT NULL,
                status VARCHAR(20) DEFAULT 'Pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        `)

        await pool.query(`
            CREATE TABLE IF NOT EXISTS inventory (
                id SERIAL PRIMARY KEY,
                category VARCHAR(50) NOT NULL,
                item_name VARCHAR(100) NOT NULL,
                price VARCHAR(50) NOT NULL,
                stock_quantity INT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        `)

        await pool.query(`
            CREATE TABLE IF NOT EXISTS invoices (
                id SERIAL PRIMARY KEY,
                invoice_no VARCHAR(40),
                user_id VARCHAR(50) NOT NULL,
                customer_name VARCHAR(100),
                customer_phone VARCHAR(50),
                delivery_location TEXT,
                items JSONB NOT NULL,
                total NUMERIC(12,2) NOT NULL,
                payment_method VARCHAR(50),
                payment_status VARCHAR(80),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        `)

        await pool.query(`
            CREATE TABLE IF NOT EXISTS wa_session_files (
                file_name TEXT PRIMARY KEY,
                content TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        `)
        // Private: blocks Supabase's public API (anon key). The bot connects as the database owner, so it still works.
        await pool.query('ALTER TABLE wa_session_files ENABLE ROW LEVEL SECURITY')

        await pool.query(`
            CREATE TABLE IF NOT EXISTS daily_reports (
                report_date DATE PRIMARY KEY,
                invoices INT,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        `)
        await pool.query('ALTER TABLE daily_reports ENABLE ROW LEVEL SECURITY')

        await pool.query(`
            CREATE TABLE IF NOT EXISTS bot_pauses (
                chat_id TEXT PRIMARY KEY,
                paused_until BIGINT NOT NULL
            );
        `)
        await pool.query('ALTER TABLE bot_pauses ENABLE ROW LEVEL SECURITY')

        console.log("✅ Supabase PostgreSQL tables ('chat_history', 'customer_orders', 'inventory', 'invoices', 'wa_session_files', 'daily_reports', 'bot_pauses') ready!")
    } catch (err) {
        console.error("❌ Error initializing Supabase database tables:", err.message)
    }
}

async function saveChatMessageToPg(userId, role, content) {
    try {
        await pool.query(
            'INSERT INTO chat_history (user_id, role, content) VALUES ($1, $2, $3)',
            [userId, role, content]
        )
    } catch (err) {
        console.error("❌ Error saving chat history to PG:", err.message)
    }
}

async function getChatHistoryFromPg(userId, limit = 12) {
    try {
        const res = await pool.query(
            'SELECT role, content FROM chat_history WHERE user_id = $1 ORDER BY id DESC LIMIT $2',
            [userId, limit]
        )
        return res.rows.reverse()
    } catch (err) {
        console.error("❌ Error reading chat history from PG:", err.message)
        return []
    }
}

async function getLiveInventoryFromDb() {
    try {
        const res = await pool.query('SELECT category, item_name, price, stock_quantity FROM inventory ORDER BY id')
        return res.rows
    } catch (err) {
        console.error("❌ Error fetching inventory:", err.message)
        return []
    }
}

async function logCustomerOrderToPg(userId, orderDetails) {
    try {
        await pool.query(
            'INSERT INTO customer_orders (user_id, order_details) VALUES ($1, $2)',
            [userId, orderDetails]
        )
        console.log(`📦 Customer order logged in Supabase for user: ${userId}`)
    } catch (err) {
        console.error("❌ Error logging customer order to PG:", err.message)
    }
}

// =============================================================
// 💾 WHATSAPP SESSION BACKUP — keeps you logged in on hosts that wipe their disk (e.g. Render free)
// =============================================================
const SESSION_DIR = path.resolve('bot_sessions')
const syncedFiles = new Map() // file name -> content already saved in Supabase
let sessionSyncRunning = false

async function restoreSessionFromDb() {
    try {
        const res = await pool.query('SELECT file_name, content FROM wa_session_files')
        if (!res.rows.length) {
            console.log('ℹ️ No saved WhatsApp session in Supabase yet (a QR code will be needed once).')
            return
        }
        fs.mkdirSync(SESSION_DIR, { recursive: true })
        for (const row of res.rows) {
            fs.writeFileSync(path.join(SESSION_DIR, path.basename(row.file_name)), row.content, 'utf8')
            syncedFiles.set(path.basename(row.file_name), row.content)
        }
        console.log(`✅ Restored WhatsApp session (${res.rows.length} files) from Supabase`)
    } catch (err) {
        console.error('❌ Could not restore WhatsApp session:', err.message)
    }
}

async function backupSessionToDb() {
    if (sessionSyncRunning || !fs.existsSync(SESSION_DIR)) return
    sessionSyncRunning = true
    try {
        const files = fs.readdirSync(SESSION_DIR).filter((f) => f.endsWith('.json'))

        for (const file of files) {
            let content
            try {
                content = fs.readFileSync(path.join(SESSION_DIR, file), 'utf8')
                JSON.parse(content) // skip files that are still being written
            } catch {
                continue
            }
            if (syncedFiles.get(file) === content) continue
            await pool.query(
                `INSERT INTO wa_session_files (file_name, content) VALUES ($1, $2)
                 ON CONFLICT (file_name) DO UPDATE SET content = EXCLUDED.content, updated_at = NOW()`,
                [file, content]
            )
            syncedFiles.set(file, content)
        }

        for (const file of [...syncedFiles.keys()]) {
            if (!files.includes(file)) {
                await pool.query('DELETE FROM wa_session_files WHERE file_name = $1', [file])
                syncedFiles.delete(file)
            }
        }
    } catch (err) {
        console.error('❌ WhatsApp session backup failed:', err.message)
    } finally {
        sessionSyncRunning = false
    }
}

// =============================================================
// 🔗 2. CATALOG URL
// =============================================================
const DIGITAL_CATALOG_URL = 'https://www.snedashopping.com/'

// Shop details printed on invoices (edit these)
const STORE_NAME = 'SNEDA Shopping Center'
const STORE_ADDRESS = 'Accra, Ghana'
const STORE_PHONE = '0543812261' // optional, e.g. '+233 24 000 0000'
const OWNER_NUMBER = '' // optional: WhatsApp number that gets a copy of every invoice, e.g. '233241234567'
const REPORT_TIME = '21:30' // daily sales report time, Accra time (24-hour HH:MM)
const REPORT_FORMATS = ['xlsx', 'pdf'] // choose any of: 'xlsx' (Excel), 'pdf'
const HUMAN_TAKEOVER = true // pause the bot in a chat when a person replies from the phone
const HUMAN_PAUSE_MINUTES = 30 // the bot stays quiet until the person has been silent this long
const PAYMENT_DETAILS = 'MTN MoMo: 0257575551 (SNEDA Shopping Center)' // shown to customers exactly as written

// Owner numbers (OWNER_NUMBER can hold several, separated by commas) receive order alerts and daily reports
const OWNER_NUMBERS = OWNER_NUMBER.split(',').map((n) => n.replace(/\D/g, '')).filter(Boolean)
const isOwner = (from) => OWNER_NUMBERS.includes(String(from).replace(/\D/g, ''))

// =============================================================
// ⚙️ 3. GROQ AI WITH SUPABASE LIVE INVENTORY & MEMORY
// =============================================================
const apiKey = process.env.GROQ_API_KEY
const groq = apiKey ? new Groq({ apiKey, timeout: 20000, maxRetries: 1 }) : null

// =============================================================
// 🧮 ORDER MATHS — done in code, never by the AI
// =============================================================
const parsePrice = (p) => {
    const n = parseFloat(String(p).replace(/[^0-9.]/g, ''))
    return Number.isFinite(n) ? n : null
}
const round2 = (n) => Math.round(n * 100) / 100
const money = (n) => {
    const [whole, dec] = round2(n).toFixed(2).split('.')
    return `GH₵ ${whole.replace(/\B(?=(\d{3})+(?!\d))/g, ',')}.${dec}`
}

const looksLikeOrder = (msg, history) =>
    /\d|order|buy|want|need|take|get|pay|momo|mobile money|cash|deliver|total|how much/i.test(msg) ||
    history.slice(-6).some((m) => m.role === 'user' && /\d/.test(m.content))

// Remembers each customer's order, so later messages ("mobile money", "1",
// an address) can never change the quantity they already gave.
const userCarts = new Map() // userId -> { items: [{ item_name, quantity }], at }
const CART_TTL_MS = 2 * 60 * 60 * 1000

const getCart = (userId) => {
    const cart = userCarts.get(userId)
    if (!cart) return null
    if (Date.now() - cart.at > CART_TTL_MS) {
        userCarts.delete(userId)
        return null
    }
    return cart
}

const NUMBER_WORDS = /\b(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|dozen|twenty|thirty|forty|fifty|hundred)\b/i
const isBareChoice = (m) => /^\s*(?:option|number|no\.?|choice|#)?\s*\d{1,2}\s*[.)]?\s*(?:please|pls|plz|thanks|thank you)?\s*[.!]*\s*$/i.test(m)
const isPaymentOnly = (m) =>
    /(momo|mobile money|cash|visa|master ?card|telecel|\bat money\b|\bpay\b|payment)/i.test(m) && !/\d/.test(m)

const detectPayment = (m) => {
    if (/(momo|mobile money|telecel|\bat money\b)/i.test(m)) return 'Mobile Money'
    if (/(cash|on delivery)/i.test(m)) return 'Cash on Delivery'
    if (/(visa|master ?card|\bcard\b)/i.test(m)) return 'Card'
    return null
}

// Customer is asking where to send the money / for the payment number
const asksForPaymentNumber = (m) =>
    /\b(number|account|details|momo no|where (?:do|should|can) i (?:pay|send)|how (?:do|can|should) i pay)\b/i.test(m)

// Payment instructions are written by the code (not the AI), so the number is always correct
const buildPaymentReply = (method, total) => {
    if (method === 'Mobile Money') {
        return [
            `Great! You'll pay by *Mobile Money*.`,
            '',
            `• *Amount:* ${money(total)}`,
            `• *Send to:* ${PAYMENT_DETAILS}`,
            '',
            `When you've sent the money, reply *PAID* and we'll generate your invoice.`
        ].join('\n')
    }
    return null
}

function noteCartDetails(userId, message) {
    const cart = getCart(userId)
    const method = detectPayment(message)
    if (cart && method) cart.payment = method
}

// Prices a list of { item_name, quantity } against the live inventory (pure maths, no AI)
function priceItems(items, inventory) {
    const lines = []
    let total = 0
    for (const it of items) {
        const row = inventory.find((r) => r.item_name.toLowerCase() === it.item_name.toLowerCase())
        const unit = row ? parsePrice(row.price) : null
        if (!row || unit === null) continue
        const subtotal = round2(unit * it.quantity)
        total = round2(total + subtotal)
        lines.push({ item: row.item_name, qty: it.quantity, unit, subtotal, stock: row.stock_quantity, enough: row.stock_quantity >= it.quantity })
    }
    return lines.length ? { lines, total } : null
}

// The AI only READS the order (item + quantity). The code does the multiplication.
async function calculateOrder(userId, history, userMessage, inventory) {
    if (!groq || inventory.length === 0) return null

    const saved = getCart(userId)
    let items = saved ? saved.items : []

    // Menu choices ("1") and payment answers ("mobile money") never change the order
    const skipAI = saved && (isBareChoice(userMessage) || isPaymentOnly(userMessage))

    if (!skipAI) {
        try {
            const recent = history.slice(-8).map((m) => `${m.role}: ${m.content}`).join('\n')
            const names = inventory.map((i) => i.item_name)

            const res = await groq.chat.completions.create({
                model: 'openai/gpt-oss-20b',
                reasoning_effort: 'low',
                temperature: 0,
                max_tokens: 500,
                response_format: { type: 'json_object' },
                messages: [
                    {
                        role: 'system',
                        content:
                            'You extract shopping orders. Reply with JSON only, in this shape: ' +
                            '{"items":[{"item_name":"<exact name from the list>","quantity":<whole number>}]}. ' +
                            'Use ONLY names from this list: ' + JSON.stringify(names) + '. ' +
                            'Return the FULL current order. ' +
                            'Previous order (keep it exactly as it is unless the latest message clearly changes it): ' +
                            JSON.stringify(items) + '. ' +
                            'Only change a quantity when the customer clearly says how many units of an item they want ' +
                            '(for example "make it 5", "10 boxes of Indomie", "add 2 Milo"). ' +
                            'Menu choices like "1" or "2", phone numbers, addresses, house numbers and payment answers are NOT quantities. ' +
                            'If the customer clearly starts a different order, return only the new items. ' +
                            'If nothing has been chosen yet, reply {"items":[]}. Never calculate prices.'
                    },
                    { role: 'user', content: `Conversation so far:\n${recent}\n\nLatest customer message: ${userMessage}` }
                ]
            }, { timeout: 8000, maxRetries: 0 })

            const text = (res.choices[0]?.message?.content || '').replace(/```json|```/g, '').trim()
            const parsed = JSON.parse(text || '{}')
            const found = Array.isArray(parsed.items) ? parsed.items : []

            const clean = []
            for (const it of found) {
                const row = inventory.find((r) => r.item_name.toLowerCase() === String(it.item_name).toLowerCase())
                const qty = parseInt(it.quantity, 10)
                if (!row || !qty || qty < 1 || qty > 10000) continue
                clean.push({ item_name: row.item_name, quantity: qty })
            }

            if (clean.length) {
                // A saved quantity can only change if the customer actually typed a number
                // (phone numbers are ignored for this check)
                const withoutPhones = userMessage.replace(/\+?\d[\d\s-]{6,}\d/g, '')
                const messageHasNumber = /\d/.test(withoutPhones) || NUMBER_WORDS.test(withoutPhones)
                items = clean.map((c) => {
                    const old = (saved?.items || []).find((o) => o.item_name === c.item_name)
                    return old && old.quantity !== c.quantity && !messageHasNumber ? old : c
                })
            }
        } catch (err) {
            console.warn('⚠️ Order reading skipped:', err?.message)
        }
    }

    if (!items.length) return null
    userCarts.set(userId, { ...(saved || {}), items, at: Date.now() })
    return priceItems(items, inventory)
}

function formatOrderBlock(calc) {
    const lines = calc.lines.map(
        (l) =>
            `- ${l.qty} × ${l.item} @ ${money(l.unit)} = ${money(l.subtotal)}` +
            (l.enough ? '' : ` ⚠️ ONLY ${l.stock} IN STOCK — tell the customer`)
    )
    return `
VERIFIED ORDER CALCULATION (calculated by the system — use these exact numbers):
${lines.join('\n')}
TOTAL FOR ITEMS: ${money(calc.total)}
(Delivery fee is not included unless the store information above states one.)
`
}

// =============================================================
// 🅱️ WHATSAPP FORMATTING — turns AI markdown into real WhatsApp bold
// =============================================================
// WhatsApp bolds *single asterisks*. AI models often write **double asterisks**,
// which WhatsApp shows literally. This cleans every AI reply before it is sent.
const BOLD_ALL_REPLIES = false // set to true to make EVERY line of AI replies bold

function formatForWhatsApp(text) {
    let out = String(text)
        .replace(/\r/g, '')
        .replace(/^[ \t]*[-*_]{3,}[ \t]*$/gm, '') // horizontal rules
        .replace(/^[ \t]*\|?[ \t]*:?-{2,}:?[ \t]*(\|[ \t]*:?-{2,}:?[ \t]*)+\|?[ \t]*$\n?/gm, '') // table separator rows
        .replace(/^[ \t]*\|(.+)\|[ \t]*$/gm, (_, row) =>
            row.split('|').map((c) => c.trim()).filter(Boolean).join(' — ')) // table rows
        .replace(/\*\*\s*([^*\n]+?)\s*\*\*/g, '*$1*') // **bold** -> *bold*
        .replace(/__\s*([^_\n]+?)\s*__/g, '*$1*') // __bold__ -> *bold*
        .replace(/^#{1,6}[ \t]*(.+)$/gm, (_, t) => `*${t.replace(/\*/g, '').trim()}*`) // # headers -> bold
        .replace(/^[ \t]*[-•][ \t]+/gm, '• ') // bullets
        .replace(/\n{3,}/g, '\n\n')
        .trim()

    if (BOLD_ALL_REPLIES) {
        out = out
            .split('\n')
            .map((line) => {
                const t = line.replace(/\*/g, '').trim()
                return t ? `*${t}*` : ''
            })
            .join('\n')
    }
    return out
}

// =============================================================
// 🔢 ITEM NUMBERS — lets customers reply with the number shown next to an item
// =============================================================
const userLists = new Map() // userId -> { rows, at, awaiting }
const LIST_TTL_MS = 60 * 60 * 1000

// Call this right after showing a numbered list to a customer
function setActiveList(userId, rows) {
    userLists.set(userId, { rows, at: Date.now(), awaiting: true })
}

// Turns "3", "3 please", "1 and 3", "number 3, 5 pieces" into the real item names
function expandListReferences(userId, text) {
    const list = userLists.get(userId)
    if (!list) return text
    if (Date.now() - list.at > LIST_TTL_MS) {
        userLists.delete(userId)
        return text
    }
    const nameOf = (n) => list.rows[n - 1]?.item_name
    const numbersIn = (str) => (str.match(/\d{1,3}/g) || []).map(Number)

    // 1) the whole message is just number(s): "3", "number 3", "1 and 3", "2 please"
    const only = /^\s*(items?|numbers?|nos?\.?|options?|#)?\s*(\d{1,3}(?:\s*(?:,|and|&|\+)\s*\d{1,3})*)\s*(?:please|pls|plz)?\s*[.!]*\s*$/i.exec(text)
    if (only && (list.awaiting || only[1])) {
        const names = numbersIn(only[2]).map(nameOf)
        if (names.length && names.every(Boolean)) {
            list.awaiting = false // later bare numbers (quantities, payment choices) are not item picks
            return `I want ${names.join(' and ')}. [The customer picked this by its list number and has not said how many yet]`
        }
        return text
    }

    // 2) a number reference inside a longer message: "number 3, 5 pieces", "2 of item 4"
    let changed = false
    const replaced = text.replace(
        /\b(?:items?|numbers?|nos?\.?|options?)\s*#?\s*(\d{1,3}(?:\s*(?:,|and|&)\s*\d{1,3})*)|#\s*(\d{1,3})\b/gi,
        (full, many, one) => {
            const nums = numbersIn(many || one)
            const names = nums.map(nameOf)
            if (names.length && names.every(Boolean)) {
                changed = true
                return names.join(' and ')
            }
            return full
        }
    )
    if (changed) list.awaiting = false
    return replaced
}

async function getGroqCompletion(userId, userMessage) {
    try {
        if (!groq) {
            return "Welcome to *SNEDA Shopping Center*! How can I assist you today?"
        }

        userMessage = expandListReferences(userId, userMessage) // "3" becomes the 3rd item shown in the list

        const liveInventory = await getLiveInventoryFromDb()
        const history = await getChatHistoryFromPg(userId, 12)
        await saveChatMessageToPg(userId, 'user', userMessage)

        let orderBlock = ''
        let calc = null
        if (getCart(userId) || looksLikeOrder(userMessage, history)) {
            calc = await calculateOrder(userId, history, userMessage, liveInventory)
            if (calc) orderBlock = formatOrderBlock(calc)
        }
        noteCartDetails(userId, userMessage)

        // Payment instructions are written by the code, so the number is always correct
        const cartNow = getCart(userId)
        if (
            PAYMENT_DETAILS && calc && cartNow?.payment === 'Mobile Money' &&
            (isPaymentOnly(userMessage) || asksForPaymentNumber(userMessage))
        ) {
            const reply = buildPaymentReply('Mobile Money', calc.total)
            await saveChatMessageToPg(userId, 'assistant', reply)
            return reply
        }

        const dynamicSystemPrompt = `
You are SNEDA Shopping Center's friendly, sharp, and accurate sales assistant located in Accra, Ghana.
Official Store Website: ${DIGITAL_CATALOG_URL}

STORE INFORMATION:
- Opening Hours: Mon–Sat (8:00 AM – 9:00 PM), Sun (10:00 AM – 6:00 PM GMT)
- Delivery: Same-day delivery across Greater Accra for orders placed before 2:00 PM.
- Payment Options: Mobile Money (MTN MoMo, Telecel Cash, AT Money), Visa/Mastercard, or Cash on Delivery.
- Payment details: ${PAYMENT_DETAILS || `NOT SET. Say the payment details are not available in this chat yet and ask the customer to contact the store${STORE_PHONE ? ' on ' + STORE_PHONE : ' through the website'}. Never promise that a representative will reach out. Never invent account, MoMo or reference numbers.`}

LIVE DATABASE INVENTORY & STOCK LEVELS:
${JSON.stringify(liveInventory, null, 2)}
${orderBlock}
STRICT INSTRUCTIONS:
1. Always check the LIVE DATABASE INVENTORY above before answering product questions.
2. If an item is in stock, state its exact price in Ghana Cedis (GH₵) and mention that it's available. If stock quantity is low, you can create friendly urgency.
3. If an item is NOT in the database list, politely inform the customer and suggest checking our website or offering a close alternative. Never hallucinate fake items or prices.
4. Keep answers conversational, warm, and optimized for WhatsApp. Use WhatsApp formatting only: *single asterisks* for bold (prices, item names, totals). NEVER use **double asterisks**, # headings, markdown tables or --- lines.
5. NEVER do arithmetic yourself. If a VERIFIED ORDER CALCULATION section is present, use its exact quantities, line totals and total. When the customer gives a payment method or asks for the total, confirm the order with the full breakdown (quantity × unit price = line total) and the TOTAL FOR ITEMS.
6. If no VERIFIED ORDER CALCULATION is present and the customer asks for a total, ask which item and how many units they want. If an item shows a stock warning, tell the customer how many are available.
7. Once the customer has chosen a payment method and you have shown the total, explain how to pay (use the payment details above only) and ask them to reply *PAID* once they have paid, so their invoice can be generated. For Cash on Delivery, ask them to reply *CONFIRM ORDER*. NEVER say the payment number is on the website or tell the customer to look it up elsewhere. The payment details above are the only ones to use.
8. When the customer asks how to pay or for the payment number, answer that directly using the payment details above. Do not use numbered lists for payment steps (use bullets), and do not mention payment reference numbers unless the payment details include one.
9. A reply that is only a number or an option, such as '1', '1 please' or 'option 2', is a menu choice, NOT a quantity. Never change or question the order quantity unless the customer clearly says so (for example 'make it 5'). If you are not sure what the number refers to, ask which option they mean.
10. Do not number your own product lists (use bullets), because customers may reply with a number from the store's numbered menu.
`

        const chatCompletion = await groq.chat.completions.create({
            messages: [
                { role: 'system', content: dynamicSystemPrompt },
                ...history,
                { role: 'user', content: userMessage }
            ],
            model: 'openai/gpt-oss-20b', // llama-3.1-8b-instant was shut down by Groq
            reasoning_effort: 'low',
            temperature: 0.3,
            max_tokens: 800
        }, { timeout: 15000, maxRetries: 1 })

        const rawReply = chatCompletion.choices[0]?.message?.content || "How else can I assist you at SNEDA Shopping Center today?"
        const aiReply = formatForWhatsApp(rawReply)

        await saveChatMessageToPg(userId, 'assistant', aiReply)
        return aiReply
    } catch (error) {
        console.error('❌ Groq API Error:', error?.status, error?.message)
        return "I'd be happy to check our current inventory for that! Let me connect you with a sales representative, or you can browse our full catalog at " + DIGITAL_CATALOG_URL
    }
}

// =============================================================
// ⏱️ 4. HELPERS
// =============================================================
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
const withTimeout = (promise, ms) => Promise.race([promise, delay(ms)])

async function simulateTyping(ctx, provider, ms = 1500) {
    try {
        const jid = ctx.key.remoteJid
        if (provider?.vendor?.sendPresenceUpdate) {
            await withTimeout(provider.vendor.sendPresenceUpdate('composing', jid), 3000)
            await delay(ms)
            await withTimeout(provider.vendor.sendPresenceUpdate('paused', jid), 3000)
        }
    } catch (error) {
        console.warn('⚠️ Typing presence skipped:', error.message || 'Connection lost')
    }
}

// Exact-match greetings only. Handled in code (not as flow keywords),
// so words like "chicken" or quantities like "10" can never reopen the menu.
const GREETINGS = ['hi', 'hello', 'hey', 'welcome', 'start', 'menu', '0']
const isGreeting = (text) => GREETINGS.includes(String(text).trim().toLowerCase())

// =============================================================
// 🧾 INVOICES — created after the customer confirms payment
// =============================================================
const NOT_YET = /\b(not|haven'?t|hasn'?t|didn'?t|never)\b/i
const looksPaid = (m) =>
    /\b(paid|payment\s+(?:is\s+|has\s+been\s+)?(?:made|done|sent|complete[d]?)|(?:i|we)(?:\s+have|'ve|’ve)?\s+(?:already\s+)?(?:sent|transferred|made)\s+(?:the\s+)?(?:money|payment)|money\s+(?:sent|transferred)|sent\s+the\s+money)\b/i.test(m)
const looksConfirmed = (m) => /\b(confirm(?:ed)?(?:\s+my)?(?:\s+the)?\s+order|place\s+(?:my\s+|the\s+)?order|go\s+ahead|proceed)\b/i.test(m)

const shouldInvoice = (userId, msg) => {
    const cart = getCart(userId)
    if (!cart || NOT_YET.test(msg)) return false
    if (looksPaid(msg)) return true
    return cart.payment === 'Cash on Delivery' && looksConfirmed(msg)
}

const pdfMoney = (n) => money(n).replace('GH₵', 'GHS') // standard PDF fonts have no ₵ symbol
const accraTime = (d) => d.toLocaleString('en-GB', { timeZone: 'Africa/Accra', dateStyle: 'medium', timeStyle: 'short' })

// Reads delivery location / name from what the customer already wrote
async function getDeliveryDetails(userId) {
    if (!groq) return {}
    try {
        const history = await getChatHistoryFromPg(userId, 14)
        const customerText = history.filter((m) => m.role === 'user').map((m) => m.content).join('\n')
        if (!customerText) return {}

        const res = await groq.chat.completions.create({
            model: 'openai/gpt-oss-20b',
            reasoning_effort: 'low',
            temperature: 0,
            max_tokens: 300,
            response_format: { type: 'json_object' },
            messages: [
                {
                    role: 'system',
                    content:
                        'Extract delivery details from the customer messages. Reply with JSON only: ' +
                        '{"location":"<delivery area or address, or null>","name":"<customer name, or null>"}. ' +
                        'Use null when it is not clearly given. Never invent details.'
                },
                { role: 'user', content: customerText }
            ]
        }, { timeout: 8000, maxRetries: 0 })

        const parsed = JSON.parse((res.choices[0]?.message?.content || '').replace(/```json|```/g, '').trim() || '{}')
        const clean = (v) => (typeof v === 'string' && v.trim() && v.trim().toLowerCase() !== 'null' ? v.trim() : null)
        return { location: clean(parsed.location), name: clean(parsed.name) }
    } catch (err) {
        console.warn('⚠️ Delivery details not read:', err?.message)
        return {}
    }
}

function buildInvoiceText(inv) {
    const items = inv.lines
        .map((l) => `${l.qty} × ${l.item}\n      @ ${money(l.unit)} = ${money(l.subtotal)}`)
        .join('\n')
    return [
        `🧾 *INVOICE ${inv.invoiceNo}*`,
        `📅 ${inv.dateText}`,
        '━━━━━━━━━━━━━━',
        `*${STORE_NAME}*`,
        '',
        `👤 *Customer:* ${inv.name ? inv.name + ' — ' : ''}+${inv.phone}`,
        `📍 *Delivery:* ${inv.location}`,
        '━━━━━━━━━━━━━━',
        items,
        '━━━━━━━━━━━━━━',
        `💰 *TOTAL: ${money(inv.total)}*`,
        `💳 *Payment:* ${inv.payMethod}`,
        `${inv.isCash ? '🚚' : '✅'} *Status:* ${inv.paymentStatus}`,
        '',
        `Thank you for shopping with ${STORE_NAME}! 🙏`
    ].join('\n')
}

// PDF invoice (needs: npm install pdfkit). Returns the file path, or null if pdfkit is missing.
async function buildInvoicePdf(inv) {
    let PDFDocument
    try {
        PDFDocument = (await import('pdfkit')).default
    } catch {
        console.warn('⚠️ pdfkit is not installed, sending text invoice only. Run: npm install pdfkit')
        return null
    }

    fs.mkdirSync('invoices', { recursive: true })
    const file = path.resolve('invoices', `${inv.invoiceNo}.pdf`)

    return await new Promise((resolve, reject) => {
        const doc = new PDFDocument({ size: 'A4', margin: 50 })
        const stream = fs.createWriteStream(file)
        stream.on('finish', () => resolve(file))
        stream.on('error', reject)
        doc.pipe(stream)

        // Header
        doc.font('Helvetica-Bold').fontSize(18).fillColor('#111111').text(STORE_NAME, 50, 50)
        doc.font('Helvetica').fontSize(10).fillColor('#555555').text(STORE_ADDRESS, 50, 74)
        if (STORE_PHONE) doc.text(STORE_PHONE, 50, 88)
        doc.font('Helvetica-Bold').fontSize(26).fillColor('#111111').text('INVOICE', 350, 50, { width: 195, align: 'right' })
        doc.font('Helvetica').fontSize(10).fillColor('#555555')
            .text(`No: ${inv.invoiceNo}`, 350, 84, { width: 195, align: 'right' })
            .text(`Date: ${inv.dateText}`, 350, 98, { width: 195, align: 'right' })

        // Bill to
        doc.font('Helvetica-Bold').fontSize(11).fillColor('#111111').text('BILL TO', 50, 140)
        doc.font('Helvetica').fontSize(10).fillColor('#333333')
            .text(inv.name || 'Customer', 50, 158)
            .text(`WhatsApp: +${inv.phone}`, 50, 172)
            .text(`Delivery: ${inv.location}`, 50, 186, { width: 495 })

        // Table header
        let y = 232
        doc.rect(50, y - 6, 495, 24).fill('#eeeeee')
        doc.font('Helvetica-Bold').fontSize(10).fillColor('#111111')
        doc.text('Item', 56, y, { width: 235 })
        doc.text('Qty', 295, y, { width: 50, align: 'right' })
        doc.text('Unit price', 350, y, { width: 90, align: 'right' })
        doc.text('Amount', 450, y, { width: 90, align: 'right' })
        y += 30

        // Rows
        doc.font('Helvetica').fontSize(10).fillColor('#222222')
        for (const l of inv.lines) {
            const h = doc.heightOfString(l.item, { width: 235 })
            if (y + h > 740) {
                doc.addPage()
                y = 50
            }
            doc.text(l.item, 56, y, { width: 235 })
            doc.text(String(l.qty), 295, y, { width: 50, align: 'right' })
            doc.text(pdfMoney(l.unit), 350, y, { width: 90, align: 'right' })
            doc.text(pdfMoney(l.subtotal), 450, y, { width: 90, align: 'right' })
            y += Math.max(22, h + 8)
        }

        doc.moveTo(50, y).lineTo(545, y).strokeColor('#cccccc').stroke()
        y += 14
        doc.font('Helvetica-Bold').fontSize(14).fillColor('#111111')
            .text(`TOTAL: ${pdfMoney(inv.total)}`, 300, y, { width: 245, align: 'right' })

        // Payment
        y += 50
        doc.font('Helvetica-Bold').fontSize(11).text('PAYMENT', 50, y)
        doc.font('Helvetica').fontSize(10).fillColor('#333333')
            .text(`Method: ${inv.payMethod}`, 50, y + 18)
            .text(`Status: ${inv.paymentStatus}`, 50, y + 32, { width: 495 })

        doc.font('Helvetica').fontSize(10).fillColor('#777777')
            .text(`Thank you for shopping with ${STORE_NAME}!`, 50, 760, { width: 495, align: 'center' })

        doc.end()
    })
}

// Saves the invoice, sends it to the customer (text + PDF) and copies the owner.
async function issueInvoice(ctx, provider, flowDynamic) {
    const cart = getCart(ctx.from)
    if (!cart) return false

    const inventory = await getLiveInventoryFromDb()
    const priced = priceItems(cart.items, inventory)
    if (!priced) return false

    const payMethod = cart.payment || 'Not specified'
    const isCash = payMethod === 'Cash on Delivery'
    const paymentStatus = isCash
        ? 'Pay cash on delivery'
        : 'Payment confirmed by customer, being verified by our team'

    const now = new Date()
    const stamp = now.toLocaleDateString('en-CA', { timeZone: 'Africa/Accra' }).replace(/-/g, '')
    let invoiceNo = `INV-${stamp}-${Math.floor(1000 + Math.random() * 9000)}`

    try {
        const res = await pool.query(
            `INSERT INTO invoices (user_id, customer_name, customer_phone, delivery_location, items, total, payment_method, payment_status)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id`,
            [ctx.from, cart.customerName || null, ctx.from, cart.location, JSON.stringify(priced.lines), priced.total, payMethod, paymentStatus]
        )
        invoiceNo = `INV-${stamp}-${String(res.rows[0].id).padStart(4, '0')}`
        await pool.query('UPDATE invoices SET invoice_no = $1 WHERE id = $2', [invoiceNo, res.rows[0].id])
    } catch (err) {
        console.error('❌ Error saving invoice to PG:', err.message)
    }

    const inv = {
        invoiceNo,
        dateText: accraTime(now),
        phone: String(ctx.from).replace(/\D/g, ''),
        name: cart.customerName || null,
        location: cart.location,
        lines: priced.lines,
        total: priced.total,
        payMethod,
        isCash,
        paymentStatus
    }

    await flowDynamic(buildInvoiceText(inv))

    try {
        const file = await buildInvoicePdf(inv)
        if (file) await flowDynamic([{ body: '📄 Your invoice (PDF)', media: file }])
    } catch (err) {
        console.error('❌ Invoice PDF failed:', err.message)
    }

    if (OWNER_NUMBER) {
        try {
            const short = priced.lines.some((l) => !l.enough) ? '\n⚠️ Some items exceed stock. Please check.' : ''
            for (const ownerNo of OWNER_NUMBERS) await provider.sendMessage(
                ownerNo,
                `🔔 *New order ${invoiceNo}*\n${inv.lines.map((l) => `${l.qty} × ${l.item}`).join('\n')}\n*Total:* ${money(inv.total)}\n*Payment:* ${payMethod} (${paymentStatus})\n*Customer:* +${inv.phone}\n*Delivery:* ${inv.location}${short}`,
                {}
            )
        } catch (err) {
            console.warn('⚠️ Could not notify owner:', err?.message)
        }
    }

    // TODO (when we migrate to Meta Cloud API): reduce stock here after an invoice is issued, e.g.
    // UPDATE inventory SET stock_quantity = GREATEST(stock_quantity - $1, 0) WHERE lower(item_name) = lower($2)
    // Also: send the invoice PDF from a public URL (Meta cannot send local files), and add buttons.
    userCarts.delete(ctx.from)
    console.log(`🧾 Invoice ${invoiceNo} issued to ${ctx.from}`)
    return true
}

// =============================================================
// 📊 DAILY SALES REPORT (Excel + PDF) — sent to the owner after closing
// =============================================================
const accraNow = (d = new Date()) => ({
    date: d.toLocaleDateString('en-CA', { timeZone: 'Africa/Accra' }),
    hhmm: d.toLocaleTimeString('en-GB', { timeZone: 'Africa/Accra', hour: '2-digit', minute: '2-digit', hourCycle: 'h23' })
})
const dayBefore = (date) => new Date(new Date(date + 'T00:00:00Z').getTime() - 86400000).toISOString().slice(0, 10)
const dateLabel = (date) =>
    new Date(date + 'T00:00:00Z').toLocaleDateString('en-GB', { timeZone: 'UTC', day: 'numeric', month: 'short', year: 'numeric' })

// "report" -> today so far, "report 2026-10-20" -> that day (owner only)
const reportCommand = (text) => {
    const m = String(text).trim().match(/^report(?:\s+(\d{4}-\d{2}-\d{2}))?$/i)
    return m ? m[1] || 'today' : null
}

async function getSalesForDate(date) {
    const res = await pool.query(
        `SELECT invoice_no, customer_name, customer_phone, delivery_location, items, total, payment_method, payment_status,
                to_char(created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Africa/Accra', 'HH24:MI') AS time_text
         FROM invoices
         WHERE (created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Africa/Accra')::date = $1::date
         ORDER BY id`,
        [date]
    )
    return res.rows.map((r) => ({
        invoiceNo: r.invoice_no,
        name: r.customer_name,
        phone: r.customer_phone,
        location: r.delivery_location,
        lines: Array.isArray(r.items) ? r.items : JSON.parse(r.items || '[]'),
        total: Number(r.total),
        payMethod: r.payment_method || 'Not specified',
        status: r.payment_status,
        time: r.time_text
    }))
}

function summarizeSales(date, rows) {
    const methods = new Map()
    const items = new Map()
    let total = 0
    for (const r of rows) {
        total = round2(total + r.total)
        const m = methods.get(r.payMethod) || { method: r.payMethod, count: 0, total: 0 }
        m.count += 1
        m.total = round2(m.total + r.total)
        methods.set(r.payMethod, m)
        for (const l of r.lines) {
            const i = items.get(l.item) || { item: l.item, qty: 0, revenue: 0 }
            i.qty += Number(l.qty) || 0
            i.revenue = round2(i.revenue + (Number(l.subtotal) || 0))
            items.set(l.item, i)
        }
    }
    return {
        date,
        dateText: dateLabel(date),
        count: rows.length,
        total,
        byMethod: [...methods.values()].sort((a, b) => b.total - a.total),
        items: [...items.values()].sort((a, b) => b.revenue - a.revenue),
        invoices: rows
    }
}

function buildReportText(r) {
    if (!r.count) return `📊 *Daily sales report — ${r.dateText}*\n\nNo invoices were issued on this day.`
    const methods = r.byMethod.map((m) => `• ${m.method}: ${m.count} order(s) — ${money(m.total)}`).join('\n')
    const top = r.items.slice(0, 5).map((i) => `• ${i.qty} × ${i.item} — ${money(i.revenue)}`).join('\n')
    const needsChecking = r.byMethod.some((m) => m.method !== 'Cash on Delivery')
    const hasCash = r.byMethod.some((m) => m.method === 'Cash on Delivery')
    return [
        `📊 *Daily sales report — ${r.dateText}*`,
        '',
        `🧾 *Invoices:* ${r.count}`,
        `💰 *Total sales: ${money(r.total)}*`,
        '',
        '*By payment method*',
        methods,
        '',
        '*Top items*',
        top,
        needsChecking ? '\n⚠️ Mobile Money / card payments are only confirmed by the customers. Please check them against your MoMo statement.' : null,
        hasCash ? '🚚 Cash on Delivery amounts are still to be collected on delivery.' : null
    ].filter((line) => line !== null).join('\n')
}

// Excel report (needs: npm install exceljs). Returns the file path, or null if exceljs is missing.
async function buildReportXlsx(r) {
    let ExcelJS
    try {
        ExcelJS = (await import('exceljs')).default
    } catch {
        console.warn('⚠️ exceljs is not installed, skipping the Excel report. Run: npm install exceljs')
        return null
    }

    const cedi = '"GH₵" #,##0.00'
    const grey = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFEEEEEE' } }
    const styleHeader = (row) => {
        row.font = { bold: true }
        row.eachCell((cell) => { cell.fill = grey })
    }

    const wb = new ExcelJS.Workbook()
    wb.creator = STORE_NAME

    // Sheet 1: summary
    const sum = wb.addWorksheet('Summary')
    sum.columns = [{ width: 34 }, { width: 14 }, { width: 20 }]
    sum.addRow([STORE_NAME]).font = { bold: true, size: 14 }
    sum.addRow(['Daily sales report', r.dateText])
    sum.addRow([])
    sum.addRow(['Invoices issued', r.count])
    const totalRow = sum.addRow(['Total sales', null, r.total])
    totalRow.font = { bold: true }
    totalRow.getCell(3).numFmt = cedi
    sum.addRow([])
    styleHeader(sum.addRow(['Payment method', 'Invoices', 'Amount']))
    for (const m of r.byMethod) {
        const row = sum.addRow([m.method, m.count, m.total])
        row.getCell(3).numFmt = cedi
    }
    sum.addRow([])
    sum.addRow(['Mobile Money / card payments are confirmed by customers only. Please verify them against your MoMo statement.'])

    // Sheet 2: items sold
    const itemsSheet = wb.addWorksheet('Items sold')
    itemsSheet.columns = [
        { header: 'Item', key: 'item', width: 38 },
        { header: 'Quantity sold', key: 'qty', width: 16 },
        { header: 'Revenue', key: 'revenue', width: 18 }
    ]
    styleHeader(itemsSheet.getRow(1))
    for (const i of r.items) {
        const row = itemsSheet.addRow({ item: i.item, qty: i.qty, revenue: i.revenue })
        row.getCell('revenue').numFmt = cedi
    }

    // Sheet 3: every invoice
    const inv = wb.addWorksheet('Invoices')
    inv.columns = [
        { header: 'Invoice no', key: 'no', width: 20 },
        { header: 'Time', key: 'time', width: 8 },
        { header: 'Customer phone', key: 'phone', width: 18 },
        { header: 'Name', key: 'name', width: 18 },
        { header: 'Delivery location', key: 'location', width: 28 },
        { header: 'Items', key: 'items', width: 48 },
        { header: 'Total', key: 'total', width: 16 },
        { header: 'Payment method', key: 'method', width: 18 },
        { header: 'Status', key: 'status', width: 46 }
    ]
    styleHeader(inv.getRow(1))
    for (const x of r.invoices) {
        const row = inv.addRow({
            no: x.invoiceNo,
            time: x.time,
            phone: x.phone ? '+' + String(x.phone).replace(/\D/g, '') : '',
            name: x.name || '',
            location: x.location || '',
            items: x.lines.map((l) => `${l.qty} × ${l.item}`).join('; '),
            total: x.total,
            method: x.payMethod,
            status: x.status || ''
        })
        row.getCell('total').numFmt = cedi
    }
    const totalLine = inv.addRow({ no: 'TOTAL', total: r.total })
    totalLine.font = { bold: true }
    totalLine.getCell('total').numFmt = cedi

    fs.mkdirSync('reports', { recursive: true })
    const file = path.resolve('reports', `Sales-${r.date}.xlsx`)
    await wb.xlsx.writeFile(file)
    return file
}

// PDF report (uses pdfkit, same as the invoices)
async function buildReportPdf(r) {
    let PDFDocument
    try {
        PDFDocument = (await import('pdfkit')).default
    } catch {
        console.warn('⚠️ pdfkit is not installed, skipping the PDF report. Run: npm install pdfkit')
        return null
    }

    fs.mkdirSync('reports', { recursive: true })
    const file = path.resolve('reports', `Sales-${r.date}.pdf`)

    return await new Promise((resolve, reject) => {
        const doc = new PDFDocument({ size: 'A4', margin: 50 })
        const stream = fs.createWriteStream(file)
        stream.on('finish', () => resolve(file))
        stream.on('error', reject)
        doc.pipe(stream)

        // Draws a table and returns the y position under it (adds pages when needed)
        const drawTable = (startY, cols, rows) => {
            let y = startY
            const header = () => {
                doc.rect(50, y - 5, 495, 20).fill('#eeeeee')
                doc.font('Helvetica-Bold').fontSize(9).fillColor('#111111')
                for (const c of cols) doc.text(c.label, c.x, y, { width: c.w, align: c.align || 'left' })
                y += 22
                doc.font('Helvetica').fontSize(9).fillColor('#222222')
            }
            header()
            for (const row of rows) {
                const h = Math.max(...cols.map((c) => doc.heightOfString(String(row[c.key] ?? ''), { width: c.w })))
                if (y + h > 770) {
                    doc.addPage()
                    y = 50
                    header()
                }
                for (const c of cols) doc.text(String(row[c.key] ?? ''), c.x, y, { width: c.w, align: c.align || 'left' })
                y += h + 6
            }
            return y
        }
        const section = (title, y) => {
            doc.font('Helvetica-Bold').fontSize(11).fillColor('#111111').text(title, 50, y)
            return y + 20
        }

        doc.font('Helvetica-Bold').fontSize(18).fillColor('#111111').text(STORE_NAME, 50, 50)
        doc.font('Helvetica').fontSize(12).fillColor('#555555').text(`Daily sales report — ${r.dateText}`, 50, 76)
        doc.font('Helvetica-Bold').fontSize(12).fillColor('#111111')
            .text(`Invoices: ${r.count}`, 50, 106)
            .text(`Total sales: ${pdfMoney(r.total)}`, 50, 124)

        let y = section('Payment methods', 160)
        y = drawTable(y, [
            { label: 'Method', key: 'method', x: 56, w: 230 },
            { label: 'Invoices', key: 'count', x: 290, w: 60, align: 'right' },
            { label: 'Amount', key: 'amount', x: 360, w: 180, align: 'right' }
        ], r.byMethod.map((m) => ({ method: m.method, count: m.count, amount: pdfMoney(m.total) })))

        y = section('Items sold', y + 18)
        y = drawTable(y, [
            { label: 'Item', key: 'item', x: 56, w: 280 },
            { label: 'Qty', key: 'qty', x: 340, w: 50, align: 'right' },
            { label: 'Revenue', key: 'revenue', x: 395, w: 145, align: 'right' }
        ], r.items.map((i) => ({ item: i.item, qty: i.qty, revenue: pdfMoney(i.revenue) })))

        y = section('Invoices', y + 18)
        y = drawTable(y, [
            { label: 'Invoice', key: 'no', x: 56, w: 95 },
            { label: 'Time', key: 'time', x: 155, w: 40 },
            { label: 'Customer', key: 'phone', x: 200, w: 95 },
            { label: 'Payment', key: 'method', x: 300, w: 95 },
            { label: 'Total', key: 'total', x: 400, w: 140, align: 'right' }
        ], r.invoices.map((x) => ({
            no: x.invoiceNo, time: x.time, phone: x.phone ? '+' + String(x.phone).replace(/\D/g, '') : '', method: x.payMethod, total: pdfMoney(x.total)
        })))

        doc.font('Helvetica').fontSize(8).fillColor('#777777').text(
            'Mobile Money / card payments are confirmed by customers only. Please verify them against your MoMo statement.',
            50, y + 16, { width: 495 })

        doc.end()
    })
}

// Builds the report for one day and sends it (message + files) to the given numbers
async function sendDailyReport(provider, dateInput, recipients, preloadedRows) {
    const date = dateInput === 'today' ? accraNow().date : dateInput
    let report
    let files = []
    try {
        report = summarizeSales(date, preloadedRows || (await getSalesForDate(date)))
        if (report.count) {
            if (REPORT_FORMATS.includes('xlsx')) {
                try {
                    const f = await buildReportXlsx(report)
                    if (f) files.push({ file: f, label: `📊 Excel sales report — ${report.dateText}` })
                } catch (err) {
                    console.error('❌ Excel report failed:', err.message)
                }
            }
            if (REPORT_FORMATS.includes('pdf')) {
                try {
                    const f = await buildReportPdf(report)
                    if (f) files.push({ file: f, label: `📄 PDF sales report — ${report.dateText}` })
                } catch (err) {
                    console.error('❌ PDF report failed:', err.message)
                }
            }
        }
    } catch (err) {
        console.error('❌ Could not build the daily report:', err.message)
        for (const to of recipients) await provider.sendMessage(to, '⚠️ Sorry, I could not build the sales report. Please check the server logs.', {})
        return false
    }

    const text = buildReportText(report)
    for (const to of recipients) {
        await provider.sendMessage(to, text, {})
        for (const f of files) {
            try {
                await provider.sendMessage(to, f.label, { media: f.file })
            } catch (err) {
                console.error('❌ Could not send report file:', err.message)
            }
        }
    }
    console.log(`📊 Daily report for ${date} sent (${report.count} invoices)`)
    return true
}

// Runs every minute. Sends today's report at REPORT_TIME, and catches up on yesterday's if the bot was asleep.
let reportCheckRunning = false
async function runReportCheck(provider, now = accraNow()) {
    if (reportCheckRunning || OWNER_NUMBERS.length === 0) return
    if (!provider?.vendor?.user) return // WhatsApp not connected yet, try again next minute
    reportCheckRunning = true
    try {
        for (const [date, waitForTime] of [[dayBefore(now.date), false], [now.date, true]]) {
            if (waitForTime && now.hhmm < REPORT_TIME) continue

            const done = await pool.query('SELECT 1 FROM daily_reports WHERE report_date = $1', [date])
            if (done.rows.length) continue

            const rows = await getSalesForDate(date)
            if (!waitForTime && rows.length === 0) {
                await pool.query('INSERT INTO daily_reports (report_date, invoices) VALUES ($1, 0) ON CONFLICT DO NOTHING', [date])
                continue
            }

            const sent = await sendDailyReport(provider, date, OWNER_NUMBERS, rows)
            if (sent) {
                await pool.query('INSERT INTO daily_reports (report_date, invoices) VALUES ($1, $2) ON CONFLICT DO NOTHING', [date, rows.length])
            }
        }
    } catch (err) {
        console.error('❌ Daily report check failed:', err.message)
    } finally {
        reportCheckRunning = false
    }
}

// =============================================================
// 🙋 HUMAN TAKEOVER — the bot stays silent while a person is chatting with the customer
// =============================================================
const pausedChats = new Map() // chatId -> time (ms) when the bot may talk again
const botSentIds = new Set() // ids of messages the bot sent (so they are never mistaken for a person)
const recentBotSends = [] // { chatId, text, at }
const autoPauseTimes = []
let humanTakeoverBroken = false
const HUMAN_DETECT_DELAY_MS = 2500
const WATCH_INTERVAL_MS = 3000

const chatIdOf = (x) => String(x || '').split('@')[0].split(':')[0].replace(/\D/g, '')
const ghNumber = (x) => {
    let d = String(x).replace(/\D/g, '')
    if (d.length === 10 && d.startsWith('0')) d = '233' + d.slice(1)
    return d
}

function setPause(chatId, minutes) {
    const until = Date.now() + minutes * 60000
    pausedChats.set(chatId, until)
    pool.query(
        'INSERT INTO bot_pauses (chat_id, paused_until) VALUES ($1, $2) ON CONFLICT (chat_id) DO UPDATE SET paused_until = EXCLUDED.paused_until',
        [chatId, until]
    ).catch((err) => console.error('❌ Could not save pause:', err.message))
}

function clearPause(chatId) {
    pausedChats.delete(chatId)
    pool.query('DELETE FROM bot_pauses WHERE chat_id = $1', [chatId])
        .catch((err) => console.error('❌ Could not clear pause:', err.message))
}

async function loadPauses() {
    try {
        const res = await pool.query('SELECT chat_id, paused_until FROM bot_pauses')
        const now = Date.now()
        for (const r of res.rows) if (Number(r.paused_until) > now) pausedChats.set(r.chat_id, Number(r.paused_until))
        if (pausedChats.size) console.log(`⏸️ ${pausedChats.size} chat(s) still paused from before the restart`)
    } catch (err) {
        console.error('❌ Could not load paused chats:', err.message)
    }
}

// Used at the start of every flow step: true = a person is handling this customer, so the bot says nothing
function isPaused(ctx) {
    const id = chatIdOf(ctx.key?.remoteJid || ctx.from)
    const until = pausedChats.get(id)
    if (!until) return false
    if (Date.now() >= until) {
        clearPause(id)
        return false
    }
    return true
}

// While paused, keep the customer's messages so the AI knows what was said when it resumes
function notePausedMessage(ctx) {
    return saveChatMessageToPg(ctx.from, 'user', ctx.body)
}

function rememberBotSend(jid, text) {
    const at = Date.now()
    recentBotSends.push({ chatId: chatIdOf(jid), text: String(text || '').trim(), at })
    while (recentBotSends.length && at - recentBotSends[0].at > 120000) recentBotSends.shift()
}

// Did the bot itself send this message? (three checks, so the bot can never pause itself by mistake)
function sentByBot(jid, id, text, eventAt) {
    if (id && botSentIds.has(id)) return true
    const chatId = chatIdOf(jid)
    const t = String(text || '').trim()
    return recentBotSends.some(
        (s) => s.chatId === chatId && (Math.abs(eventAt - s.at) < 8000 || (t && s.text === t))
    )
}

function onHumanMessage(jid, text) {
    if (humanTakeoverBroken) return
    const chatId = chatIdOf(jid)
    if (!chatId) return

    // The person types #bot in the chat when they are finished
    if (/^\s*#bot\s*$/i.test(text)) {
        clearPause(chatId)
        console.log(`🤖 Bot resumed in chat ${chatId} (#bot)`)
        return
    }

    if (!pausedChats.has(chatId)) {
        // Safety: many "takeovers" in a short time means the bot is probably seeing its own messages
        const now = Date.now()
        autoPauseTimes.push(now)
        while (autoPauseTimes.length && now - autoPauseTimes[0] > 120000) autoPauseTimes.shift()
        if (autoPauseTimes.length >= 5) {
            humanTakeoverBroken = true
            for (const id of [...pausedChats.keys()]) clearPause(id)
            console.error('❌ Human takeover switched OFF: too many pauses in 2 minutes (the bot may be seeing its own messages).')
            return
        }
        console.log(`🙋 A person is chatting with ${chatId}: bot paused`)
    }
    setPause(chatId, HUMAN_PAUSE_MINUTES)
    if (text) saveChatMessageToPg(chatId, 'assistant', `[Staff] ${text}`)
}

const timestampMs = (ts) => {
    if (ts == null) return null
    const n = typeof ts === 'object' ? (typeof ts.toNumber === 'function' ? ts.toNumber() : Number(ts.low)) : Number(ts)
    return Number.isFinite(n) ? n * 1000 : null
}

// Watches the WhatsApp connection for messages typed by a person on the phone
function startHumanTakeoverWatcher(provider) {
    let attachedTo = null
    setInterval(() => {
        const sock = provider.vendor
        if (!sock?.ev?.on || typeof sock.sendMessage !== 'function' || sock === attachedTo) return
        attachedTo = sock // a new connection object appears after every reconnect, so attach again

        // 1) remember everything the bot sends
        const originalSend = sock.sendMessage.bind(sock)
        sock.sendMessage = async (jid, content, ...rest) => {
            rememberBotSend(jid, content?.text || content?.caption || '')
            const sent = await originalSend(jid, content, ...rest)
            if (sent?.key?.id) {
                botSentIds.add(sent.key.id)
                if (botSentIds.size > 2000) botSentIds.clear()
            }
            return sent
        }

        // 2) a message from "me" that the bot did not send = a person typed it on the phone
        sock.ev.on('messages.upsert', ({ messages, type }) => {
            for (const m of messages || []) {
                if (!m?.key?.fromMe || !m.message) continue
                const jid = m.key.remoteJid || ''
                if (!/@(s\.whatsapp\.net|lid)$/.test(jid)) continue // skip groups, status and broadcasts

                const inner = m.message.ephemeralMessage?.message || m.message
                const text =
                    inner.conversation || inner.extendedTextMessage?.text || inner.imageMessage?.caption ||
                    inner.videoMessage?.caption || inner.documentMessage?.caption || ''
                const isContent =
                    text || inner.imageMessage || inner.videoMessage || inner.documentMessage ||
                    inner.audioMessage || inner.stickerMessage || inner.contactMessage || inner.locationMessage
                if (!isContent) continue // reactions, deletes and other background messages

                const sentAt = timestampMs(m.messageTimestamp)
                if (sentAt !== null ? Date.now() - sentAt > 90000 : type !== 'notify') continue // old history

                const eventAt = Date.now()
                const id = m.key.id
                setTimeout(() => {
                    if (!sentByBot(jid, id, text, eventAt)) onHumanMessage(jid, text)
                }, HUMAN_DETECT_DELAY_MS)
            }
        })
        console.log('🙋 Human takeover is on: the bot pauses when a person replies from the phone.')
    }, WATCH_INTERVAL_MS)
}

// Owner commands (sent from an owner number): pause 0241234567 | resume 0241234567 | resume all | paused
async function handleOwnerCommand(text) {
    const t = String(text).trim()
    if (/^paused$/i.test(t)) {
        const now = Date.now()
        const list = [...pausedChats].filter(([, until]) => until > now)
            .map(([id, until]) => `• +${id} — ${Math.ceil((until - now) / 60000)} min left`)
        return list.length ? `⏸️ *Chats where the bot is paused:*\n${list.join('\n')}` : '▶️ The bot is not paused in any chat.'
    }
    const m = t.match(/^(pause|resume)\s+(all|\+?[\d\s-]{8,})$/i)
    if (!m) return null
    const action = m[1].toLowerCase()
    if (m[2].toLowerCase() === 'all') {
        if (action === 'pause') return 'To pause one customer, send: pause 0241234567'
        const n = pausedChats.size
        for (const id of [...pausedChats.keys()]) clearPause(id)
        return `▶️ Bot resumed in ${n} chat(s).`
    }
    const id = ghNumber(m[2])
    if (action === 'pause') {
        setPause(id, HUMAN_PAUSE_MINUTES)
        return `⏸️ Bot paused for +${id} (${HUMAN_PAUSE_MINUTES} min).`
    }
    clearPause(id)
    return `▶️ Bot resumed for +${id}.`
}

// =============================================================
// 🛍️ 5. FLOW DEFINITIONS
// =============================================================
const orderCaptureFlow = addKeyword(EVENTS.ACTION)
    .addAction({ capture: true }, async (ctx, { provider, flowDynamic, gotoFlow }) => {
        if (isPaused(ctx)) return notePausedMessage(ctx)
        if (isGreeting(ctx.body)) { userCarts.delete(ctx.from); userLists.delete(ctx.from); return gotoFlow(welcomeFlow) }
        if (shouldInvoice(ctx.from, ctx.body)) return gotoFlow(invoiceFlow)
        if (isOwner(ctx.from) && reportCommand(ctx.body)) {
            await sendDailyReport(provider, reportCommand(ctx.body), [ctx.from])
            return gotoFlow(orderCaptureFlow)
        }
        if (isOwner(ctx.from)) {
            const reply = await handleOwnerCommand(ctx.body)
            if (reply) {
                await flowDynamic(reply)
                return gotoFlow(orderCaptureFlow)
            }
        }

        await simulateTyping(ctx, provider, 1800)
        await logCustomerOrderToPg(ctx.from, ctx.body)
        const aiResponse = await getGroqCompletion(ctx.from, ctx.body)
        await flowDynamic(aiResponse)
        return gotoFlow(orderCaptureFlow)
    })

const orderPromptFlow = addKeyword(EVENTS.ACTION)
    .addAnswer(
        '🛒 *Great choice!* Please reply with the *item number* or *item name*, the *quantity*, your *delivery location* in Accra, and your *payment method* (e.g., Mobile Money or Cash on Delivery):',
        { capture: true },
        async (ctx, { provider, flowDynamic, gotoFlow }) => {
            if (isPaused(ctx)) return notePausedMessage(ctx)
            if (isGreeting(ctx.body)) { userCarts.delete(ctx.from); userLists.delete(ctx.from); return gotoFlow(welcomeFlow) }
            if (shouldInvoice(ctx.from, ctx.body)) return gotoFlow(invoiceFlow)

            await simulateTyping(ctx, provider, 2000)
            await logCustomerOrderToPg(ctx.from, ctx.body)
            const aiResponse = await getGroqCompletion(ctx.from, `Customer ordered/inquired: ${ctx.body}`)
            await flowDynamic(aiResponse)
            return gotoFlow(orderCaptureFlow)
        }
    )

const categorySelectionFlow = addKeyword(EVENTS.ACTION)
    .addAnswer(
        'Which category would you like to explore?\n\n1️⃣ *Groceries*\n2️⃣ *Electronics*\n3️⃣ *View Official Web Store*',
        { capture: true },
        async (ctx, { gotoFlow, provider, flowDynamic }) => {
            if (isPaused(ctx)) return notePausedMessage(ctx)
            const input = ctx.body.trim().toLowerCase()
            if (isGreeting(input)) { userCarts.delete(ctx.from); userLists.delete(ctx.from); return gotoFlow(welcomeFlow) }

            await simulateTyping(ctx, provider, 1500)

            const liveInventory = await getLiveInventoryFromDb()

            if (input === '1' || input.includes('grocery') || input.includes('groceries')) {
                const groceries = liveInventory.filter(item => item.category.toLowerCase() === 'groceries')
                const itemsList = groceries
                    .map((item, index) => `${index + 1}. *${item.item_name}* — ${item.price} (Stock: ${item.stock_quantity})`)
                    .join('\n')

                setActiveList(ctx.from, groceries)
                await flowDynamic(`🛒 *Available Groceries:*\n\n${itemsList || '_No items listed yet._'}\n\n_Reply with the *item number* (for example *2*) or type the item name and quantity._`)
                return gotoFlow(orderPromptFlow)

            } else if (input === '2' || input.includes('electronic')) {
                const electronics = liveInventory.filter(item => item.category.toLowerCase() === 'electronics')
                const itemsList = electronics
                    .map((item, index) => `${index + 1}. *${item.item_name}* — ${item.price} (Stock: ${item.stock_quantity})`)
                    .join('\n')

                setActiveList(ctx.from, electronics)
                await flowDynamic(`⚡ *Available Electronics:*\n\n${itemsList || '_No items listed yet._'}\n\n_Reply with the *item number* (for example *2*) or type the item name and quantity._`)
                return gotoFlow(orderPromptFlow)

            } else if (input === '3' || input.includes('link') || input.includes('web')) {
                await flowDynamic(`🔗 Browse our complete online store here:\n${DIGITAL_CATALOG_URL}`)
                return gotoFlow(orderCaptureFlow)
            } else {
                const aiReply = await getGroqCompletion(ctx.from, ctx.body)
                await flowDynamic(aiReply)
                return gotoFlow(orderCaptureFlow)
            }
        }
    )

const retailFlow = addKeyword(EVENTS.ACTION)
    .addAnswer(
        [
            '🛍️ *SNEDA Retail Services*',
            'Welcome to SNEDA Shopping Center!',
            '',
            'How would you like to explore our store today?',
            `1️⃣ *Visit Store Website* (${DIGITAL_CATALOG_URL})`,
            '2️⃣ *Browse In-Chat Categories*'
        ].join('\n'),
        { capture: true },
        async (ctx, { gotoFlow, provider, flowDynamic }) => {
            if (isPaused(ctx)) return notePausedMessage(ctx)
            const selection = ctx.body.trim().toLowerCase()
            if (isGreeting(selection)) { userCarts.delete(ctx.from); userLists.delete(ctx.from); return gotoFlow(welcomeFlow) }

            if (selection === '1' || selection.includes('web') || selection.includes('site')) {
                await simulateTyping(ctx, provider, 1500)
                await flowDynamic(`🌐 Click here to open our official store website:\n${DIGITAL_CATALOG_URL}`)
                return gotoFlow(orderCaptureFlow)
            } else if (selection === '2' || selection.includes('browse') || selection.includes('chat')) {
                return gotoFlow(categorySelectionFlow)
            } else {
                const aiReply = await getGroqCompletion(ctx.from, ctx.body)
                await flowDynamic(aiReply)
                return gotoFlow(orderCaptureFlow)
            }
        }
    )

const wholesaleFlow = addKeyword(EVENTS.ACTION)
    .addAnswer(
        [
            '📦 *SNEDA Wholesale & B2B*',
            'We offer bulk pricing and commercial distribution across Accra and beyond.',
            '',
            '👉 *Please specify your Business Name and the bulk items/quantities you require:*'
        ].join('\n'),
        { capture: true },
        async (ctx, { gotoFlow, provider, flowDynamic }) => {
            if (isPaused(ctx)) return notePausedMessage(ctx)
            if (isGreeting(ctx.body)) { userCarts.delete(ctx.from); userLists.delete(ctx.from); return gotoFlow(welcomeFlow) }

            await simulateTyping(ctx, provider, 2000)
            await logCustomerOrderToPg(ctx.from, `WHOLESALE INQUIRY: ${ctx.body}`)
            const aiResponse = await getGroqCompletion(ctx.from, `Wholesale inquiry: ${ctx.body}`)
            await flowDynamic(aiResponse)
            return gotoFlow(orderCaptureFlow)
        }
    )

const supportFlow = addKeyword(EVENTS.ACTION)
    .addAnswer(
        [
            '📞 *SNEDA Support Center*',
            'Our support team and AI assistant are ready to help you.',
            '',
            '👉 *Please type your inquiry below:*'
        ].join('\n'),
        { capture: true },
        async (ctx, { gotoFlow, provider, flowDynamic }) => {
            if (isPaused(ctx)) return notePausedMessage(ctx)
            if (isGreeting(ctx.body)) { userCarts.delete(ctx.from); userLists.delete(ctx.from); return gotoFlow(welcomeFlow) }

            await simulateTyping(ctx, provider, 2000)
            const aiResponse = await getGroqCompletion(ctx.from, ctx.body)
            await flowDynamic(aiResponse)
            return gotoFlow(orderCaptureFlow)
        }
    )

const invoiceFlow = addKeyword(EVENTS.ACTION)
    .addAction(async (ctx, { provider, flowDynamic, gotoFlow }) => {
        if (isPaused(ctx)) return notePausedMessage(ctx)
        const cart = getCart(ctx.from)
        if (!cart) {
            await flowDynamic('I could not find an active order. Type *menu* to start a new one.')
            return gotoFlow(orderCaptureFlow)
        }

        noteCartDetails(ctx.from, ctx.body)

        if (!cart.location) {
            const details = await getDeliveryDetails(ctx.from)
            if (details.location) cart.location = details.location
            if (details.name) cart.customerName = details.name
        }
        if (!cart.location) return gotoFlow(deliveryDetailsFlow)

        await simulateTyping(ctx, provider, 1500)
        const ok = await issueInvoice(ctx, provider, flowDynamic)
        if (!ok) await flowDynamic('Sorry, I could not generate your invoice. A sales representative will help you shortly.')
        return gotoFlow(orderCaptureFlow)
    })

const deliveryDetailsFlow = addKeyword(EVENTS.ACTION)
    .addAnswer(
        '📍 Almost done! Please send your *delivery location* in Accra (area and nearest landmark) so we can complete your invoice:',
        { capture: true },
        async (ctx, { gotoFlow }) => {
            if (isPaused(ctx)) return notePausedMessage(ctx)
            if (isGreeting(ctx.body)) { userCarts.delete(ctx.from); userLists.delete(ctx.from); return gotoFlow(welcomeFlow) }
            const cart = getCart(ctx.from)
            if (cart) cart.location = ctx.body.trim()
            return gotoFlow(invoiceFlow)
        }
    )

// =============================================================
// 🛒 6. WELCOME MENU (reached only through gotoFlow, no keywords)
// =============================================================
const welcomeFlow = addKeyword(EVENTS.ACTION)
    .addAnswer(
        [
            '🛒 *Welcome to SNEDA Shopping Center!*',
            'We’re thrilled to have you here today.',
            '',
            'How can we assist you today? Please select an option:',
            '',
            '1️⃣ *Retail & Store Website* (Daily deals, online shopping)',
            '2️⃣ *Wholesale Marketing* (Bulk orders, distributor pricing)',
            '3️⃣ *General Inquiry / Support*',
            '',
            '👉 *Reply with 1, 2, or 3 (or ask any question directly!):*'
        ].join('\n'),
        { capture: true },
        async (ctx, { gotoFlow, provider, flowDynamic }) => {
            if (isPaused(ctx)) return notePausedMessage(ctx)
            const userInput = ctx.body.trim().toLowerCase()

            if (userInput === '1' || userInput.includes('retail')) {
                return gotoFlow(retailFlow)
            }
            if (userInput === '2' || userInput.includes('wholesale')) {
                return gotoFlow(wholesaleFlow)
            }
            if (userInput === '3' || userInput.includes('support')) {
                return gotoFlow(supportFlow)
            }

            await simulateTyping(ctx, provider, 2000)
            const aiReply = await getGroqCompletion(ctx.from, ctx.body)
            await flowDynamic(aiReply)
            return gotoFlow(orderCaptureFlow)
        }
    )

// Catches every first message from a customer
const globalFallbackFlow = addKeyword(EVENTS.WELCOME)
    .addAction(async (ctx, { provider, flowDynamic, gotoFlow }) => {
        if (isPaused(ctx)) return notePausedMessage(ctx)
        if (isGreeting(ctx.body)) { userCarts.delete(ctx.from); userLists.delete(ctx.from); return gotoFlow(welcomeFlow) }
        if (shouldInvoice(ctx.from, ctx.body)) return gotoFlow(invoiceFlow)
        if (isOwner(ctx.from) && reportCommand(ctx.body)) {
            await sendDailyReport(provider, reportCommand(ctx.body), [ctx.from])
            return gotoFlow(orderCaptureFlow)
        }
        if (isOwner(ctx.from)) {
            const reply = await handleOwnerCommand(ctx.body)
            if (reply) {
                await flowDynamic(reply)
                return gotoFlow(orderCaptureFlow)
            }
        }

        await simulateTyping(ctx, provider, 1800)
        const aiReply = await getGroqCompletion(ctx.from, ctx.body)
        await flowDynamic(aiReply)
        return gotoFlow(orderCaptureFlow)
    })

// =============================================================
// 🚀 7. BOT INITIALIZATION
// =============================================================
const main = async () => {
    await initDbTables()
    await restoreSessionFromDb() // must run before the WhatsApp provider starts
    await loadPauses()

    const adapterFlow = createFlow([
        globalFallbackFlow,
        welcomeFlow,
        retailFlow,
        categorySelectionFlow,
        orderPromptFlow,
        orderCaptureFlow,
        invoiceFlow,
        deliveryDetailsFlow,
        wholesaleFlow,
        supportFlow
    ])

    const adapterProvider = createProvider(BaileysProvider)

    // MemoryDB is enough: chat history, orders and inventory
    // are stored in Supabase through the `pool` queries above.
    const adapterDB = new MemoryDB()

    const { httpServer } = await createBot({
        flow: adapterFlow,
        provider: adapterProvider,
        database: adapterDB,
    }, {
        queue: { timeout: 45000, concurrencyLimit: 50 }
    })

    // ---- Health endpoints (for UptimeRobot) ----
    const sendJson = (res, status, body) => {
        res.writeHead(status, { 'Content-Type': 'application/json' })
        res.end(JSON.stringify(body))
    }

    // /health      -> 200 as long as the bot process is running (use this to keep a host awake)
    const healthHandler = (_req, res) =>
        sendJson(res, 200, { status: 'ok', uptime_seconds: Math.round(process.uptime()) })

    // /health/full -> 200 only if Supabase AND WhatsApp are both working, otherwise 503 (use this for alerts)
    const fullHealthHandler = async (_req, res) => {
        let dbOk = false
        try {
            await withTimeout(pool.query('SELECT 1').then(() => { dbOk = true }), 5000)
        } catch {
            dbOk = false
        }
        const whatsappOk = !!adapterProvider.vendor?.user
        sendJson(res, dbOk && whatsappOk ? 200 : 503, {
            status: dbOk && whatsappOk ? 'ok' : 'degraded',
            database: dbOk ? 'up' : 'down',
            whatsapp: whatsappOk ? 'connected' : 'disconnected',
            uptime_seconds: Math.round(process.uptime())
        })
    }

    // UptimeRobot may use GET or HEAD requests, so both are supported
    for (const method of ['get', 'head']) {
        adapterProvider.server[method]('/health', healthHandler)
        adapterProvider.server[method]('/health/full', fullHealthHandler)
    }

    // ---- QR page: link WhatsApp from any browser (no need to read the Render logs) ----
    const QR_KEY = process.env.QR_KEY || crypto.randomBytes(6).toString('hex')
    const qrAllowed = (req) => new URL(req.url, 'http://x').searchParams.get('key') === QR_KEY
    const sendHtml = (res, status, html) => {
        res.writeHead(status, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' })
        res.end(html)
    }
    const qrPage = (inner, refresh) =>
        `<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">` +
        (refresh ? `<meta http-equiv="refresh" content="${refresh}">` : '') +
        `<body style="font-family:sans-serif;text-align:center;padding:24px">${inner}</body>`

    adapterProvider.server.get('/qr', (req, res) => {
        if (!qrAllowed(req)) return sendHtml(res, 403, 'Forbidden')
        if (adapterProvider.vendor?.user) {
            return sendHtml(res, 200, qrPage('<h2>✅ WhatsApp is connected</h2><p>No QR code needed.</p>'))
        }
        if (!fs.existsSync(path.resolve('bot.qr.png'))) {
            return sendHtml(res, 200, qrPage('<h2>⏳ Waiting for a QR code…</h2><p>This page refreshes by itself.</p>', 5))
        }
        sendHtml(res, 200, qrPage(
            `<h2>Scan with WhatsApp</h2><p>Settings → Linked devices → Link a device</p>` +
            `<img src="/qr.png?key=${QR_KEY}&t=${Date.now()}" style="width:min(320px,90vw)">` +
            `<p>The code changes every ~20 seconds, so this page refreshes automatically.</p>`, 10))
    })

    adapterProvider.server.get('/qr.png', (req, res) => {
        const file = path.resolve('bot.qr.png')
        if (!qrAllowed(req) || !fs.existsSync(file)) {
            res.writeHead(404)
            return res.end()
        }
        res.writeHead(200, { 'Content-Type': 'image/png', 'Cache-Control': 'no-store' })
        res.end(fs.readFileSync(file))
    })

    // Save the WhatsApp login to Supabase every 15 seconds (only changed files are uploaded)
    setInterval(backupSessionToDb, 15000)

    // ---- Keep-alive: pings this bot's own public URL every 5 minutes so Render's free plan does not put it to sleep ----
    // Render sets RENDER_EXTERNAL_URL automatically. You can also set PUBLIC_URL yourself. Off when running on your PC.
    const KEEP_ALIVE_URL = (process.env.PUBLIC_URL || process.env.RENDER_EXTERNAL_URL || '').replace(/\/$/, '')
    if (KEEP_ALIVE_URL) {
        const pingSelf = async () => {
            try {
                const res = await fetch(`${KEEP_ALIVE_URL}/health`, { signal: AbortSignal.timeout(15000) })
                if (!res.ok) console.warn(`⚠️ Keep-alive ping returned status ${res.status}`)
            } catch (err) {
                console.warn('⚠️ Keep-alive ping failed:', err?.message)
            }
        }
        setInterval(pingSelf, 5 * 60 * 1000)
        console.log(`⏰ Keep-alive on: pinging ${KEEP_ALIVE_URL}/health every 5 minutes`)
    } else {
        console.log('⏰ Keep-alive off (no RENDER_EXTERNAL_URL or PUBLIC_URL found)')
    }

    // ---- Human takeover: pause the bot while a person is chatting with a customer ----
    if (HUMAN_TAKEOVER) startHumanTakeoverWatcher(adapterProvider)

    // ---- Daily sales report: checks every minute, sends once a day at REPORT_TIME (Accra time) ----
    if (OWNER_NUMBERS.length) {
        setInterval(() => runReportCheck(adapterProvider), 60 * 1000)
        console.log(`📊 Daily sales report on: sent at ${REPORT_TIME} (Accra time) to ${OWNER_NUMBERS.length} number(s). Owner can also send "report".`)
    } else {
        console.log('📊 Daily sales report off: set OWNER_NUMBER to turn it on')
    }

    // Hosts like Render/Northflank give the port through the PORT variable
    const PORT = Number(process.env.PORT) || 3008
    httpServer(PORT)
    console.log(`🌐 Server running on port ${PORT}. Bot connected live to Supabase PostgreSQL!`)
    console.log(`❤️ Health checks: /health (alive) and /health/full (database + WhatsApp)`)
    console.log(`📱 To link WhatsApp, open: https://YOUR-APP.onrender.com/qr?key=${QR_KEY}`)
}

main()
