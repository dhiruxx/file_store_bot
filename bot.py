import os
import logging
import secrets
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, Request, HTTPException
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)

# =========================
# ENVIRONMENT CONFIGURATION
# =========================
BOT_TOKEN = os.environ["BOT_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]
STORAGE_CHANNEL_ID = int(os.environ["STORAGE_CHANNEL_ID"])
ADMIN_IDS = {
    int(x.strip()) for x in os.environ.get("ADMIN_IDS", "").split(",")
    if x.strip()
}

# Exactly five channels. Use numeric channel IDs, e.g. -1001234567890
FORCE_CHANNELS = [
    int(x.strip()) for x in os.environ.get("FORCE_CHANNELS", "").split(",")
    if x.strip()
]
FORCE_LINKS = [
    x.strip() for x in os.environ.get("FORCE_LINKS", "").split(",")
    if x.strip()
]

MAX_FILES = 1000
MAX_FILE_MB = 40
MAX_FILE_BYTES = MAX_FILE_MB * 1024 * 1024

WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "").rstrip("/")
PORT = int(os.environ.get("PORT", "10000"))

if len(FORCE_CHANNELS) != 5 or len(FORCE_LINKS) != 5:
    raise RuntimeError("FORCE_CHANNELS aur FORCE_LINKS mein exactly 5-5 values honi chahiye.")

if len(ADMIN_IDS) == 0:
    raise RuntimeError("ADMIN_IDS mein kam se kam ek admin Telegram ID daalo.")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
log = logging.getLogger("file-store-bot")

db_pool = None
telegram_app = None
# Admin uploads currently being collected: user_id -> list of storage channel message IDs
upload_sessions = {}


# =========================
# DATABASE
# =========================
async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=5,
        command_timeout=60,
        ssl="require"
    )
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                first_seen TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS files (
                id BIGSERIAL PRIMARY KEY,
                storage_message_id BIGINT NOT NULL,
                file_size BIGINT NOT NULL,
                added_by BIGINT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS bundles (
                code TEXT PRIMARY KEY,
                created_by BIGINT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS bundle_files (
                bundle_code TEXT REFERENCES bundles(code) ON DELETE CASCADE,
                file_id BIGINT REFERENCES files(id) ON DELETE CASCADE,
                position INTEGER NOT NULL,
                PRIMARY KEY (bundle_code, file_id)
            );
        """)
    log.info("Database ready")


async def close_db():
    global db_pool
    if db_pool:
        await db_pool.close()
        db_pool = None


async def is_subscribed(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    for channel_id in FORCE_CHANNELS:
        try:
            member = await context.bot.get_chat_member(channel_id, user_id)
            if member.status not in ("member", "administrator", "creator"):
                return False
        except Exception as exc:
            log.warning("Subscription check failed for %s: %s", channel_id, exc)
            return False
    return True


def force_sub_keyboard():
    rows = []
    for index, link in enumerate(FORCE_LINKS, start=1):
        rows.append([InlineKeyboardButton(f"Join Channel {index}", url=link)])
    rows.append([InlineKeyboardButton("I have joined - Verify", callback_data="verify_sub")])
    return InlineKeyboardMarkup(rows)


async def require_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return False
    if await is_subscribed(user.id, context):
        return True

    text = "Bot use karne se pehle neeche diye gaye 5 channels join karo. Join karne ke baad Verify dabao."
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(
            text, reply_markup=force_sub_keyboard()
        )
    elif update.effective_message:
        await update.effective_message.reply_text(
            text, reply_markup=force_sub_keyboard()
        )
    return False


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


async def save_user(user_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO users(user_id) VALUES($1) ON CONFLICT DO NOTHING",
            user_id
        )


# =========================
# TELEGRAM COMMANDS
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await save_user(user.id)

    if not await require_subscription(update, context):
        return

    if context.args:
        code = context.args[0].strip()
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT f.storage_message_id
                FROM bundle_files bf
                JOIN files f ON f.id = bf.file_id
                WHERE bf.bundle_code = $1
                ORDER BY bf.position
            """, code)

        if not rows:
            await update.message.reply_text("Ye link invalid hai ya files available nahi hain.")
            return

        await update.message.reply_text(f"Is link mein {len(rows)} file(s) hain. Files bheji ja rahi hain...")
        for row in rows:
            try:
                await context.bot.copy_message(
                    chat_id=user.id,
                    from_chat_id=STORAGE_CHANNEL_ID,
                    message_id=row["storage_message_id"]
                )
            except Exception:
                log.exception("Could not deliver stored message %s", row["storage_message_id"])
                await update.message.reply_text(
                    "Ek file deliver nahi ho paayi. Admin ko inform karo."
                )
        return

    await update.message.reply_text(
        "Welcome to File Store Bot!\n\n"
        "Admin files add karne ke liye /addfiles use kare.\n"
        "Files complete hone ke baad /done bheje.\n\n"
        "Commands:\n"
        "/start - Start bot\n"
        "/addfiles - Naya share link banana shuru\n"
        "/done - Share link generate\n"
        "/cancel - Upload cancel\n"
        "/stats - Bot statistics (admin)"
    )


async def verify_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if await is_subscribed(query.from_user.id, context):
        await query.edit_message_text("Subscription verify ho gaya. Ab /start bhejo.")
    else:
        await query.edit_message_text(
            "Abhi sabhi 5 channels join nahi hue. Join karke dobara Verify dabao.",
            reply_markup=force_sub_keyboard()
        )


async def addfiles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Ye command sirf admin ke liye hai.")
        return
    if not await require_subscription(update, context):
        return

    upload_sessions[user_id] = []
    await update.message.reply_text(
        "Ab files bhejo. Har file 40 MB ya usse chhoti honi chahiye.\n"
        "Jitni files chaho bhej sakte ho. Complete hone par /done bhejo.\n"
        "Cancel karne ke liye /cancel."
    )


def get_message_file_size(message):
    media = message.document or message.video or message.audio
    if media:
        return media.file_size or 0
    if message.photo:
        return message.photo[-1].file_size or 0
    animation = message.animation
    if animation:
        return animation.file_size or 0
    return None


async def receive_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return
    if user_id not in upload_sessions:
        await update.message.reply_text("Pehle /addfiles bhejo.")
        return

    size = get_message_file_size(update.message)
    if size is None:
        await update.message.reply_text("Photo, video, audio, animation ya document bhejo.")
        return
    if size > MAX_FILE_BYTES:
        await update.message.reply_text("File 40 MB se badi hai. Is file ko add nahi kiya.")
        return

    current_count = len(upload_sessions[user_id])
    if current_count >= MAX_FILES:
        await update.message.reply_text("Ek bundle mein maximum 1000 files add kar sakte ho.")
        return

    async with db_pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM files")
    if total >= MAX_FILES:
        await update.message.reply_text(
            "Storage limit 1000 files poori ho chuki hai. Admin ko database se purani files clean karni hongi."
        )
        return

    try:
        copied = await context.bot.copy_message(
            chat_id=STORAGE_CHANNEL_ID,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id
        )
        upload_sessions[user_id].append((copied.message_id, size))
        await update.message.reply_text(
            f"File add ho gayi. Is bundle mein ab {len(upload_sessions[user_id])} file(s) hain."
        )
    except Exception as exc:
        log.exception("Storage copy failed")
        await update.message.reply_text(f"Storage channel mein file save nahi hui: {exc}")


async def done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Ye command sirf admin ke liye hai.")
        return

    items = upload_sessions.get(user_id, [])
    if not items:
        await update.message.reply_text("Koi file add nahi hui. /addfiles se shuru karo.")
        return

    async with db_pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM files")
        if total + len(items) > MAX_FILES:
            await update.message.reply_text("Total storage limit 1000 files cross ho jayegi.")
            return

        code = secrets.token_urlsafe(8).replace("-", "").replace("_", "")
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO bundles(code, created_by) VALUES($1, $2)",
                code, user_id
            )
            for position, (message_id, size) in enumerate(items, start=1):
                file_id = await conn.fetchval("""
                    INSERT INTO files(storage_message_id, file_size, added_by)
                    VALUES($1, $2, $3) RETURNING id
                """, message_id, size, user_id)
                await conn.execute("""
                    INSERT INTO bundle_files(bundle_code, file_id, position)
                    VALUES($1, $2, $3)
                """, code, file_id, position)

    upload_sessions.pop(user_id, None)
    me = await context.bot.get_me()
    link = f"https://t.me/{me.username}?start={code}"
    await update.message.reply_text(
        f"Done! {len(items)} file(s) ka share link ban gaya:\n\n{link}",
        disable_web_page_preview=True
    )


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return
    upload_sessions.pop(user_id, None)
    await update.message.reply_text("Current upload session cancel ho gaya. Storage channel mein copied files reh sakti hain.")


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Ye command sirf admin ke liye hai.")
        return

    async with db_pool.acquire() as conn:
        users = await conn.fetchval("SELECT COUNT(*) FROM users")
        files = await conn.fetchval("SELECT COUNT(*) FROM files")
        bundles = await conn.fetchval("SELECT COUNT(*) FROM bundles")
        size = await conn.fetchval("SELECT COALESCE(SUM(file_size), 0) FROM files")

    await update.message.reply_text(
        f"Bot Stats\n\nUsers: {users}\nFiles: {files}/{MAX_FILES}\n"
        f"Share links: {bundles}\nStored size: {size / (1024 * 1024):.2f} MB"
    )


# =========================
# FASTAPI WEBHOOK
# =========================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global telegram_app
    await init_db()

    telegram_app = (
        Application.builder()
        .token(BOT_TOKEN)
        .build()
    )

    telegram_app.add_handler(CommandHandler("start", start))
    telegram_app.add_handler(CommandHandler("addfiles", addfiles))
    telegram_app.add_handler(CommandHandler("done", done))
    telegram_app.add_handler(CommandHandler("cancel", cancel))
    telegram_app.add_handler(CommandHandler("stats", stats))
    telegram_app.add_handler(CallbackQueryHandler(verify_subscription, pattern="^verify_sub$"))
    telegram_app.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE & (
                filters.Document.ALL | filters.VIDEO | filters.AUDIO |
                filters.PHOTO | filters.ANIMATION
            ),
            receive_file
        )
    )

    await telegram_app.initialize()
    await telegram_app.start()

    if WEBHOOK_URL:
        await telegram_app.bot.set_webhook(
            url=f"{WEBHOOK_URL}/telegram",
            secret_token=WEBHOOK_SECRET or None,
            allowed_updates=Update.ALL_TYPES
        )

    log.info("Bot started")
    try:
        yield
    finally:
        if WEBHOOK_URL:
            await telegram_app.bot.delete_webhook()
        await telegram_app.stop()
        await telegram_app.shutdown()
        await close_db()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def home():
    return {"status": "File Store Bot is running"}


@app.post("/telegram")
async def telegram_webhook(request: Request):
    if WEBHOOK_SECRET:
        supplied = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if supplied != WEBHOOK_SECRET:
            raise HTTPException(status_code=403, detail="Invalid webhook secret")

    data = await request.json()
    update = Update.de_json(data, telegram_app.bot)
    await telegram_app.process_update(update)
    return {"ok": True}
    
