
import os
import re
import asyncio
import logging
import secrets
from contextlib import asynccontextmanager
from datetime import timedelta

import asyncpg
from fastapi import FastAPI, Request, HTTPException
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.error import RetryAfter, TelegramError
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# ==================================================
# CONFIGURATION
# ==================================================

BOT_TOKEN = os.environ["BOT_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]
STORAGE_CHANNEL_ID = int(os.environ["STORAGE_CHANNEL_ID"])

WEBHOOK_URL = os.environ["WEBHOOK_URL"].rstrip("/")
WEBHOOK_SECRET = os.environ["WEBHOOK_SECRET"]

ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip()
}

FORCE_CHANNELS = [
    x.strip()
    for x in os.getenv("FORCE_CHANNELS", "").split(",")
    if x.strip()
]

FORCE_LINKS = [
    x.strip()
    for x in os.getenv("FORCE_LINKS", "").split(",")
    if x.strip()
]

MAX_FILES = 1000
MAX_FILE_SIZE = 40 * 1024 * 1024
LINK_HOURS = 2

if len(FORCE_CHANNELS) != 5 or len(FORCE_LINKS) != 5:
    raise RuntimeError(
        "Exactly 5 FORCE_CHANNELS and 5 FORCE_LINKS are required."
    )

if len(WEBHOOK_SECRET) < 16:
    raise RuntimeError("WEBHOOK_SECRET must be at least 16 characters.")

if not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", WEBHOOK_SECRET):
    raise RuntimeError("Invalid WEBHOOK_SECRET format.")

# ==================================================
# LOGGING
# ==================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("file_store_bot")

# ==================================================
# GLOBAL OBJECTS
# ==================================================

pool = None
tg_app = Application.builder().token(BOT_TOKEN).build()


# ==================================================
# DATABASE
# ==================================================

async def init_database():
    global pool

    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=5,
        ssl="require",
        command_timeout=60,
    )

    async with pool.acquire() as con:
        await con.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS storage_lock (
                id INTEGER PRIMARY KEY CHECK (id = 1)
            );

            INSERT INTO storage_lock (id)
            VALUES (1)
            ON CONFLICT DO NOTHING;

            CREATE TABLE IF NOT EXISTS upload_sessions (
                id BIGSERIAL PRIMARY KEY,
                owner_id BIGINT NOT NULL,
                active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS files (
                id BIGSERIAL PRIMARY KEY,
                owner_id BIGINT NOT NULL,
                storage_message_id BIGINT NOT NULL,
                file_name TEXT,
                file_size BIGINT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS session_files (
                session_id BIGINT REFERENCES upload_sessions(id)
                    ON DELETE CASCADE,
                file_id BIGINT REFERENCES files(id)
                    ON DELETE CASCADE,
                PRIMARY KEY (session_id, file_id)
            );

            CREATE TABLE IF NOT EXISTS bundles (
                id BIGSERIAL PRIMARY KEY,
                token TEXT UNIQUE NOT NULL,
                owner_id BIGINT NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS bundle_files (
                bundle_id BIGINT REFERENCES bundles(id)
                    ON DELETE CASCADE,
                file_id BIGINT REFERENCES files(id),
                PRIMARY KEY (bundle_id, file_id)
            );

            CREATE INDEX IF NOT EXISTS idx_files_owner
            ON files(owner_id);

            CREATE INDEX IF NOT EXISTS idx_bundles_token
            ON bundles(token);

            CREATE INDEX IF NOT EXISTS idx_bundle_expiry
            ON bundles(expires_at);
        """)

    logger.info("Database initialized successfully.")


async def register_user(user):
    if not user:
        return

    await pool.execute(
        """
        INSERT INTO users (user_id, username, first_name)
        VALUES ($1, $2, $3)
        ON CONFLICT (user_id)
        DO UPDATE SET
            username = EXCLUDED.username,
            first_name = EXCLUDED.first_name
        """,
        user.id,
        user.username,
        user.first_name,
    )


# ==================================================
# SUBSCRIPTION SYSTEM
# ==================================================

async def check_subscription(user_id):
    for channel in FORCE_CHANNELS:
        try:
            member = await tg_app.bot.get_chat_member(
                chat_id=channel,
                user_id=user_id,
            )

            if member.status in ("left", "kicked"):
                return False

            if member.status == "restricted":
                if not getattr(member, "is_member", False):
                    return False

        except TelegramError:
            logger.exception(
                "Subscription verification failed for channel %s",
                channel,
            )
            return False

    return True


async def subscription_message(update, token=None):
    keyboard = []

    for i, link in enumerate(FORCE_LINKS, start=1):
        keyboard.append([
            InlineKeyboardButton(
                f"Join Channel {i}",
                url=link,
            )
        ])

    callback = f"v:{token}" if token else "v:home"

    keyboard.append([
        InlineKeyboardButton(
            "Verify Subscription",
            callback_data=callback,
        )
    ])

    text = (
        "Welcome to File Store Bot!\n\n"
        "Bot use karne se pehle hamare "
        "5 channels join karo.\n\n"
        "Sabhi channels join karne ke baad "
        "Verify Subscription dabao."
    )

    if update.callback_query:
        await update.callback_query.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    else:
        await update.effective_message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )


# ==================================================
# START COMMAND
# ==================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await register_user(user)

    token = context.args[0] if context.args else None

    if not await check_subscription(user.id):
        await subscription_message(update, token)
        return

    if token:
        await deliver_bundle(user.id, token, context)
        return

    await update.effective_message.reply_text(
        "Welcome to File Store Bot!\n\n"
        "Commands:\n"
        "/addfiles - Start uploading files\n"
        "/done - Finish upload and create link\n"
        "/myfiles - See your uploaded files\n"
        "/help - Help and instructions\n\n"
        "Upload karne ke liye /addfiles dabao."
    )


# ==================================================
# SUBSCRIPTION VERIFY
# ==================================================

async def verify_callback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    query = update.callback_query
    await query.answer()

    user = query.from_user
    await register_user(user)

    token = query.data.split(":", 1)[1]

    if not await check_subscription(user.id):
        await query.message.reply_text(
            "Abhi bhi kuch channels join nahi hue hain. "
            "Sabhi 5 channels join karke dobara Verify karo."
        )
        return

    await query.message.reply_text(
        "Subscription verified successfully!"
    )

    if token == "home":
        await query.message.reply_text(
            "Bot ready hai. Upload ke liye /addfiles bhejo."
        )
    else:
        await deliver_bundle(user.id, token, context)


# ==================================================
# UPLOAD SESSION
# ==================================================

async def addfiles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await register_user(user)

    if not await check_subscription(user.id):
        await subscription_message(update)
        return

    async with pool.acquire() as con:
        async with con.transaction():
            await con.execute(
                """
                UPDATE upload_sessions
                SET active = FALSE
                WHERE owner_id = $1 AND active = TRUE
                """,
                user.id,
            )

            session_id = await con.fetchval(
                """
                INSERT INTO upload_sessions (owner_id)
                VALUES ($1)
                RETURNING id
                """,
                user.id,
            )

    await update.effective_message.reply_text(
        "Upload session start ho gaya!\n\n"
        "Ab apni files bhejo.\n"
        "Har file maximum 40 MB ki honi chahiye.\n"
        "Upload complete hone ke baad /done bhejo.\n\n"
        "Files ko isi session mein ek sharing link milega."
    )


# ==================================================
# FILE DETAILS
# ==================================================

def get_file_details(message):
    media = [
        ("document", message.document),
        ("video", message.video),
        ("audio", message.audio),
        ("animation", message.animation),
        ("voice", message.voice),
        ("video_note", message.video_note),
    ]

    for file_type, item in media:
        if item:
            name = getattr(item, "file_name", None)
            return (
                file_type,
                item.file_size or 0,
                name or file_type,
            )

    if message.photo:
        item = message.photo[-1]
        return ("photo", item.file_size or 0, "photo.jpg")

    return None


# ==================================================
# RECEIVE AND STORE FILES
# ==================================================

async def receive_file(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    user = update.effective_user
    message = update.effective_message

    await register_user(user)

    if not await check_subscription(user.id):
        await subscription_message(update)
        return

    details = get_file_details(message)

    if not details:
        return

    file_type, file_size, file_name = details

    if file_size > MAX_FILE_SIZE:
        await message.reply_text(
            "File 40 MB se badi hai. "
            "Isliye upload accept nahi hua."
        )
        return

    async with pool.acquire() as con:
        async with con.transaction():
            session = await con.fetchrow(
                """
                SELECT id FROM upload_sessions
                WHERE owner_id = $1 AND active = TRUE
                ORDER BY id DESC LIMIT 1
                FOR UPDATE
                """,
                user.id,
            )

            if not session:
                await message.reply_text(
                    "Pehle /addfiles bhejo, "
                    "uske baad files upload karo."
                )
                return

            await con.fetchrow(
                "SELECT id FROM storage_lock WHERE id = 1 FOR UPDATE"
            )

            total = await con.fetchval(
                "SELECT COUNT(*) FROM files"
            )

            if total >= MAX_FILES:
                await message.reply_text(
                    "Storage ki 1000 files ki limit complete ho gayi."
                )
                return

            try:
                copied = await context.bot.copy_message(
                    chat_id=STORAGE_CHANNEL_ID,
                    from_chat_id=user.id,
                    message_id=message.message_id,
                )

            except TelegramError:
                logger.exception("Storage channel copy failed")
                await message.reply_text(
                    "File storage channel mein save nahi hui. "
                    "Admin se contact karo."
                )
                return

            file_id = await con.fetchval(
                """
                INSERT INTO files (
                    owner_id,
                    storage_message_id,
                    file_name,
                    file_size
                )
                VALUES ($1, $2, $3, $4)
                RETURNING id
                """,
                user.id,
                copied.message_id,
                file_name,
                file_size,
            )

            await con.execute(
                """
                INSERT INTO session_files (session_id, file_id)
                VALUES ($1, $2)
                """,
                session["id"],
                file_id,
            )

    await message.reply_text(
        f"File successfully saved!\n\n"
        f"Name: {file_name}\n"
        f"Size: {file_size / (1024 * 1024):.2f} MB\n"
        f"Total storage files: {total + 1}/{MAX_FILES}\n\n"
        f"Next file bhejo ya /done dabao."
    )


# ==================================================
# CREATE SHARE LINK
# ==================================================

async def done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await register_user(user)

    if not await check_subscription(user.id):
        await subscription_message(update)
        return

    async with pool.acquire() as con:
        async with con.transaction():
            session = await con.fetchrow(
                """
                SELECT id FROM upload_sessions
                WHERE owner_id = $1 AND active = TRUE
                ORDER BY id DESC LIMIT 1
                FOR UPDATE
                """,
                user.id,
            )

            if not session:
                await update.effective_message.reply_text(
                    "Koi active upload session nahi hai. "
                    "/addfiles bhejo."
                )
                return

            file_ids = await con.fetch(
                """
                SELECT file_id FROM session_files
                WHERE session_id = $1
                ORDER BY file_id
                """,
                session["id"],
            )

            if not file_ids:
                await update.effective_message.reply_text(
                    "Is session mein koi file upload nahi hui."
                )
                return

            token = secrets.token_urlsafe(12)

            bundle_id = await con.fetchval(
                """
                INSERT INTO bundles (
                    token, owner_id, expires_at
                )
                VALUES (
                    $1, $2, NOW() + INTERVAL '2 hours'
                )
                RETURNING id
                """,
                token,
                user.id,
            )

            await con.executemany(
                """
                INSERT INTO bundle_files (bundle_id, file_id)
                VALUES ($1, $2)
                """,
                [
                    (bundle_id, row["file_id"])
                    for row in file_ids
                ],
            )

            await con.execute(
                """
                UPDATE upload_sessions
                SET active = FALSE
                WHERE id = $1
                """,
                session["id"],
            )

    bot_info = await context.bot.get_me()
    share_link = f"https://t.me/{bot_info.username}?start={token}"

    await update.effective_message.reply_text(
        "Your sharing link is ready!\n\n"
        f"Total files: {len(file_ids)}\n"
        "Expiry: 2 hours\n\n"
        f"{share_link}\n\n"
        "Is link ko kisi bhi user ke saath share kar sakte ho."
    )


# ==================================================
# DELIVER BUNDLE
# ==================================================

async def deliver_bundle(user_id, token, context):
    async with pool.acquire() as con:
        bundle = await con.fetchrow(
            """
            SELECT id, expires_at
            FROM bundles
            WHERE token = $1 AND expires_at > NOW()
            """,
            token,
        )

        if not bundle:
            await context.bot.send_message(
                user_id,
                "Ye link expire ho chuka hai ya invalid hai."
            )
            return

        files = await con.fetch(
            """
            SELECT f.storage_message_id
            FROM bundle_files bf
            JOIN files f ON f.id = bf.file_id
            WHERE bf.bundle_id = $1
            ORDER BY f.id
            """,
            bundle["id"],
        )

    if not files:
        await context.bot.send_message(
            user_id,
            "Is link mein koi file available nahi hai."
        )
        return

    await context.bot.send_message(
        user_id,
        f"Link verified!\nTotal files: {len(files)}\n"
        "Files bheji ja rahi hain..."
    )

    successful = 0
    failed = 0

    for item in files:
        while True:
            try:
                await context.bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=STORAGE_CHANNEL_ID,
                    message_id=item["storage_message_id"],
                )
                successful += 1
                break

            except RetryAfter as exc:
                await asyncio.sleep(float(exc.retry_after) + 1)

            except TelegramError:
                logger.exception(
                    "File delivery failed for user %s",
                    user_id,
                )
                failed += 1
                break

    await context.bot.send_message(
        user_id,
        f"Delivery completed!\n"
        f"Successful: {successful}\n"
        f"Failed: {failed}"
    )


# ==================================================
# MY FILES
# ==================================================

async def myfiles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await register_user(user)

    if not await check_subscription(user.id):
        await subscription_message(update)
        return

    async with pool.acquire() as con:
        rows = await con.fetch(
            """
            SELECT id, file_name, file_size, created_at
            FROM files
            WHERE owner_id = $1
            ORDER BY id DESC
            LIMIT 20
            """,
            user.id,
        )

    if not rows:
        await update.effective_message.reply_text(
            "Abhi tumne koi file upload nahi ki hai."
        )
        return

    text = "Your latest 20 uploaded files:\n\n"

    for row in rows:
        text += (
            f"ID: {row['id']}\n"
            f"Name: {row['file_name']}\n"
            f"Size: {row['file_size'] / (1024 * 1024):.2f} MB\n\n"
        )

    await update.effective_message.reply_text(text)


# ==================================================
# ADMIN STATS
# ==================================================

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await register_user(user)

    if user.id not in ADMIN_IDS:
        await update.effective_message.reply_text(
            "You are not authorized to use this command."
        )
        return

    async with pool.acquire() as con:
        total_users = await con.fetchval(
            "SELECT COUNT(*) FROM users"
        )

        total_files = await con.fetchval(
            "SELECT COUNT(*) FROM files"
        )

        total_bundles = await con.fetchval(
            "SELECT COUNT(*) FROM bundles"
        )

    await update.effective_message.reply_text(
        "ADMIN STATISTICS\n\n"
        f"Users: {total_users}\n"
        f"Files: {total_files}/{MAX_FILES}\n"
        f"Bundles: {total_bundles}"
    )


# ==================================================
# HELP
# ==================================================

async def help_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    await update.effective_message.reply_text(
        "FILE STORE BOT HELP\n\n"
        "/start - Start bot\n"
        "/addfiles - Start uploading\n"
        "/done - Create sharing link\n"
        "/myfiles - See your files\n"
        "/help - Help\n\n"
        "Maximum file size: 40 MB\n"
        "Total file capacity: 1000\n"
        "Link expiry: 2 hours\n"
    )


# ==================================================
# ERROR HANDLER
# ==================================================

async def error_handler(update, context):
    logger.error(
        "Unhandled bot error",
        exc_info=context.error,
    )


# ==================================================
# REGISTER HANDLERS
# ==================================================

tg_app.add_handler(CommandHandler("start", start))
tg_app.add_handler(CommandHandler("addfiles", addfiles))
tg_app.add_handler(CommandHandler("done", done))
tg_app.add_handler(CommandHandler("myfiles", myfiles))
tg_app.add_handler(CommandHandler("stats", stats))
tg_app.add_handler(CommandHandler("help", help_command))

tg_app.add_handler(
    CallbackQueryHandler(
        verify_callback,
        pattern=r"^v:",
    )
)

tg_app.add_handler(
    MessageHandler(
        filters.ChatType.PRIVATE
        & (
            filters.Document.ALL
            | filters.PHOTO
            | filters.VIDEO
            | filters.AUDIO
            | filters.ANIMATION
            | filters.VOICE
            | filters.VIDEO_NOTE
        ),
        receive_file,
    )
)

tg_app.add_error_handler(error_handler)


# ==================================================
# FASTAPI WEBHOOK SERVER
# ==================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_database()

    await tg_app.initialize()
    await tg_app.start()

    await tg_app.bot.set_webhook(
        url=f"{WEBHOOK_URL}/telegram",
        secret_token=WEBHOOK_SECRET,
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=False,
    )

    logger.info("Telegram webhook configured.")
    logger.info("File Store Bot started successfully.")

    yield

    await tg_app.stop()
    await tg_app.shutdown()

    if pool:
        await pool.close()

    logger.info("Bot shutdown completed.")


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def home():
    return {
        "status": "online",
        "bot": "File Store Bot",
        "database": "connected",
    }


@app.get("/health")
async def health():
    if pool is None:
        raise HTTPException(status_code=503, detail="Database unavailable")

    try:
        await pool.fetchval("SELECT 1")
    except Exception:
        logger.exception("Health check failed")
        raise HTTPException(status_code=503, detail="Database error")

    return {"status": "healthy"}


@app.post("/telegram")
async def telegram_webhook(request: Request):
    secret = request.headers.get(
        "X-Telegram-Bot-Api-Secret-Token"
    )

    if secret != WEBHOOK_SECRET:
        raise HTTPException(
            status_code=403,
            detail="Invalid webhook secret",
        )

    try:
        data = await request.json()
        update = Update.de_json(data, tg_app.bot)
        await tg_app.update_queue.put(update)

        return {"ok": True}

    except Exception:
        logger.exception("Webhook processing error")
        raise HTTPException(
            status_code=400,
            detail="Invalid update",
        )
