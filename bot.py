
import asyncio
import logging
import os
import re
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import asyncpg
from fastapi import FastAPI, Request, HTTPException
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatMemberStatus
from telegram.error import TelegramError, RetryAfter
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# =====================================
# LOGGING
# =====================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

log = logging.getLogger("file-store-bot")


# =====================================
# ENVIRONMENT SETTINGS
# =====================================

BOT_TOKEN = os.environ["BOT_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]

STORAGE_CHANNEL_ID = int(os.environ["STORAGE_CHANNEL_ID"])

ADMIN_IDS = {
    int(x.strip())
    for x in os.environ.get("ADMIN_IDS", "").split(",")
    if x.strip()
}

# Channel IDs: comma, semicolon or space separated
FORCE_CHANNELS = [
    int(x.strip())
    for x in re.split(
        r"[,;\s]+",
        os.environ.get("FORCE_CHANNELS", "").strip()
    )
    if x.strip()
]

# Channel links: comma, semicolon or space separated
FORCE_LINKS = [
    x.strip()
    for x in re.split(
        r"[,;\s]+",
        os.environ.get("FORCE_LINKS", "").strip()
    )
    if x.strip()
]

MAX_FILES = 1000
MAX_FILE_MB = 40
MAX_FILE_BYTES = MAX_FILE_MB * 1024 * 1024

LINK_EXPIRY_HOURS = 2

WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "").rstrip("/")
PORT = int(os.environ.get("PORT", "10000"))

log.info("FORCE_CHANNELS count: %s", len(FORCE_CHANNELS))
log.info("FORCE_LINKS count: %s", len(FORCE_LINKS))

if len(FORCE_CHANNELS) != 5 or len(FORCE_LINKS) != 5:
    raise RuntimeError(
        f"Channel IDs mile: {len(FORCE_CHANNELS)}, "
        f"channel links mile: {len(FORCE_LINKS)}. "
        "Render Environment check karo."
    )

if not ADMIN_IDS:
    raise RuntimeError("ADMIN_IDS mein kam se kam ek admin ID zaroori hai.")

if not WEBHOOK_URL:
    raise RuntimeError("WEBHOOK_URL Render Environment mein missing hai.")


# =====================================
# GLOBAL VARIABLES
# =====================================

db_pool = None
telegram_app = None
BOT_USERNAME = ""

admin_upload_sessions = set()
upload_file_ids = {}

worker_task = None


# =====================================
# DATABASE
# =====================================

async def init_db():
    global db_pool

    db_pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=5,
        command_timeout=60,
        ssl="require",
    )

    async with db_pool.acquire() as conn:

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_seen TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id BIGSERIAL PRIMARY KEY,
                storage_message_id BIGINT NOT NULL,
                file_name TEXT NOT NULL,
                file_size BIGINT NOT NULL,
                media_type TEXT NOT NULL,
                added_by BIGINT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bundles (
                token TEXT PRIMARY KEY,
                owner_id BIGINT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                activated_at TIMESTAMPTZ,
                expires_at TIMESTAMPTZ
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bundle_files (
                bundle_token TEXT NOT NULL
                    REFERENCES bundles(token) ON DELETE CASCADE,
                file_id BIGINT NOT NULL
                    REFERENCES files(id),
                position INTEGER NOT NULL,
                PRIMARY KEY (bundle_token, file_id),
                UNIQUE (bundle_token, position)
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS pending_access (
                user_id BIGINT PRIMARY KEY,
                bundle_token TEXT NOT NULL
                    REFERENCES bundles(token) ON DELETE CASCADE,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS delivery_jobs (
                id BIGSERIAL PRIMARY KEY,
                bundle_token TEXT NOT NULL
                    REFERENCES bundles(token) ON DELETE CASCADE,
                user_id BIGINT NOT NULL,
                next_index INTEGER DEFAULT 0,
                status TEXT DEFAULT 'queued',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_delivery_jobs_status
            ON delivery_jobs(status, created_at)
        """)

    log.info("Database ready")


async def close_db():
    global db_pool

    if db_pool:
        await db_pool.close()
        db_pool = None


# =====================================
# ADMIN CHECK
# =====================================

def is_admin(user_id):
    return user_id in ADMIN_IDS


# =====================================
# SAVE USER
# =====================================

async def save_user(user):

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO users(user_id, username, first_name)
            VALUES($1, $2, $3)
            ON CONFLICT(user_id)
            DO UPDATE SET
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                last_seen = NOW()
            """,
            user.id,
            user.username,
            user.first_name,
        )


# =====================================
# FORCE SUBSCRIPTION
# =====================================

async def subscribed_to_all(user_id, context):

    missing = []

    for channel_id, invite_link in zip(
        FORCE_CHANNELS,
        FORCE_LINKS
    ):

        try:
            member = await context.bot.get_chat_member(
                chat_id=channel_id,
                user_id=user_id,
            )

            if (
                member.status in (
                    ChatMemberStatus.LEFT,
                    ChatMemberStatus.BANNED,
                )
                or (
                    member.status == ChatMemberStatus.RESTRICTED
                    and not member.is_member
                )
            ):
                missing.append(invite_link)

        except TelegramError:
            log.exception(
                "Membership check failed for channel %s",
                channel_id,
            )
            missing.append(invite_link)

    return missing


def subscription_keyboard(missing):

    buttons = []

    for index, link in enumerate(missing, start=1):
        buttons.append([
            InlineKeyboardButton(
                f"Join Channel {index}",
                url=link,
            )
        ])

    buttons.append([
        InlineKeyboardButton(
            "I Joined - Verify",
            callback_data="verify",
        )
    ])

    return InlineKeyboardMarkup(buttons)


async def show_subscription_prompt(chat_id, missing, context):

    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            "Files lene se pehle hamare 5 channels join karein.\n\n"
            "Sabhi channels join karne ke baad "
            "'I Joined - Verify' button dabayein."
        ),
        reply_markup=subscription_keyboard(missing),
    )


# =====================================
# BUNDLE ACTIVATION
# =====================================

async def activate_bundle(token):

    async with db_pool.acquire() as conn:

        row = await conn.fetchrow(
            """
            UPDATE bundles
            SET
                activated_at = COALESCE(activated_at, NOW()),
                expires_at = COALESCE(
                    expires_at,
                    NOW() + ($2 * INTERVAL '1 hour')
                )
            WHERE token = $1
            RETURNING activated_at, expires_at
            """,
            token,
            LINK_EXPIRY_HOURS,
        )

    return row


# =====================================
# QUEUE DELIVERY
# =====================================

async def queue_delivery(token, user_id, chat_id, context):

    async with db_pool.acquire() as conn:

        bundle = await conn.fetchrow(
            """
            SELECT expires_at
            FROM bundles
            WHERE token = $1
            """,
            token,
        )

        if not bundle:
            await context.bot.send_message(
                chat_id,
                "Ye link valid nahi hai.",
            )
            return

        if (
            bundle["expires_at"] is None
            or bundle["expires_at"] <= datetime.now(timezone.utc)
        ):
            await context.bot.send_message(
                chat_id,
                "Is link ki validity khatam ho gayi hai.",
            )
            return

        count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM bundle_files
            WHERE bundle_token = $1
            """,
            token,
        )

        if not count:
            await context.bot.send_message(
                chat_id,
                "Is link mein koi file nahi hai.",
            )
            return

        job_id = await conn.fetchval(
            """
            INSERT INTO delivery_jobs(
                bundle_token,
                user_id,
                status
            )
            VALUES($1, $2, 'queued')
            RETURNING id
            """,
            token,
            user_id,
        )

    await context.bot.send_message(
        chat_id,
        f"Subscription verified!\n"
        f"Total files: {count}\n"
        f"Delivery queue mein laga di gayi hai.\n"
        f"Job ID: {job_id}\n\n"
        "Files batches mein aayengi.",
    )


# =====================================
# START COMMAND
# =====================================

async def start(update, context):

    user = update.effective_user

    await save_user(user)

    if not context.args:

        await update.effective_message.reply_text(
            "Welcome to File Store Bot!\n\n"
            "Agar aapke paas file share link hai, "
            "use open karein.\n\n"
            "Admin commands:\n"
            "/addfiles - Upload files\n"
            "/done - Create share link\n"
            "/cancel - Cancel upload\n"
            "/stats - Storage statistics"
        )

        return

    arg = context.args[0]

    if not arg.startswith("get_"):

        await update.effective_message.reply_text(
            "Invalid link. Sahi share link open karein."
        )

        return

    token = arg[4:]

    async with db_pool.acquire() as conn:

        bundle = await conn.fetchrow(
            "SELECT token FROM bundles WHERE token = $1",
            token,
        )

    if not bundle:

        await update.effective_message.reply_text(
            "Ye link valid nahi hai ya delete ho chuka hai."
        )

        return

    await activate_bundle(token)

    async with db_pool.acquire() as conn:

        status = await conn.fetchrow(
            """
            SELECT expires_at
            FROM bundles
            WHERE token = $1
            """,
            token,
        )

    if (
        not status
        or status["expires_at"] <= datetime.now(timezone.utc)
    ):

        await update.effective_message.reply_text(
            "Is link ki validity khatam ho gayi hai."
        )

        return

    missing = await subscribed_to_all(user.id, context)

    if missing:

        await show_subscription_prompt(
            update.effective_chat.id,
            missing,
            context,
        )

        async with db_pool.acquire() as conn:

            await conn.execute(
                """
                INSERT INTO pending_access(
                    user_id,
                    bundle_token,
                    updated_at
                )
                VALUES($1, $2, NOW())
                ON CONFLICT(user_id)
                DO UPDATE SET
                    bundle_token = EXCLUDED.bundle_token,
                    updated_at = NOW()
                """,
                user.id,
                token,
            )

        return

    await queue_delivery(
        token,
        user.id,
        update.effective_chat.id,
        context,
    )


# =====================================
# VERIFY SUBSCRIPTION BUTTON
# =====================================

async def verify_callback(update, context):

    query = update.callback_query

    await query.answer()

    user_id = query.from_user.id

    missing = await subscribed_to_all(
        user_id,
        context,
    )

    if missing:

        await query.message.reply_text(
            "Abhi kuch channels join nahi hue hain.\n"
            "Sabhi 5 channels join karke phir Verify dabayein.",
            reply_markup=subscription_keyboard(missing),
        )

        return

    async with db_pool.acquire() as conn:

        row = await conn.fetchrow(
            """
            SELECT bundle_token
            FROM pending_access
            WHERE user_id = $1
            """,
            user_id,
        )

        await conn.execute(
            "DELETE FROM pending_access WHERE user_id = $1",
            user_id,
        )

    if not row:

        await query.message.reply_text(
            "Pending link nahi mila. Share link dobara open karein."
        )

        return

    await queue_delivery(
        row["bundle_token"],
        user_id,
        query.message.chat_id,
        context,
    )


# =====================================
# ADD FILES COMMAND
# =====================================

async def addfiles(update, context):

    user_id = update.effective_user.id

    if not is_admin(user_id):

        await update.effective_message.reply_text(
            "Access denied."
        )

        return

    admin_upload_sessions.add(user_id)

    upload_file_ids.setdefault(user_id, [])

    await update.effective_message.reply_text(
        "File upload mode ON!\n\n"
        "Ab ek-ek karke ya multiple files bhejein.\n"
        "Sab files bhejne ke baad /done likhein.\n"
        "Cancel karne ke liye /cancel likhein."
    )


# =====================================
# CANCEL COMMAND
# =====================================

async def cancel_upload(update, context):

    user_id = update.effective_user.id

    if not is_admin(user_id):
        return

    admin_upload_sessions.discard(user_id)
    upload_file_ids.pop(user_id, None)

    await update.effective_message.reply_text(
        "Upload session cancel kar diya gaya."
    )


# =====================================
# RECEIVE FILES
# =====================================

async def receive_file(update, context):

    user = update.effective_user

    if not user or user.id not in admin_upload_sessions:
        return

    msg = update.effective_message

    media = None
    media_type = ""
    file_name = ""
    file_size = 0

    if msg.document:

        media = msg.document
        media_type = "document"
        file_name = (
            msg.document.file_name
            or f"document_{msg.message_id}"
        )
        file_size = msg.document.file_size or 0

    elif msg.video:

        media = msg.video
        media_type = "video"
        file_name = (
            msg.video.file_name
            or f"video_{msg.message_id}.mp4"
        )
        file_size = msg.video.file_size or 0

    elif msg.audio:

        media = msg.audio
        media_type = "audio"
        file_name = (
            msg.audio.file_name
            or f"audio_{msg.message_id}"
        )
        file_size = msg.audio.file_size or 0

    elif msg.animation:

        media = msg.animation
        media_type = "animation"
        file_name = (
            msg.animation.file_name
            or f"animation_{msg.message_id}.mp4"
        )
        file_size = msg.animation.file_size or 0

    elif msg.photo:

        media = msg.photo[-1]
        media_type = "photo"
        file_name = f"photo_{msg.message_id}.jpg"
        file_size = media.file_size or 0

    else:
        return

    if file_size > MAX_FILE_BYTES:

        await msg.reply_text(
            f"File {MAX_FILE_MB} MB se badi hai. Reject kar di gayi."
        )

        return

    async with db_pool.acquire() as conn:

        current = await conn.fetchval(
            "SELECT COUNT(*) FROM files"
        )

        if current >= MAX_FILES:

            await msg.reply_text(
                f"Storage limit {MAX_FILES} files poori ho chuki hai."
            )

            return

    try:

        copied = await context.bot.copy_message(
            chat_id=STORAGE_CHANNEL_ID,
            from_chat_id=msg.chat_id,
            message_id=msg.message_id,
        )

    except TelegramError:

        log.exception("File storage copy failed")

        await msg.reply_text(
            "Storage channel mein file save nahi hui. "
            "Bot ki admin permissions check karein."
        )

        return

    async with db_pool.acquire() as conn:

        file_id = await conn.fetchval(
            """
            INSERT INTO files(
                storage_message_id,
                file_name,
                file_size,
                media_type,
                added_by
            )
            VALUES($1, $2, $3, $4, $5)
            RETURNING id
            """,
            copied.message_id,
            file_name,
            file_size,
            media_type,
            user.id,
        )

    upload_file_ids.setdefault(user.id, []).append(file_id)

    total_uploaded = len(upload_file_ids[user.id])

    await msg.reply_text(
        f"File saved successfully!\n\n"
        f"Name: {file_name}\n"
        f"File ID: {file_id}\n"
        f"Size: {file_size / (1024 * 1024):.2f} MB\n"
        f"Current session files: {total_uploaded}\n\n"
        "Agli file bhejein ya sab hone par /done likhein."
    )


# =====================================
# DONE COMMAND
# =====================================

async def done_upload(update, context):

    user_id = update.effective_user.id

    if not is_admin(user_id):

        await update.effective_message.reply_text(
            "Access denied."
        )

        return

    if user_id not in admin_upload_sessions:

        await update.effective_message.reply_text(
            "Pehle /addfiles likhein."
        )

        return

    session_ids = upload_file_ids.get(user_id, [])

    if not session_ids:

        admin_upload_sessions.discard(user_id)

        await update.effective_message.reply_text(
            "Is session mein koi file upload nahi hui."
        )

        return

    token = secrets.token_hex(16)

    async with db_pool.acquire() as conn:

        async with conn.transaction():

            await conn.execute(
                """
                INSERT INTO bundles(token, owner_id)
                VALUES($1, $2)
                """,
                token,
                user_id,
            )

            for position, file_id in enumerate(session_ids):

                await conn.execute(
                    """
                    INSERT INTO bundle_files(
                        bundle_token,
                        file_id,
                        position
                    )
                    VALUES($1, $2, $3)
                    """,
                    token,
                    file_id,
                    position,
                )

    total_files = len(session_ids)

    admin_upload_sessions.discard(user_id)
    upload_file_ids.pop(user_id, None)

    link = (
        f"https://t.me/{BOT_USERNAME}"
        f"?start=get_{token}"
    )

    await update.effective_message.reply_text(
        f"Bundle ready!\n\n"
        f"Total files: {total_files}\n\n"
        f"Share link:\n{link}\n\n"
        f"Link pehli baar open hone par "
        f"{LINK_EXPIRY_HOURS} ghante ke liye activate hoga.",
        disable_web_page_preview=True,
    )


# =====================================
# STATS COMMAND
# =====================================

async def stats(update, context):

    if not is_admin(update.effective_user.id):
        return

    async with db_pool.acquire() as conn:

        file_count = await conn.fetchval(
            "SELECT COUNT(*) FROM files"
        )

        bundle_count = await conn.fetchval(
            "SELECT COUNT(*) FROM bundles"
        )

        user_count = await conn.fetchval(
            "SELECT COUNT(*) FROM users"
        )

        total_size = await conn.fetchval(
            "SELECT COALESCE(SUM(file_size), 0) FROM files"
        )

    await update.effective_message.reply_text(
        f"ADMIN STATISTICS\n\n"
        f"Files: {file_count}/{MAX_FILES}\n"
        f"Bundles: {bundle_count}\n"
        f"Users: {user_count}\n"
        f"Stored size: {total_size / (1024 ** 3):.2f} GB"
    )


# =====================================
# DELIVERY WORKER
# =====================================

async def worker_loop():

    log.info("Delivery worker started")

    while True:

        try:

            async with db_pool.acquire() as conn:

                job = await conn.fetchrow(
                    """
                    SELECT id, bundle_token, user_id, next_index
                    FROM delivery_jobs
                    WHERE status IN ('queued', 'running')
                    ORDER BY created_at
                    LIMIT 1
                    """
                )

            if not job:

                await asyncio.sleep(2)
                continue

            async with db_pool.acquire() as conn:

                bundle = await conn.fetchrow(
                    """
                    SELECT expires_at
                    FROM bundles
                    WHERE token = $1
                    """,
                    job["bundle_token"],
                )

                if (
                    not bundle
                    or bundle["expires_at"] is None
                    or bundle["expires_at"] <= datetime.now(timezone.utc)
                ):

                    await conn.execute(
                        """
                        UPDATE delivery_jobs
                        SET status = 'expired', updated_at = NOW()
                        WHERE id = $1
                        """,
                        job["id"],
                    )

                    continue

                item = await conn.fetchrow(
                    """
                    SELECT
                        f.storage_message_id,
                        f.file_name
                    FROM bundle_files bf
                    JOIN files f ON f.id = bf.file_id
                    WHERE bf.bundle_token = $1
                    ORDER BY bf.position
                    OFFSET $2
                    LIMIT 1
                    """,
                    job["bundle_token"],
                    job["next_index"],
                )

            if not item:

                await db_pool.execute(
                    """
                    UPDATE delivery_jobs
                    SET status = 'completed', updated_at = NOW()
                    WHERE id = $1
                    """,
                    job["id"],
                )

                try:

                    await telegram_app.bot.send_message(
                        chat_id=job["user_id"],
                        text="Saari files deliver ho gayi hain!",
                    )

                except TelegramError:
                    log.exception("Completion message failed")

                continue

            try:

                await telegram_app.bot.copy_message(
                    chat_id=job["user_id"],
                    from_chat_id=STORAGE_CHANNEL_ID,
                    message_id=item["storage_message_id"],
                )

                await db_pool.execute(
                    """
                    UPDATE delivery_jobs
                    SET
                        next_index = next_index + 1,
                        status = 'running',
                        updated_at = NOW()
                    WHERE id = $1
                    """,
                    job["id"],
                )

                await asyncio.sleep(0.08)

            except RetryAfter as error:

                delay = error.retry_after

                if hasattr(delay, "total_seconds"):
                    delay = delay.total_seconds()

                await asyncio.sleep(float(delay) + 1)

            except TelegramError:

                log.exception(
                    "Delivery failed for job %s",
                    job["id"],
                )

                await db_pool.execute(
                    """
                    UPDATE delivery_jobs
                    SET status = 'failed', updated_at = NOW()
                    WHERE id = $1
                    """,
                    job["id"],
                )

                try:

                    await telegram_app.bot.send_message(
                        chat_id=job["user_id"],
                        text=(
                            "File delivery mein error aaya. "
                            f"Admin ko Job ID {job['id']} batayein."
                        ),
                    )

                except TelegramError:
                    pass

        except asyncio.CancelledError:
            raise

        except Exception:

            log.exception("Delivery worker error")

            await asyncio.sleep(5)


# =====================================
# FASTAPI LIFESPAN
# =====================================

@asynccontextmanager
async def lifespan(app: FastAPI):

    global telegram_app
    global BOT_USERNAME
    global worker_task

    await init_db()

    telegram_app = (
        Application.builder()
        .token(BOT_TOKEN)
        .build()
    )

    telegram_app.add_handler(
        CommandHandler("start", start)
    )

    telegram_app.add_handler(
        CommandHandler("addfiles", addfiles)
    )

    telegram_app.add_handler(
        CommandHandler("done", done_upload)
    )

    telegram_app.add_handler(
        CommandHandler("cancel", cancel_upload)
    )

    telegram_app.add_handler(
        CommandHandler("stats", stats)
    )

    telegram_app.add_handler(
        CallbackQueryHandler(
            verify_callback,
            pattern="^verify$",
        )
    )

    telegram_app.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE & (
                filters.Document.ALL
                | filters.VIDEO
                | filters.AUDIO
                | filters.PHOTO
                | filters.ANIMATION
            ),
            receive_file,
        )
    )

    await telegram_app.initialize()
    await telegram_app.start()

    bot_info = await telegram_app.bot.get_me()
    BOT_USERNAME = bot_info.username

    await telegram_app.bot.set_webhook(
        url=f"{WEBHOOK_URL}/telegram",
        secret_token=WEBHOOK_SECRET or None,
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=False,
    )

    worker_task = asyncio.create_task(worker_loop())

    log.info("Bot started as @%s", BOT_USERNAME)

    try:

        yield

    finally:

        if worker_task:

            worker_task.cancel()

            try:
                await worker_task
            except asyncio.CancelledError:
                pass

        if WEBHOOK_URL:

            await telegram_app.bot.delete_webhook()

        await telegram_app.stop()
        await telegram_app.shutdown()

        await close_db()

        log.info("Bot stopped")


# =====================================
# FASTAPI APP
# =====================================

app = FastAPI(lifespan=lifespan)


@app.get("/")
async def home():

    return {
        "status": "File Store Bot is running",
        "files_limit": MAX_FILES,
        "file_size_limit_mb": MAX_FILE_MB,
    }


@app.post("/telegram")
async def telegram_webhook(request: Request):

    if WEBHOOK_SECRET:

        supplied = request.headers.get(
            "X-Telegram-Bot-Api-Secret-Token",
            "",
        )

        if supplied != WEBHOOK_SECRET:

            raise HTTPException(
                status_code=403,
                detail="Invalid webhook secret",
            )

    data = await request.json()

    update = Update.de_json(
        data,
        telegram_app.bot,
    )

    await telegram_app.update_queue.put(update)

    return {"ok": True}
