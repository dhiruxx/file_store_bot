import os
import html
import secrets
import logging
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, Request, HTTPException
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
import uvicorn


# ---------------- CONFIGURATION ----------------

BOT_TOKEN = os.environ["BOT_TOKEN"]
STORAGE_CHANNEL_ID = int(os.environ["STORAGE_CHANNEL_ID"])
DATABASE_URL = os.environ["DATABASE_URL"]

WEBHOOK_BASE_URL = (
    os.getenv("WEBHOOK_BASE_URL")
    or os.getenv("RENDER_EXTERNAL_URL")
)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET") or secrets.token_urlsafe(32)

MAX_FILE_MB = int(os.getenv("MAX_FILE_MB", "49"))

if not WEBHOOK_BASE_URL:
    raise RuntimeError("WEBHOOK_BASE_URL is missing")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

db_pool = None
tg_app = None


# ---------------- DATABASE ----------------

def clean_database_url(url):
    parts = urlsplit(url)

    # asyncpg ko SSL alag se denge
    query = [
        (key, value)
        for key, value in parse_qsl(parts.query)
        if key.lower() not in ("sslmode", "channel_binding")
    ]

    return urlunsplit((
        parts.scheme,
        parts.netloc,
        parts.path,
        urlencode(query),
        parts.fragment,
    ))


async def setup_database():
    global db_pool

    db_pool = await asyncpg.create_pool(
        dsn=clean_database_url(DATABASE_URL),
        ssl="require",
        min_size=1,
        max_size=5,
    )

    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                storage_message_id BIGINT NOT NULL,
                file_name TEXT NOT NULL,
                media_type TEXT NOT NULL,
                file_size BIGINT,
                share_token TEXT UNIQUE,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS files_user_id_idx
            ON files(user_id)
        """)


# ---------------- HELPERS ----------------

def get_file_details(message):
    media_types = [
        ("document", message.document),
        ("video", message.video),
        ("audio", message.audio),
        ("voice", message.voice),
        ("animation", message.animation),
        ("video_note", message.video_note),
    ]

    for media_type, media in media_types:
        if media:
            name = getattr(media, "file_name", None)
            if not name:
                name = f"{media_type}_{message.message_id}"

            return (
                media_type,
                name,
                getattr(media, "file_size", None),
            )

    if message.photo:
        photo = message.photo[-1]
        return (
            "photo",
            f"photo_{message.message_id}.jpg",
            photo.file_size,
        )

    return None


def private_chat(update):
    return (
        update.effective_chat
        and update.effective_chat.type == "private"
    )


# ---------------- START / HELP ----------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    user = update.effective_user

    if not private_chat(update):
        return

    if context.args:
        token = context.args[0]

        async with db_pool.acquire() as conn:
            record = await conn.fetchrow("""
                SELECT storage_message_id
                FROM files
                WHERE share_token = $1
            """, token)

        if not record:
            await message.reply_text(
                "Ye sharing link valid nahi hai ya band kar diya gaya hai."
            )
            return

        try:
            await context.bot.copy_message(
                chat_id=message.chat_id,
                from_chat_id=STORAGE_CHANNEL_ID,
                message_id=record["storage_message_id"],
            )
        except Exception:
            logger.exception("Shared file delivery failed")
            await message.reply_text(
                "File abhi deliver nahi ho pa rahi."
            )
        return

    await message.reply_text(
        f"Namaste {user.first_name}!\n\n"
        "Welcome to File Store Bot.\n\n"
        "Apni file mujhe bhejo. Main use private storage mein save karunga.\n\n"
        "Commands:\n"
        "/myfiles - Apni files dekho\n"
        "/share ID - File ka sharing link banao\n"
        "/unshare ID - Sharing link band karo\n"
        "/delete ID - Apni file delete karo\n"
        "/help - Help"
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not private_chat(update):
        return

    await message.reply_text(
        "File Store Bot Help\n\n"
        "File upload: Koi document, photo, video ya audio bhejo.\n"
        "/myfiles - Apni files dekho\n"
        "/myfiles 2 - Agla page dekho\n"
        "/share 12 - File ID 12 ka link banao\n"
        "/unshare 12 - Link band karo\n"
        "/delete 12 - File delete karo\n\n"
        "Sirf apni file ID par commands chalao."
    )


# ---------------- FILE UPLOAD ----------------

async def save_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    user = update.effective_user

    if not private_chat(update):
        return

    details = get_file_details(message)

    if not details:
        await message.reply_text(
            "Mujhe document, photo, video ya audio file bhejo."
        )
        return

    media_type, file_name, file_size = details

    if file_size and file_size > MAX_FILE_MB * 1024 * 1024:
        await message.reply_text(
            f"File bahut badi hai. Is bot ki configured limit "
            f"{MAX_FILE_MB} MB hai."
        )
        return

    status = await message.reply_text("File save ho rahi hai...")

    try:
        copied = await context.bot.copy_message(
            chat_id=STORAGE_CHANNEL_ID,
            from_chat_id=message.chat_id,
            message_id=message.message_id,
        )

        async with db_pool.acquire() as conn:
            record_id = await conn.fetchval("""
                INSERT INTO files (
                    user_id,
                    storage_message_id,
                    file_name,
                    media_type,
                    file_size
                )
                VALUES ($1, $2, $3, $4, $5)
                RETURNING id
            """,
                user.id,
                copied.message_id,
                file_name,
                media_type,
                file_size,
            )

        await status.edit_text(
            "File successfully save ho gayi!\n\n"
            f"File ID: {record_id}\n"
            f"Name: {html.escape(file_name)}\n\n"
            f"Share karne ke liye:\n/share {record_id}"
        )

    except Exception:
        logger.exception("File upload failed")

        try:
            if "copied" in locals():
                await context.bot.delete_message(
                    chat_id=STORAGE_CHANNEL_ID,
                    message_id=copied.message_id,
                )
        except Exception:
            logger.exception("Storage cleanup failed")

        await status.edit_text(
            "File save nahi ho payi. Thodi der baad dobara try karo."
        )


# ---------------- MY FILES ----------------

async def myfiles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message

    if not private_chat(update):
        return

    page = 1

    if context.args:
        try:
            page = max(1, int(context.args[0]))
        except ValueError:
            await message.reply_text("Sahi page number likho.")
            return

    limit = 10
    offset = (page - 1) * limit

    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, file_name, share_token, created_at
            FROM files
            WHERE user_id = $1
            ORDER BY id DESC
            LIMIT $2 OFFSET $3
        """, user.id, limit, offset)

    if not rows:
        await message.reply_text("Is page par koi file nahi mili.")
        return

    lines = [f"Your Files - Page {page}\n"]

    for row in rows:
        name = html.escape(row["file_name"][:55])
        shared = "Shared" if row["share_token"] else "Private"

        lines.append(
            f"ID: {row['id']}\n"
            f"Name: {name}\n"
            f"Status: {shared}\n"
        )

    lines.append(
        f"\nAgla page: /myfiles {page + 1}"
    )

    await message.reply_text("\n".join(lines))


# ---------------- SHARE FILE ----------------

async def share_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message

    if not private_chat(update):
        return

    if not context.args:
        await message.reply_text("Use: /share FILE_ID")
        return

    try:
        file_id = int(context.args[0])
    except ValueError:
        await message.reply_text("File ID number hona chahiye.")
        return

    token = secrets.token_urlsafe(24)

    async with db_pool.acquire() as conn:
        record = await conn.fetchrow("""
            UPDATE files
            SET share_token = $1
            WHERE id = $2 AND user_id = $3
            RETURNING id
        """, token, file_id, user.id)

    if not record:
        await message.reply_text("Ye tumhari file nahi mili.")
        return

    bot_info = await context.bot.get_me()
    link = f"https://t.me/{bot_info.username}?start={token}"

    await message.reply_text(
        "Sharing link taiyar hai:\n\n"
        f"{link}\n\n"
        "Jiske paas ye link hoga, woh is file ko access kar sakta hai."
    )


# ---------------- UNSHARE FILE ----------------

async def unshare_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message

    if not private_chat(update):
        return

    if not context.args:
        await message.reply_text("Use: /unshare FILE_ID")
        return

    try:
        file_id = int(context.args[0])
    except ValueError:
        await message.reply_text("File ID number hona chahiye.")
        return

    async with db_pool.acquire() as conn:
        result = await conn.execute("""
            UPDATE files
            SET share_token = NULL
            WHERE id = $1 AND user_id = $2
        """, file_id, user.id)

    if result == "UPDATE 0":
        await message.reply_text("Ye tumhari file nahi mili.")
        return

    await message.reply_text("Sharing link band kar diya gaya hai.")


# ---------------- DELETE FILE ----------------

async def delete_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message

    if not private_chat(update):
        return

    if not context.args:
        await message.reply_text("Use: /delete FILE_ID")
        return

    try:
        file_id = int(context.args[0])
    except ValueError:
        await message.reply_text("File ID number hona chahiye.")
        return

    async with db_pool.acquire() as conn:
        record = await conn.fetchrow("""
            SELECT storage_message_id
            FROM files
            WHERE id = $1 AND user_id = $2
        """, file_id, user.id)

    if not record:
        await message.reply_text("Ye tumhari file nahi mili.")
        return

    try:
        await context.bot.delete_message(
            chat_id=STORAGE_CHANNEL_ID,
            message_id=record["storage_message_id"],
        )

        async with db_pool.acquire() as conn:
            await conn.execute("""
                DELETE FROM files
                WHERE id = $1 AND user_id = $2
            """, file_id, user.id)

        await message.reply_text("File delete ho gayi.")

    except Exception:
        logger.exception("Delete failed")
        await message.reply_text(
            "File delete nahi ho payi. Storage channel permissions check karo."
        )


# ---------------- ERROR HANDLER ----------------

async def error_handler(update, context):
    logger.error("Bot error: %s", context.error)


# ---------------- BOT SETUP ----------------

def build_bot():
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("myfiles", myfiles))
    application.add_handler(CommandHandler("share", share_file))
    application.add_handler(CommandHandler("unshare", unshare_file))
    application.add_handler(CommandHandler("delete", delete_file))

    application.add_handler(
        MessageHandler(filters.ALL & ~filters.COMMAND, save_file)
    )

    application.add_error_handler(error_handler)

    return application


# ---------------- WEB SERVER ----------------

@asynccontextmanager
async def lifespan(api):
    global tg_app

    await setup_database()

    tg_app = build_bot()

    await tg_app.initialize()
    await tg_app.start()

    webhook_url = (
        WEBHOOK_BASE_URL.rstrip("/") + "/telegram"
    )

    await tg_app.bot.set_webhook(
        url=webhook_url,
        secret_token=WEBHOOK_SECRET,
        allowed_updates=Update.ALL_TYPES,
    )

    logger.info("Bot started and webhook configured")

    yield

    await tg_app.stop()
    await tg_app.shutdown()

    if db_pool:
        await db_pool.close()


api = FastAPI(lifespan=lifespan)


@api.get("/")
async def health():
    return {"status": "File Store Bot is running"}


@api.post("/telegram")
async def telegram_webhook(request: Request):
    secret = request.headers.get(
        "X-Telegram-Bot-Api-Secret-Token"
    )

    if secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")

    try:
        data = await request.json()
        update = Update.de_json(data, tg_app.bot)
        await tg_app.process_update(update)

        return {"ok": True}

    except Exception:
        logger.exception("Webhook processing failed")
        raise HTTPException(status_code=500, detail="Processing failed")


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))

    uvicorn.run(
        api,
        host="0.0.0.0",
        port=port,
        log_level="info",
        )
    
