import asyncio
import logging
import os
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import asyncpg
from fastapi import FastAPI, Request, HTTPException
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatMemberStatus
from telegram.error import TelegramError, RetryAfter
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler,
    ContextTypes, filters
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
log = logging.getLogger("file-store-bot")

BOT_TOKEN = os.environ["BOT_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]
STORAGE_CHANNEL_ID = int(os.environ["STORAGE_CHANNEL_ID"])
ADMIN_IDS = {int(x.strip()) for x in os.environ["ADMIN_IDS"].split(",") if x.strip()}
MAX_FILES = int(os.getenv("MAX_FILES", "5000"))
MAX_FILE_MB = int(os.getenv("MAX_FILE_MB", "50"))
LINK_EXPIRY_HOURS = int(os.getenv("LINK_EXPIRY_HOURS", "2"))
WEBHOOK_SECRET = os.environ["WEBHOOK_SECRET"]
WEBHOOK_URL = os.environ["WEBHOOK_URL"].rstrip("/")
PORT = int(os.getenv("PORT", "10000"))

# Format: -100111..., -100222... ; links in same order, comma-separated.
FORCE_CHANNELS = [
    int(x.strip()) for x in os.getenv("FORCE_CHANNELS", "").split(",") if x.strip()
]
FORCE_LINKS = [
    x.strip() for x in os.getenv("FORCE_LINKS", "").split(",") if x.strip()
]
if len(FORCE_CHANNELS) != 5 or len(FORCE_LINKS) != 5:
    raise RuntimeError("FORCE_CHANNELS aur FORCE_LINKS mein exactly 5 values honi chahiye.")

db_pool: asyncpg.Pool | None = None
tg_app: Application | None = None
BOT_USERNAME = ""
admin_upload_sessions: set[int] = set()
worker_task: asyncio.Task | None = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS files (
    id BIGSERIAL PRIMARY KEY,
    storage_message_id BIGINT NOT NULL,
    file_name TEXT NOT NULL,
    file_size BIGINT NOT NULL,
    media_type TEXT NOT NULL,
    added_by BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS bundles (
    token TEXT PRIMARY KEY,
    owner_id BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    activated_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS bundle_files (
    bundle_token TEXT NOT NULL REFERENCES bundles(token) ON DELETE CASCADE,
    file_id BIGINT NOT NULL REFERENCES files(id),
    position INTEGER NOT NULL,
    PRIMARY KEY(bundle_token, file_id),
    UNIQUE(bundle_token, position)
);
CREATE TABLE IF NOT EXISTS delivery_jobs (
    id BIGSERIAL PRIMARY KEY,
    bundle_token TEXT NOT NULL REFERENCES bundles(token) ON DELETE CASCADE,
    user_id BIGINT NOT NULL,
    next_index INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'queued',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_delivery_jobs_status ON delivery_jobs(status, created_at);
CREATE INDEX IF NOT EXISTS idx_bundle_files_token_position ON bundle_files(bundle_token, position);
"""

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

async def save_user(user):
    async with db_pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO users(user_id, username, first_name, last_seen)
               VALUES($1,$2,$3,NOW())
               ON CONFLICT(user_id) DO UPDATE SET
               username=EXCLUDED.username, first_name=EXCLUDED.first_name,
               last_seen=NOW()""",
            user.id, user.username, user.first_name
        )

async def subscribed_to_all(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    missing = []
    for channel_id, invite in zip(FORCE_CHANNELS, FORCE_LINKS):
        try:
            member = await context.bot.get_chat_member(channel_id, user_id)
            if member.status in (ChatMemberStatus.LEFT, ChatMemberStatus.BANNED):
                missing.append(invite)
        except TelegramError:
            log.exception("Membership check failed for channel %s", channel_id)
            # Fail closed: if bot cannot verify, do not give files.
            missing.append(invite)
    return missing

def subscription_keyboard(missing):
    buttons = []
    for i, url in enumerate(missing, start=1):
        buttons.append([InlineKeyboardButton(f"Join channel {i}", url=url)])
    buttons.append([InlineKeyboardButton("I Joined - Verify", callback_data="verify")])
    return InlineKeyboardMarkup(buttons)

async def show_subscription_prompt(chat_id: int, missing, context):
    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            "Files lene se pehle hamare 5 channels join karein. "
            "Sabhi channels join karne ke baad 'I Joined - Verify' dabayein."
        ),
        reply_markup=subscription_keyboard(missing)
    )

async def activate_bundle(token: str):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """UPDATE bundles
               SET activated_at = COALESCE(activated_at, NOW()),
                   expires_at = COALESCE(expires_at, NOW() + ($2 * INTERVAL '1 hour'))
               WHERE token=$1
               RETURNING activated_at, expires_at""",
            token, LINK_EXPIRY_HOURS
        )
        return row

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await save_user(user)
    if not context.args:
        await update.effective_message.reply_text(
            "Welcome to File Store Bot.\n\n"
            "Agar aapke paas file link hai, use open karein.\n"
            "Admin commands: /addfiles, /done, /cancel, /stats"
        )
        return

    arg = context.args[0]
    if not arg.startswith("get_"):
        await update.effective_message.reply_text("Invalid link. Sahi share link open karein.")
        return

    token = arg[4:]
    async with db_pool.acquire() as conn:
        bundle = await conn.fetchrow("SELECT token FROM bundles WHERE token=$1", token)
    if not bundle:
        await update.effective_message.reply_text("Ye link valid nahi hai ya delete ho chuka hai.")
        return

    await activate_bundle(token)
    async with db_pool.acquire() as conn:
        status = await conn.fetchrow(
            "SELECT expires_at FROM bundles WHERE token=$1", token
        )
    if status["expires_at"] <= datetime.now(timezone.utc):
        await update.effective_message.reply_text("Is link ki 2-hour validity khatam ho gayi hai.")
        return

    missing = await subscribed_to_all(user.id, context)
    if missing:
        await show_subscription_prompt(update.effective_chat.id, missing, context)
        # Store token with the user's chat in DB so Verify can resume the correct link.
        async with db_pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO pending_access(user_id, bundle_token, updated_at)
                   VALUES($1,$2,NOW())
                   ON CONFLICT(user_id) DO UPDATE SET
                   bundle_token=EXCLUDED.bundle_token, updated_at=NOW()""",
                user.id, token
            )
        return

    await queue_delivery(token, user.id, update.effective_chat.id, context)

async def verify_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    missing = await subscribed_to_all(user_id, context)
    if missing:
        await query.message.reply_text(
            "Abhi kuch channels join nahi hue. Sabhi 5 join karke phir Verify dabayein.",
            reply_markup=subscription_keyboard(missing)
        )
        return

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT bundle_token FROM pending_access WHERE user_id=$1", user_id
        )
        await conn.execute("DELETE FROM pending_access WHERE user_id=$1", user_id)
    if not row:
        await query.message.reply_text("Link dobara open karein; pending link nahi mila.")
        return
    await queue_delivery(row["bundle_token"], user_id, query.message.chat_id, context)

async def queue_delivery(token: str, user_id: int, chat_id: int, context):
    async with db_pool.acquire() as conn:
        bundle = await conn.fetchrow(
            "SELECT expires_at FROM bundles WHERE token=$1", token
        )
        if not bundle or bundle["expires_at"] <= datetime.now(timezone.utc):
            await context.bot.send_message(chat_id, "Link expire ho chuka hai.")
            return
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM bundle_files WHERE bundle_token=$1", token
        )
        if not count:
            await context.bot.send_message(chat_id, "Is link mein koi file nahi hai.")
            return
        job_id = await conn.fetchval(
            """INSERT INTO delivery_jobs(bundle_token,user_id,status)
               VALUES($1,$2,'queued') RETURNING id""",
            token, user_id
        )
    await context.bot.send_message(
        chat_id,
        f"Subscription verified. {count} files ki delivery queue mein laga di hai.\n"
        f"Job ID: {job_id}\nFiles batches mein aayengi."
    )

async def addfiles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return await update.effective_message.reply_text("Access denied.")
    admin_upload_sessions.add(user_id)
    await update.effective_message.reply_text(
        "File upload mode ON.\nAb ek-ek karke ya multiple files bhejein. "
        "Jab sab bhej dein, /done likhein. Radd karne ke liye /cancel."
    )

async def cancel_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    admin_upload_sessions.discard(user_id)
    await update.effective_message.reply_text("Upload session cancel kar diya.")

async def receive_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or user.id not in admin_upload_sessions:
        return
    msg = update.effective_message

    media = None
    media_type = ""
    file_name = ""
    file_size = 0

    if msg.document:
        media, media_type = msg.document, "document"
        file_name = msg.document.file_name or f"document_{msg.message_id}"
        file_size = msg.document.file_size or 0
    elif msg.video:
        media, media_type = msg.video, "video"
        file_name = msg.video.file_name or f"video_{msg.message_id}.mp4"
        file_size = msg.video.file_size or 0
    elif msg.audio:
        media, media_type = msg.audio, "audio"
        file_name = msg.audio.file_name or f"audio_{msg.message_id}"
        file_size = msg.audio.file_size or 0
    elif msg.animation:
        media, media_type = msg.animation, "animation"
        file_name = msg.animation.file_name or f"animation_{msg.message_id}.mp4"
        file_size = msg.animation.file_size or 0
    elif msg.photo:
        media, media_type = msg.photo[-1], "photo"
        file_name = f"photo_{msg.message_id}.jpg"
        file_size = media.file_size or 0
    else:
        return

    max_bytes = MAX_FILE_MB * 1024 * 1024
    if file_size > max_bytes:
        return await msg.reply_text(f"File {MAX_FILE_MB} MB se badi hai. Reject kar di gayi.")
    async with db_pool.acquire() as conn:
        current = await conn.fetchval("SELECT COUNT(*) FROM files")
        if current >= MAX_FILES:
            return await msg.reply_text(f"Storage limit {MAX_FILES} files poori ho chuki hai.")

    try:
        copied = await context.bot.copy_message(
            chat_id=STORAGE_CHANNEL_ID,
            from_chat_id=msg.chat_id,
            message_id=msg.message_id
        )
    except TelegramError:
        log.exception("Could not copy incoming file to storage channel")
        return await msg.reply_text("Storage channel mein file save nahi hui. Admin permission check karein.")

    async with db_pool.acquire() as conn:
        file_id = await conn.fetchval(
            """INSERT INTO files(storage_message_id,file_name,file_size,media_type,added_by)
               VALUES($1,$2,$3,$4,$5) RETURNING id""",
            copied.message_id, file_name, file_size, media_type, user.id
        )
    upload_file_ids.setdefault(user.id, []).append(file_id)
    await msg.reply_text(
        f"Saved: {file_name}\nFile ID: {file_id}\n"
        f"Size: {file_size / (1024*1024):.2f} MB\n"
        "Agli file bhejein ya sab hone par /done likhein."
    )

async def done_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return await update.effective_message.reply_text("Access denied.")
    if user_id not in admin_upload_sessions:
        return await update.effective_message.reply_text("Pehle /addfiles likhein.")
    admin_upload_sessions.discard(user_id)

    session_ids = upload_file_ids.pop(user_id, [])
    if not session_ids:
        return await update.effective_message.reply_text("Is session mein koi file upload nahi hui.")

    token = secrets.token_hex(16)
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO bundles(token,owner_id) VALUES($1,$2)", token, user_id
            )
            for pos, fid in enumerate(session_ids):
                await conn.execute(
                    "INSERT INTO bundle_files(bundle_token,file_id,position) VALUES($1,$2,$3)",
                    token, fid, pos
                )
    link = f"https://t.me/{BOT_USERNAME}?start=get_{token}"
    await update.effective_message.reply_text(
        f"Bundle ready!\nTotal files: {len(session_ids)}\n\n"
        f"Share link:\n{link}\n\n"
        "Link pehli baar open hone par 2 ghante ke liye activate hoga.",
        disable_web_page_preview=True
    )

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    async with db_pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM files")
        bundles = await conn.fetchval("SELECT COUNT(*) FROM bundles")
        users = await conn.fetchval("SELECT COUNT(*) FROM users")
        size = await conn.fetchval("SELECT COALESCE(SUM(file_size),0) FROM files")
    await update.effective_message.reply_text(
        f"Admin Stats\nFiles: {count}/{MAX_FILES}\nBundles: {bundles}\n"
        f"Users: {users}\nStored file size total: {size/(1024**3):.2f} GB"
    )

upload_file_ids: dict[int, list[int]] = {}

async def worker_loop():
    """Persistent delivery queue: progress is saved after each copied message."""
    while True:
        try:
            async with db_pool.acquire() as conn:
                job = await conn.fetchrow(
                    """SELECT id,bundle_token,user_id,next_index
                       FROM delivery_jobs
                       WHERE status IN ('queued','running')
                       ORDER BY created_at LIMIT 1"""
                )
            if not job:
                await asyncio.sleep(2)
                continue

            async with db_pool.acquire() as conn:
                bundle = await conn.fetchrow(
                    "SELECT expires_at FROM bundles WHERE token=$1", job["bundle_token"]
                )
                if not bundle or bundle["expires_at"] <= datetime.now(timezone.utc):
                    await conn.execute(
                        "UPDATE delivery_jobs SET status='expired',updated_at=NOW() WHERE id=$1",
                        job["id"]
                    )
                    continue
                item = await conn.fetchrow(
                    """SELECT f.storage_message_id, f.file_name
                       FROM bundle_files bf JOIN files f ON f.id=bf.file_id
                       WHERE bf.bundle_token=$1
                       ORDER BY bf.position OFFSET $2 LIMIT 1""",
                    job["bundle_token"], job["next_index"]
                )
                total = await conn.fetchval(
                    "SELECT COUNT(*) FROM bundle_files WHERE bundle_token=$1",
                    job["bundle_token"]
                )

            if not item:
                await db_pool.execute(
                    "UPDATE delivery_jobs SET status='completed',updated_at=NOW() WHERE id=$1",
                    job["id"]
                )
                try:
                    await tg_app.bot.send_message(job["user_id"], "Saari files deliver ho gayi hain.")
                except TelegramError:
                    pass
                continue

            try:
                await tg_app.bot.copy_message(
                    chat_id=job["user_id"],
                    from_chat_id=STORAGE_CHANNEL_ID,
                    message_id=item["storage_message_id"]
                )
                await db_pool.execute(
                    """UPDATE delivery_jobs SET next_index=next_index+1,
                       status='running',updated_at=NOW() WHERE id=$1""",
                    job["id"]
                )
                await asyncio.sleep(0.06)  # gentle pacing; Telegram may still rate-limit.
            except RetryAfter as e:
                await asyncio.sleep(float(e.retry_after) + 1)
            except TelegramError:
                log.exception("Delivery failed for job %s", job["id"])
                await db_pool.execute(
                    "UPDATE delivery_jobs SET status='failed',updated_at=NOW() WHERE id=$1",
                    job["id"]
                )
                try:
                    await tg_app.bot.send_message(
                        job["user_id"],
                        "File delivery mein error aaya. Admin ko Job ID "
                        f"{job['id']} batayein."
                    )
                except TelegramError:
                    pass
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Delivery worker error")
            await asyncio.sleep(5)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, tg_app, BOT_USERNAME, worker_task
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with db_pool.acquire() as conn:
        await conn.execute(SCHEMA)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS pending_access(
                user_id BIGINT PRIMARY KEY,
                bundle_token TEXT NOT NULL REFERENCES bundles(token) ON DELETE CASCADE,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
    tg_app = Application.builder().token(BOT_TOKEN).build()
    tg_app.add_handler(CommandHandler("start", start))
    tg_app.add_handler(CommandHandler("addfiles", addfiles))
    tg_app.add_handler(CommandHandler("done", done_upload))
    tg_app.add_handler(CommandHandler("cancel", cancel_upload))
    tg_app.add_handler(CommandHandler("stats", stats))
    tg_app.add_handler(CallbackQueryHandler(verify_callback, pattern="^verify$"))
    tg_app.add_handler(MessageHandler(
        filters.Document.ALL | filters.VIDEO | filters.AUDIO |
        filters.PHOTO | filters.ANIMATION,
        receive_file
    ))
    await tg_app.initialize()
    await tg_app.start()
    me = await tg_app.bot.get_me()
    BOT_USERNAME = me.username
    await tg_app.bot.set_webhook(
        url=f"{WEBHOOK_URL}/telegram/{WEBHOOK_SECRET}",
        secret_token=WEBHOOK_SECRET,
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=False
    )
    worker_task = asyncio.create_task(worker_loop())
    log.info("Bot started as @%s", BOT_USERNAME)
    yield
    if worker_task:
        worker_task.cancel()
        try:
        await worker_task
        except asyncio.CancelledError:
            pass
    await tg_app.bot.delete_webhook()
    await tg_app.stop()
    await tg_app.shutdown()
    await db_pool.close()

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def health():
    return {"ok": True, "service": "file-store-bot"}

@app.post("/telegram/{secret}")
async def telegram_webhook(secret: str, request: Request):
    if secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")
    header_secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
    if header_secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Invalid Telegram secret")
    data = await request.json()
    update = Update.de_json(data, tg_app.bot)
    await tg_app.update_queue.put(update)
    return {"ok": True}
    
