"""File Store Bot: durable upload queue, safe cancellation, protected timed delivery.
Requires: fastapi, uvicorn, asyncpg, python-telegram-bot>=21,<23.
Keep the original Neon DATABASE_URL and Telegram channel IDs.
"""
import os
import re
import asyncio
import logging
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import asyncpg
from fastapi import FastAPI, Request, HTTPException
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import RetryAfter, TelegramError, TimedOut, NetworkError, BadRequest, Forbidden
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
from telegram.request import HTTPXRequest

BOT_TOKEN = os.environ['BOT_TOKEN']
DATABASE_URL = os.environ['DATABASE_URL']
STORAGE_CHANNEL_ID = int(os.environ['STORAGE_CHANNEL_ID'])
USER_STORAGE_CHANNEL_ID = int(os.environ['USER_STORAGE_CHANNEL_ID'])
WEBHOOK_URL = os.environ['WEBHOOK_URL'].rstrip('/')
WEBHOOK_SECRET = os.environ['WEBHOOK_SECRET']
ADMIN_IDS = {int(v.strip()) for v in os.getenv('ADMIN_IDS', '').split(',') if v.strip()}
FORCE_CHANNELS = [v.strip() for v in os.getenv('FORCE_CHANNELS', '').split(',') if v.strip()]
FORCE_LINKS = [v.strip() for v in os.getenv('FORCE_LINKS', '').split(',') if v.strip()]
MAX_ADMIN_FILES = int(os.getenv("MAX_ADMIN_FILES", "1000000"))
MAX_FILE_SIZE = 40 * 1024 * 1024
UPLOAD_WORKERS = max(1, min(int(os.getenv('UPLOAD_WORKERS', '1')), 4))
DELIVERY_CONCURRENCY = max(1, min(int(os.getenv('DELIVERY_CONCURRENCY', '3')), 5))
ACCESS_HOURS = 2
if len(FORCE_CHANNELS) != len(FORCE_LINKS) or len(FORCE_CHANNELS) not in (5, 6):
    raise RuntimeError('Provide matching lists of five or six FORCE_CHANNELS and FORCE_LINKS')
if len(WEBHOOK_SECRET) < 16 or not re.fullmatch(r'[A-Za-z0-9_-]{1,256}', WEBHOOK_SECRET):
    raise RuntimeError('Invalid WEBHOOK_SECRET')
if STORAGE_CHANNEL_ID == USER_STORAGE_CHANNEL_ID:
    raise RuntimeError('Admin and user storage channels must differ')

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger('file_store_bot')
pool = None
workers = []
upload_pause_until = 0.0
last_upload_at = 0.0
upload_gate = asyncio.Lock()
UPLOAD_INTERVAL = max(1.0, float(os.getenv('UPLOAD_INTERVAL', '2.0')))
subscription_cache = {}
request = HTTPXRequest(connection_pool_size=32, connect_timeout=15, read_timeout=60, write_timeout=60, pool_timeout=30)
tg_app = Application.builder().token(BOT_TOKEN).request(request).concurrent_updates(24).build()

async def db_init():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=12, ssl='require', command_timeout=60)
    async with pool.acquire() as con:
        async with con.transaction():
            await con.execute('''
                CREATE TABLE IF NOT EXISTS users (user_id BIGINT PRIMARY KEY, username TEXT, first_name TEXT, created_at TIMESTAMPTZ DEFAULT NOW());
                CREATE TABLE IF NOT EXISTS upload_sessions (id BIGSERIAL PRIMARY KEY, owner_id BIGINT NOT NULL, active BOOLEAN DEFAULT TRUE, created_at TIMESTAMPTZ DEFAULT NOW());
                CREATE TABLE IF NOT EXISTS files (id BIGSERIAL PRIMARY KEY, owner_id BIGINT NOT NULL, storage_message_id BIGINT NOT NULL, file_name TEXT, file_size BIGINT NOT NULL, created_at TIMESTAMPTZ DEFAULT NOW());
                CREATE TABLE IF NOT EXISTS session_files (session_id BIGINT REFERENCES upload_sessions(id) ON DELETE CASCADE, file_id BIGINT REFERENCES files(id) ON DELETE CASCADE, PRIMARY KEY(session_id,file_id));
                CREATE TABLE IF NOT EXISTS bundles (id BIGSERIAL PRIMARY KEY, token TEXT UNIQUE NOT NULL, owner_id BIGINT NOT NULL, expires_at TIMESTAMPTZ NOT NULL, created_at TIMESTAMPTZ DEFAULT NOW());
                CREATE TABLE IF NOT EXISTS bundle_files (bundle_id BIGINT REFERENCES bundles(id) ON DELETE CASCADE, file_id BIGINT REFERENCES files(id), PRIMARY KEY(bundle_id,file_id));
                CREATE TABLE IF NOT EXISTS delivery_messages (id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL, message_id BIGINT NOT NULL, delete_after TIMESTAMPTZ NOT NULL, created_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE(user_id,message_id));
                CREATE TABLE IF NOT EXISTS upload_failures (id BIGSERIAL PRIMARY KEY, session_id BIGINT REFERENCES upload_sessions(id) ON DELETE CASCADE, file_name TEXT NOT NULL, file_size BIGINT NOT NULL DEFAULT 0, reason TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT NOW());
                CREATE INDEX IF NOT EXISTS idx_delivery_messages_due ON delivery_messages(delete_after);
            ''')
            await con.execute('ALTER TABLE files ADD COLUMN IF NOT EXISTS storage_channel_id BIGINT')
            await con.execute('UPDATE files SET storage_channel_id=$1 WHERE storage_channel_id IS NULL', STORAGE_CHANNEL_ID)
            await con.execute('ALTER TABLE files ALTER COLUMN storage_channel_id SET NOT NULL')
            await con.execute('''CREATE TABLE IF NOT EXISTS upload_jobs (
                id BIGSERIAL PRIMARY KEY, session_id BIGINT NOT NULL REFERENCES upload_sessions(id),
                owner_id BIGINT NOT NULL, source_chat_id BIGINT NOT NULL, source_message_id BIGINT NOT NULL,
                file_name TEXT NOT NULL, file_size BIGINT NOT NULL, target_channel BIGINT NOT NULL,
                state TEXT NOT NULL DEFAULT 'pending', created_at TIMESTAMPTZ DEFAULT NOW(),
                started_at TIMESTAMPTZ, completed_at TIMESTAMPTZ,
                UNIQUE(source_chat_id, source_message_id));''')
            await con.execute('ALTER TABLE upload_sessions ADD COLUMN IF NOT EXISTS finalized BOOLEAN NOT NULL DEFAULT TRUE')
            await con.execute('ALTER TABLE upload_sessions ADD COLUMN IF NOT EXISTS finalized_at TIMESTAMPTZ')
            await con.execute('ALTER TABLE upload_sessions ADD COLUMN IF NOT EXISTS bundle_token TEXT')
            await con.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_upload_sessions_one_active ON upload_sessions(owner_id) WHERE active=TRUE')
            await con.execute('ALTER TABLE upload_jobs ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0')
            await con.execute('ALTER TABLE upload_jobs ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW()')
            await con.execute('CREATE INDEX IF NOT EXISTS idx_upload_jobs_pending ON upload_jobs(state,next_attempt_at,id)')
            # Additive story schema. Never remove or rewrite legacy bundle records.
            await con.execute("""CREATE TABLE IF NOT EXISTS managed_stories (
                id BIGSERIAL PRIMARY KEY, owner_id BIGINT NOT NULL, name TEXT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE(owner_id,name));""")
            await con.execute("""CREATE TABLE IF NOT EXISTS managed_parts (
                id BIGSERIAL PRIMARY KEY, story_id BIGINT NOT NULL REFERENCES managed_stories(id),
                label TEXT NOT NULL, bundle_id BIGINT UNIQUE REFERENCES bundles(id),
                created_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE(story_id,label));""")
            await con.execute("""CREATE TABLE IF NOT EXISTS managed_episodes (
                part_id BIGINT NOT NULL REFERENCES managed_parts(id), episode_no INTEGER NOT NULL,
                file_id BIGINT NOT NULL REFERENCES files(id),
                PRIMARY KEY(part_id,episode_no));""")
            await con.execute('ALTER TABLE upload_sessions ADD COLUMN IF NOT EXISTS managed_part_id BIGINT REFERENCES managed_parts(id)')
            await con.execute('ALTER TABLE upload_sessions ADD COLUMN IF NOT EXISTS managed_operation TEXT')
            await con.execute('ALTER TABLE upload_sessions ADD COLUMN IF NOT EXISTS replace_from INTEGER')
            await con.execute('ALTER TABLE upload_sessions ADD COLUMN IF NOT EXISTS replace_to INTEGER')
            # Recover interrupted jobs. An in-flight Telegram copy may already have succeeded;
            # do NOT retry those automatically, to avoid duplicate copies in storage.
            await con.execute("UPDATE upload_jobs SET state='uncertain', completed_at=NOW() WHERE state='copying'")
    logger.info('Database ready; existing files and bundle tokens preserved')

async def register(user):
    if user:
        await pool.execute('''INSERT INTO users(user_id,username,first_name) VALUES($1,$2,$3)
            ON CONFLICT(user_id) DO UPDATE SET username=EXCLUDED.username, first_name=EXCLUDED.first_name''',
            user.id, user.username, user.first_name)

async def safe_call(fn, *args, **kwargs):
    for attempt in range(3):
        try:
            return await fn(*args, **kwargs)
        except RetryAfter as exc:
            if attempt == 2: raise
            await asyncio.sleep(float(exc.retry_after) + 1)
        except (TimedOut, NetworkError):
            if attempt == 2: raise
            await asyncio.sleep(1.5 * (attempt + 1))

async def subscribed(user_id):
    now = asyncio.get_running_loop().time()
    cached = subscription_cache.get(user_id)
    if cached and cached[1] > now:
        return cached[0]
    async def check(channel):
        try:
            m = await safe_call(tg_app.bot.get_chat_member, channel, user_id)
            return m.status not in ('left', 'kicked') and (m.status != 'restricted' or bool(getattr(m, 'is_member', False)))
        except TelegramError:
            logger.exception('Subscription check failed: %s', channel)
            return False
    result = all(await asyncio.gather(*(check(c) for c in FORCE_CHANNELS)))
    subscription_cache[user_id] = (result, now + (60 if result else 10))
    return result

async def join_prompt(message, token=None):
    buttons = [InlineKeyboardButton(f'Join Channel {i}', url=url) for i,url in enumerate(FORCE_LINKS,1)]
    keyboard = [buttons[i:i+2] for i in range(0,len(buttons),2)]
    keyboard.append([InlineKeyboardButton('Verify Subscription', callback_data='v:' + (token or 'home'))])
    await safe_call(message.reply_text, '🔐 Subscription Required\n\nPlease join all channels, then click the Verify button and wait up to 1 minute for the bot to start.\n\n🎧 Enjoy your favorite stories!', reply_markup=InlineKeyboardMarkup(keyboard))

async def start(update, context):
    user = update.effective_user
    await register(user)
    token = context.args[0] if context.args else None
    if not await subscribed(user.id):
        await join_prompt(update.effective_message, token)
    elif token:
        await deliver(user.id, token, context)
    else:
        await safe_call(update.effective_message.reply_text,
            'File Store Bot\n/newstory NAME - Create story\n/newpart NAME | 1-50 - Create part\n/searchstory NAME - Find all parts\n/addfiles - Upload session\n/cancelupload - Cancel all pending uploads\n/done - Finish and create link\n/myfiles - All stories\n/help - Help')

async def verify(update, context):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    await register(user)
    subscription_cache.pop(user.id, None)
    if not await subscribed(user.id):
        await safe_call(query.message.reply_text, 'Please join all channels and click Verify again.')
        return
    token = query.data.split(':',1)[1]
    if token == 'home':
        await safe_call(query.message.reply_text, 'Verified! /addfiles se upload shuru karo.')
    else:
        await deliver(user.id, token, context)

async def addfiles(update, context):
    user = update.effective_user
    await register(user)
    if not await subscribed(user.id):
        await join_prompt(update.effective_message)
        return
    async with pool.acquire() as con:
        async with con.transaction():
            active = await con.fetchrow('SELECT id FROM upload_sessions WHERE owner_id=$1 AND active=TRUE ORDER BY id DESC LIMIT 1 FOR UPDATE', user.id)
            if active:
                await safe_call(update.effective_message.reply_text, 'Pehle current batch /done se finish karo, ya /cancelupload se pending files cancel karo.')
                return
            await con.execute('INSERT INTO upload_sessions(owner_id) VALUES($1)', user.id)
    await safe_call(update.effective_message.reply_text, 'Batch started. Files bhejo (max 40 MB each).\nPending uploads cancel: /cancelupload\nFinish: /done\nHar file ka alag success message nahi aayega.')

def details(message):
    for typ in ('document','video','audio','animation','voice','video_note'):
        item = getattr(message,typ,None)
        if item: return item.file_size or 0, getattr(item,'file_name',None) or typ
    if message.photo: return message.photo[-1].file_size or 0, 'photo.jpg'
    return None

async def receive(update, context):
    user, message = update.effective_user, update.effective_message
    d = details(message)
    if not d: return
    size, name = d
    # Fast enqueue: no Telegram copy or five-channel membership check per file.
    async with pool.acquire() as con:
        async with con.transaction():
            session = await con.fetchrow('SELECT id FROM upload_sessions WHERE owner_id=$1 AND active=TRUE ORDER BY id DESC LIMIT 1', user.id)
            if not session:
                await safe_call(message.reply_text, 'Pehle /addfiles bhejo.')
                return
            if size > MAX_FILE_SIZE:
                await con.execute('INSERT INTO upload_failures(session_id,file_name,file_size,reason) VALUES($1,$2,$3,$4)', session['id'],name,size,'40 MB limit exceeded')
                return
            channel = STORAGE_CHANNEL_ID if user.id in ADMIN_IDS else USER_STORAGE_CHANNEL_ID
            await con.execute('''INSERT INTO upload_jobs(session_id,owner_id,source_chat_id,source_message_id,file_name,file_size,target_channel)
                VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(source_chat_id,source_message_id) DO NOTHING''',
                session['id'],user.id,message.chat_id,message.message_id,name,size,channel)

# Story names are owner-scoped. All management operations require ownership.
def episode_number(filename):
    # Require an explicit Ep/Episode marker to prevent accidental wrong replacements.
    m = re.search(r'(?i)(?:^|[^a-z])(?:ep|episode)[\s._-]*(\d{1,7})(?!\d)', filename)
    return int(m.group(1)) if m and int(m.group(1)) > 0 else None

async def begin_managed_session(update, part_id, operation, low=None, high=None):
    uid = update.effective_user.id
    async with pool.acquire() as con:
        async with con.transaction():
            part = await con.fetchrow("""SELECT p.id,p.label,s.name FROM managed_parts p
                JOIN managed_stories s ON s.id=p.story_id WHERE p.id=$1 AND s.owner_id=$2""",part_id,uid)
            if not part:
                await update.effective_message.reply_text('Part nahi mila ya tumhara nahi hai.')
                return
            busy = await con.fetchval('''SELECT id FROM upload_sessions
                WHERE managed_part_id=$1 AND finalized=FALSE LIMIT 1''',part_id)
            if busy:
                await update.effective_message.reply_text('Is part ka pehla batch abhi processing me hai. /batchstatus check karo.')
                return
            active = await con.fetchval('SELECT id FROM upload_sessions WHERE owner_id=$1 AND active=TRUE',uid)
            if active:
                await update.effective_message.reply_text('Pehle active upload /done karo.')
                return
            await con.execute("""INSERT INTO upload_sessions(owner_id,managed_part_id,managed_operation,replace_from,replace_to)
                VALUES($1,$2,$3,$4,$5)""",uid,part_id,operation,low,high)
    await update.effective_message.reply_text(
        f'{part["name"]} / {part["label"]} (ID {part_id})\nMode: {operation}\n'
        'Ab files bhejo. Filename me Ep451, Episode 452 jaise episode number zaroor ho.\n'
        'Sab bhejne ke baad /done. Replace me poori range ke successful uploads zaroori hain.')

async def newstory(update,context):
    name=' '.join(context.args).strip()
    if not name or len(name)>120:
        await update.effective_message.reply_text('Use: /newstory Ruthless (max 120 characters)')
        return
    await register(update.effective_user)
    row=await pool.fetchrow("""INSERT INTO managed_stories(owner_id,name) VALUES($1,$2)
        ON CONFLICT(owner_id,name) DO UPDATE SET name=EXCLUDED.name RETURNING id""",update.effective_user.id,name)
    await update.effective_message.reply_text(f'Story saved: {name} (ID {row["id"]}).\nNew part: /newpart {name} | 1-50')

async def newpart(update,context):
    raw=' '.join(context.args)
    if '|' not in raw:
        await update.effective_message.reply_text('Use: /newpart Ruthless | 1-50')
        return
    name,label=(v.strip() for v in raw.split('|',1))
    if not name or not label or len(label)>100:
        await update.effective_message.reply_text('Story name aur part label sahi likho.')
        return
    uid=update.effective_user.id
    async with pool.acquire() as con:
        async with con.transaction():
            active=await con.fetchval('SELECT id FROM upload_sessions WHERE owner_id=$1 AND active=TRUE',uid)
            if active:
                await update.effective_message.reply_text('Pehle active upload /done se close karo. Empty batch bhi ab close ho jayega.')
                return
            story=await con.fetchrow('SELECT id FROM managed_stories WHERE owner_id=$1 AND name=$2',uid,name)
            if not story:
                await update.effective_message.reply_text('Pehle /newstory se story banao.')
                return
            part=await con.fetchrow("""INSERT INTO managed_parts(story_id,label) VALUES($1,$2)
                ON CONFLICT(story_id,label) DO NOTHING RETURNING id""",story['id'],label)
            if not part:
                await update.effective_message.reply_text('Ye part pehle se hai. /searchstory se uska ID dekho.')
                return
    await begin_managed_session(update,part['id'],'new')

async def addepisodes(update,context):
    try: pid=int(context.args[0])
    except (IndexError,ValueError):
        await update.effective_message.reply_text('Use: /addepisodes PART_ID')
        return
    await begin_managed_session(update,pid,'add')

async def addmissing(update,context):
    """Add one missing episode to an existing part without changing its link."""
    try:
        if len(context.args)!=2: raise ValueError()
        pid,ep=map(int,context.args)
        if pid<1 or ep<1: raise ValueError()
    except (ValueError,TypeError):
        await update.effective_message.reply_text('Use: /addmissing PART_ID EPISODE\nExample: /addmissing 16 409')
        return
    uid=update.effective_user.id
    row=await pool.fetchrow("""SELECT p.id,p.label FROM managed_parts p
        JOIN managed_stories s ON s.id=p.story_id
        WHERE p.id=$1 AND s.owner_id=$2""",pid,uid)
    if not row:
        await update.effective_message.reply_text('Part nahi mila ya access nahi hai.')
        return
    present=await pool.fetchval('SELECT 1 FROM managed_episodes WHERE part_id=$1 AND episode_no=$2',pid,ep)
    if present:
        await update.effective_message.reply_text(f'Episode {ep} pehle se part {pid} me hai. Replace ke liye /replaceepisodes {pid} {ep} {ep} use karo.')
        return
    await begin_managed_session(update,pid,'missing',ep,ep)

async def replaceepisodes(update,context):
    try: pid,low,high=map(int,context.args[:3])
    except (ValueError,TypeError):
        await update.effective_message.reply_text('Use: /replaceepisodes PART_ID 450 500')
        return
    if low<1 or high<low or high-low>9999:
        await update.effective_message.reply_text('Episode range invalid ya bahut badi hai.')
        return
    count=await pool.fetchval("""SELECT COUNT(*) FROM managed_episodes e
        JOIN managed_parts p ON p.id=e.part_id JOIN managed_stories s ON s.id=p.story_id
        WHERE e.part_id=$1 AND s.owner_id=$2 AND e.episode_no BETWEEN $3 AND $4""",pid,update.effective_user.id,low,high)
    if count!=high-low+1:
        await update.effective_message.reply_text(f'Is range me {count}/{high-low+1} episodes hain. Khali range ko replace nahi kar sakte. /addepisodes {pid} use karo, ya /recoverbatch se pehle se saved batch jodo.')
        return
    await begin_managed_session(update,pid,'replace',low,high)

async def searchstory(update,context):
    term=' '.join(context.args).strip()
    if not term:
        await update.effective_message.reply_text('Use: /searchstory Ruthless')
        return
    uid=update.effective_user.id
    stories=await pool.fetch("""SELECT id,name FROM managed_stories
        WHERE owner_id=$1 AND name ILIKE $2 ORDER BY name LIMIT 10""",uid,'%'+term+'%')
    if not stories:
        await update.effective_message.reply_text('Story nahi mili. Purane legacy batches is search me nahi dikhte.')
        return
    me=await context.bot.get_me()
    for story in stories:
        rows=await pool.fetch("""SELECT p.id,p.label,b.token,COUNT(e.episode_no) AS n,
            MIN(e.episode_no) AS lo,MAX(e.episode_no) AS hi
            FROM managed_parts p LEFT JOIN bundles b ON b.id=p.bundle_id
            LEFT JOIN managed_episodes e ON e.part_id=p.id WHERE p.story_id=$1
            GROUP BY p.id,p.label,b.token ORDER BY p.id""",story['id'])
        if not rows:
            await update.effective_message.reply_text(f'{story["name"]}: Abhi koi part nahi hai.')
            continue
        # Pagination via chunks to respect Telegram message length.
        head=f'STORY: {story["name"]} | {len(rows)} parts\n'
        chunk=head
        for r in rows:
            nums=await pool.fetch('SELECT episode_no FROM managed_episodes WHERE part_id=$1 ORDER BY episode_no',r['id'])
            numbers=[v['episode_no'] for v in nums]
            ranges=[]
            if numbers:
                a=b=numbers[0]
                for n in numbers[1:]:
                    if n==b+1: b=n
                    else:
                        ranges.append(str(a) if a==b else f'{a}-{b}'); a=b=n
                ranges.append(str(a) if a==b else f'{a}-{b}')
            ranges_text=', '.join(ranges) or 'none'
            line=f'\nPart ID {r["id"]}: {r["label"]} | {r["n"]} files\nEpisodes: {ranges_text}\n'
            line+=(f'https://t.me/{me.username}?start={r["token"]}' if r['token'] else 'Link pending / no files')+'\n'
            if len(chunk)+len(line)>3500:
                await update.effective_message.reply_text(chunk)
                chunk=f'STORY: {story["name"]} (continued)\n'
            # Large fragmented episode lists may exceed a single Telegram message.
            if len(line)>3200: line=line[:3100]+'...\n'
            chunk+=line
        await update.effective_message.reply_text(chunk)
    await update.effective_message.reply_text('Add: /addepisodes PART_ID\nReplace: /replaceepisodes PART_ID 450 500')

async def managed_finalize(con,session,saved,failures,cancelled,uncertain):
    """Apply a managed batch transactionally; never alter an old legacy bundle."""
    sid=session['id']; pid=session['managed_part_id']; op=session['managed_operation']
    part=await con.fetchrow('SELECT id,bundle_id FROM managed_parts WHERE id=$1 FOR UPDATE',pid)
    if not part: return None,'Part record missing; existing links untouched.'
    # Match each saved upload job to its stored file using name and size.
    # A join on session_id alone would multiply 100 jobs x 100 files.
    jobs=await con.fetch("""SELECT j.source_message_id,j.file_name,j.file_size
        FROM upload_jobs j WHERE j.session_id=$1 AND j.state='saved'
        ORDER BY j.source_message_id,j.id""",sid)
    stored=await con.fetch("""SELECT f.id,f.file_name,f.file_size
        FROM session_files sf JOIN files f ON f.id=sf.file_id
        WHERE sf.session_id=$1 ORDER BY f.id""",sid)
    from collections import defaultdict
    buckets=defaultdict(list)
    for f in stored:
        buckets[(f['file_name'],f['file_size'])].append(f['id'])
    if len(jobs)!=len(stored):
        return None,'Update NOT applied: saved jobs/files count mismatch. Use recovery; files untouched.'
    if any(len(ids)>1 for ids in buckets.values()):
        return None,'Update NOT applied: identical file names and sizes; mapping ambiguous. Use checked recovery.'
    parsed=[]
    for job in jobs:
        ids=buckets[(job['file_name'],job['file_size'])]
        if not ids:
            return None,'Update NOT applied: file mapping mismatch. Use checked recovery.'
        parsed.append((episode_number(job['file_name']),ids.pop()))
    nums=[n for n,_ in parsed]
    # Never guess episode numbers from filenames or silently reorder them.
    # Failed batches remain in storage and can be recovered without reuploading.
    if not nums or any(n is None for n in nums) or len(set(nums))!=len(nums):
        missing=sum(n is None for n in nums)
        repeated=len([n for n in nums if n is not None])-len({n for n in nums if n is not None})
        return None,(f'Update NOT applied: {missing} missing episode number(s), '
                     f'{repeated} duplicate(s). Saved files remain available. '
                     f'Use /recoverbatch {sid} {pid} for a checked recovery; do NOT reupload.')
    if op=='missing':
        expected=session['replace_from']
        if len(parsed)!=1 or nums!=[expected] or failures or cancelled or uncertain:
            return None,(f'Missing episode update NOT applied: exactly one file named Ep{expected} '
                         'and zero errors required. Saved file untouched.')
    if op=='replace':
        low,high=session['replace_from'],session['replace_to']
        expected=set(range(low,high+1))
        if set(nums)!=expected or failures or cancelled or uncertain:
            return None,'Replacement NOT applied: complete correct range and zero errors required.'
        old=await con.fetch('SELECT episode_no FROM managed_episodes WHERE part_id=$1 AND episode_no BETWEEN $2 AND $3',pid,low,high)
        if {r['episode_no'] for r in old}!=expected:
            return None,'Replacement NOT applied: existing episode range changed.'
    else:
        if op=='new' and part['bundle_id']:
            return None,'Part already has a link; update not applied.'
        clashes=await con.fetch('SELECT episode_no FROM managed_episodes WHERE part_id=$1 AND episode_no=ANY($2::integer[])',pid,nums)
        if clashes:
            return None,'Update NOT applied: duplicate episode numbers already in part.'
    bid=part['bundle_id']; token=None
    if bid is None:
        token=secrets.token_urlsafe(12)
        bid=await con.fetchval("INSERT INTO bundles(token,owner_id,expires_at) VALUES($1,$2,NOW()+INTERVAL '100 years') RETURNING id",token,session['owner_id'])
        await con.execute('UPDATE managed_parts SET bundle_id=$2 WHERE id=$1',pid,bid)
    else:
        token=await con.fetchval('SELECT token FROM bundles WHERE id=$1',bid)
    if op=='replace':
        await con.execute('DELETE FROM managed_episodes WHERE part_id=$1 AND episode_no BETWEEN $2 AND $3',pid,session['replace_from'],session['replace_to'])
    await con.executemany('INSERT INTO managed_episodes(part_id,episode_no,file_id) VALUES($1,$2,$3)',[(pid,n,fid) for n,fid in parsed])
    # Synchronize this managed part's bundle. Never delete original files/storage messages.
    # Legacy bundles cannot be associated with managed_parts by this code.
    await con.execute('DELETE FROM bundle_files WHERE bundle_id=$1',bid)
    await con.execute("""INSERT INTO bundle_files(bundle_id,file_id)
        SELECT $1,file_id FROM managed_episodes WHERE part_id=$2 ORDER BY episode_no""",bid,pid)
    return token,f'Updated part {pid}: {len(parsed)} episode(s) added/replaced. Same permanent link.'

async def recoverbatch(update,context):
    """Recover completed managed batch using explicit numeric episode range.
    Does not copy Telegram messages, alter old storage, or overwrite existing episodes.
    """
    uid=update.effective_user.id
    try:
        sid,pid,lo,hi=map(int,context.args[:4])
    except (ValueError,TypeError):
        await update.effective_message.reply_text(
            'Use: /recoverbatch BATCH_ID PART_ID FROM TO\n'
            'Example: /recoverbatch 149 12 101 200')
        return
    confirm=len(context.args)>4 and context.args[4].upper()=='CONFIRM'
    if lo<1 or hi<lo or hi-lo>9999:
        await update.effective_message.reply_text('Episode range invalid.')
        return
    async with pool.acquire() as con:
        async with con.transaction():
            part=await con.fetchrow("""SELECT p.id,p.label,p.bundle_id,s.name FROM managed_parts p
                JOIN managed_stories s ON s.id=p.story_id
                WHERE p.id=$1 AND s.owner_id=$2 FOR UPDATE OF p""",pid,uid)
            sess=await con.fetchrow(
                'SELECT * FROM upload_sessions WHERE id=$1 AND owner_id=$2 FOR UPDATE',sid,uid)
            if not part or not sess or not sess['finalized'] or sess['active']:
                await update.effective_message.reply_text('Part ya completed batch nahi mila.')
                return
            if sess['managed_part_id'] != pid:
                await update.effective_message.reply_text(
                    'Batch kisi doosre part ka hai. Recovery roki gayi.')
                return
            if sess['managed_operation']=='replace':
                await update.effective_message.reply_text(
                    'Replacement batch ko is command se recover nahi kar sakte.')
                return
            if sess['bundle_token']:
                await update.effective_message.reply_text(
                    'Is batch ka link pehle se hai. Duplicate recovery nahi ki.')
                return
            jobs=await con.fetch("""SELECT id,source_message_id,file_name,file_size,state
                FROM upload_jobs WHERE session_id=$1
                ORDER BY source_message_id,id""",sid)
            saved=await con.fetch("""SELECT f.id,f.file_name,f.file_size
                FROM session_files sf JOIN files f ON f.id=sf.file_id
                WHERE sf.session_id=$1 ORDER BY f.id""",sid)
            fails=await con.fetchval(
                'SELECT COUNT(*) FROM upload_failures WHERE session_id=$1',sid)
            count=hi-lo+1
            if (fails or len(jobs)!=count or len(saved)!=count or
                any(j['state']!='saved' for j in jobs)):
                await update.effective_message.reply_text(
                    f'Expected {count}; saved {len(saved)}; jobs {len(jobs)}; '
                    f'failures {fails}. Incomplete batch, no changes.')
                return
            already=await con.fetchval(
                'SELECT COUNT(*) FROM managed_episodes WHERE part_id=$1',pid)
            if already:
                await update.effective_message.reply_text(
                    'Part me pehle se episodes hain. Recovery overwrite nahi karegi.')
                return
            # Match jobs to stored files by unique (name, size), not by an
            # incorrect cross-join or assumed completion order.
            from collections import defaultdict
            buckets=defaultdict(list)
            for f in saved:
                buckets[(f['file_name'],f['file_size'])].append(f)
            mapped=[]
            ambiguous=False
            for j in jobs:
                bucket=buckets[(j['file_name'],j['file_size'])]
                if not bucket:
                    await update.effective_message.reply_text(
                        'Saved files aur upload jobs ka metadata mismatch hai. No changes.')
                    return
                if len(bucket)>1:
                    ambiguous=True
                mapped.append(bucket.pop(0))
            expected=list(range(lo,hi+1))
            detected=[episode_number(j['file_name']) for j in jobs]
            known=[n for n in detected if n is not None]
            if (len(known)!=len(set(known)) or
                any(n!=expected[i] for i,n in enumerate(detected) if n is not None)):
                await update.effective_message.reply_text(
                    'Episode numbers upload order se mismatch/duplicate hain. '
                    'Recovery roki gayi. Filenames check karo.')
                return
            if (len(known)!=count or ambiguous) and not confirm:
                await update.effective_message.reply_text(
                    f'Batch #{sid}: {count} saved; {len(known)} episode numbers detected.\n'
                    f'Part {pid}, expected {lo}-{hi}.\n'
                    'Kuch filenames unnumbered ya identical hain. '
                    'Sirf agar files sahi episode order me upload ki thi, tab bhejo:\n'
                    f'/recoverbatch {sid} {pid} {lo} {hi} CONFIRM')
                return
            bid=part['bundle_id']
            if bid is None:
                token=secrets.token_urlsafe(12)
                bid=await con.fetchval("""INSERT INTO bundles(token,owner_id,expires_at)
                    VALUES($1,$2,NOW()+INTERVAL '100 years') RETURNING id""",token,uid)
                await con.execute(
                    'UPDATE managed_parts SET bundle_id=$2 WHERE id=$1',pid,bid)
            else:
                token=await con.fetchval(
                    'SELECT token FROM bundles WHERE id=$1',bid)
            await con.executemany(
                'INSERT INTO managed_episodes(part_id,episode_no,file_id) VALUES($1,$2,$3)',
                [(pid,n,f['id']) for n,f in zip(expected,mapped)])
            await con.executemany("""INSERT INTO bundle_files(bundle_id,file_id)
                VALUES($1,$2) ON CONFLICT DO NOTHING""",
                [(bid,f['id']) for f in mapped])
            await con.execute(
                'UPDATE upload_sessions SET bundle_token=$2 WHERE id=$1',sid,token)
    me=await context.bot.get_me()
    await update.effective_message.reply_text(
        f'Recovered batch #{sid}: {count} episodes attached to part {pid}.\n'
        f'Permanent link: https://t.me/{me.username}?start={token}',
        disable_web_page_preview=True)

async def upload_worker(number):
    """Retry 429 without losing the job. Network timeouts remain uncertain, not retried."""
    global upload_pause_until, last_upload_at
    loop = asyncio.get_running_loop()
    while True:
        job = None
        try:
            wait = upload_pause_until - loop.time()
            if wait > 0:
                await asyncio.sleep(wait)
                continue
            async with pool.acquire() as con:
                async with con.transaction():
                    job = await con.fetchrow("""SELECT * FROM upload_jobs
                        WHERE state='pending' AND next_attempt_at <= NOW()
                        ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED""")
                    if job:
                        await con.execute("UPDATE upload_jobs SET state='copying',started_at=NOW() WHERE id=$1", job['id'])
            if not job:
                await asyncio.sleep(0.7)
                continue
            try:
                if job['owner_id'] in ADMIN_IDS:
                    count = await pool.fetchval('SELECT COUNT(*) FROM files WHERE owner_id = ANY($1::bigint[])', list(ADMIN_IDS))
                    if count >= MAX_ADMIN_FILES:
                        raise ValueError('Admin storage limit reached')
                # If Telegram times out, it may have copied the file. Never blindly retry timeouts.
                # Serialize storage API requests across workers, respecting a global cooldown.
                async with upload_gate:
                    wait = max(upload_pause_until - loop.time(),
                               UPLOAD_INTERVAL - (loop.time() - last_upload_at))
                    if wait > 0:
                        await asyncio.sleep(wait)
                    last_upload_at = loop.time()
                    copied = await tg_app.bot.copy_message(
                        chat_id=job['target_channel'], from_chat_id=job['source_chat_id'],
                        message_id=job['source_message_id'])
                async with pool.acquire() as con:
                    async with con.transaction():
                        fid = await con.fetchval("""INSERT INTO files(owner_id,storage_message_id,storage_channel_id,file_name,file_size)
                            VALUES($1,$2,$3,$4,$5) RETURNING id""",
                            job['owner_id'], copied.message_id, job['target_channel'], job['file_name'], job['file_size'])
                        await con.execute('INSERT INTO session_files(session_id,file_id) VALUES($1,$2)',job['session_id'],fid)
                        await con.execute("UPDATE upload_jobs SET state='saved',completed_at=NOW() WHERE id=$1",job['id'])
            except RetryAfter as exc:
                delay = min(max(float(exc.retry_after) + 3, 5), 3600)
                upload_pause_until = max(upload_pause_until, loop.time() + delay)
                await pool.execute("""UPDATE upload_jobs SET state='pending', retry_count=retry_count+1,
                    next_attempt_at=NOW()+($2::double precision * INTERVAL '1 second'),started_at=NULL
                    WHERE id=$1""",job['id'],delay)
                logger.warning('Telegram rate limit: job %s queued again in %.1f seconds',job['id'],delay)
            except (TimedOut, NetworkError) as exc:
                logger.warning('Upload %s uncertain; manually check storage: %s',job['id'],exc)
                await pool.execute("UPDATE upload_jobs SET state='uncertain',completed_at=NOW() WHERE id=$1",job['id'])
            except Exception as exc:
                logger.exception('Upload job %s failed',job['id'])
                async with pool.acquire() as con:
                    async with con.transaction():
                        await con.execute("UPDATE upload_jobs SET state='failed',completed_at=NOW() WHERE id=$1",job['id'])
                        await con.execute('INSERT INTO upload_failures(session_id,file_name,file_size,reason) VALUES($1,$2,$3,$4)',
                            job['session_id'],job['file_name'],job['file_size'],str(exc)[:160])
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception('Upload worker error; will retry')
            await asyncio.sleep(2)

async def uploadstatus(update, context):
    user = update.effective_user
    row = await pool.fetchrow('SELECT id FROM upload_sessions WHERE owner_id=$1 AND active=TRUE ORDER BY id DESC LIMIT 1',user.id)
    if not row:
        await safe_call(update.effective_message.reply_text, 'Koi active upload session nahi hai.')
        return
    states = await pool.fetch('SELECT state,COUNT(*) AS n FROM upload_jobs WHERE session_id=$1 GROUP BY state',row['id'])
    counts = {r['state']:r['n'] for r in states}
    failed_oversize = await pool.fetchval('SELECT COUNT(*) FROM upload_failures WHERE session_id=$1',row['id'])
    text = (f"UPLOAD STATUS\nSaved: {counts.get('saved',0)}\nPending: {counts.get('pending',0)}"
            f"\nCopying: {counts.get('copying',0)}\nFailed: {max(counts.get('failed',0),failed_oversize)}"
            f"\nUncertain: {counts.get('uncertain',0)}\nCancelled: {counts.get('cancelled',0)}"
            "\n\nPending 0 aur Copying 0 hone par /done bhejo.")
    await safe_call(update.effective_message.reply_text,text)

async def cancelupload(update, context):
    user = update.effective_user
    async with pool.acquire() as con:
        async with con.transaction():
            session = await con.fetchrow('SELECT id FROM upload_sessions WHERE owner_id=$1 AND active=TRUE ORDER BY id DESC LIMIT 1 FOR UPDATE',user.id)
            if not session:
                await safe_call(update.effective_message.reply_text, 'Koi active batch nahi hai.')
                return
            rows = await con.fetch("""UPDATE upload_jobs SET state='cancelled',completed_at=NOW()
                WHERE session_id=$1 AND state='pending' RETURNING id""",session['id'])
            copying = await con.fetchval("SELECT COUNT(*) FROM upload_jobs WHERE session_id=$1 AND state='copying'",session['id'])
    await safe_call(update.effective_message.reply_text,
        f'{len(rows)} pending files cancelled. {copying} file(s) copying me hain; unhe cancel karna guaranteed nahi.\nSaved files untouched. /done se batch finish karo.')

async def done(update, context):
    user = update.effective_user
    await register(user)
    if not await subscribed(user.id):
        await join_prompt(update.effective_message)
        return
    async with pool.acquire() as con:
        async with con.transaction():
            session = await con.fetchrow('SELECT id FROM upload_sessions WHERE owner_id=$1 AND active=TRUE ORDER BY id DESC LIMIT 1 FOR UPDATE', user.id)
            if not session:
                await safe_call(update.effective_message.reply_text, 'Koi open batch nahi. /addfiles se naya batch shuru karo.')
                return
            sid = session['id']
            total = await con.fetchval('SELECT COUNT(*) FROM upload_jobs WHERE session_id=$1', sid)
            errors = await con.fetchval('SELECT COUNT(*) FROM upload_failures WHERE session_id=$1', sid)
            if not total and not errors:
                # Empty batch: close it without creating a link or touching any saved files.
                await con.execute('''UPDATE upload_sessions
                    SET active=FALSE, finalized=TRUE, finalized_at=NOW()
                    WHERE id=$1''', sid)
                empty_closed = True
            else:
                empty_closed = False
            if not empty_closed:
                # Close input immediately. Background finalizer waits for ONLY this session.
                await con.execute('UPDATE upload_sessions SET active=FALSE,finalized=FALSE WHERE id=$1', sid)
    if empty_closed:
        await safe_call(update.effective_message.reply_text,
            f'Empty batch #{sid} closed. Koi file ya purani link delete nahi hui. Ab /newpart ya /addfiles use kar sakte ho.')
    else:
        await safe_call(update.effective_message.reply_text,
            f'Batch #{sid} locked ({total} received). Ab /addfiles se agla batch shuru kar sakte ho. Is batch ki alag permanent link processing poori hote hi milegi.')

async def finalize_worker():
    while True:
        try:
            sessions = await pool.fetch('''SELECT s.* FROM upload_sessions s
                WHERE s.active=FALSE AND s.finalized=FALSE
                ORDER BY s.id LIMIT 30''')
            for session in sessions:
                sid, owner = session['id'], session['owner_id']
                async with pool.acquire() as con:
                    async with con.transaction():
                        locked = await con.fetchrow('SELECT * FROM upload_sessions WHERE id=$1 AND active=FALSE AND finalized=FALSE FOR UPDATE SKIP LOCKED', sid)
                        if not locked:
                            continue
                        outstanding = await con.fetchval("SELECT COUNT(*) FROM upload_jobs WHERE session_id=$1 AND state IN ('pending','copying')",sid)
                        if outstanding:
                            continue
                        saved = await con.fetch('''SELECT f.id,f.file_size FROM session_files sf JOIN files f ON f.id=sf.file_id
                            WHERE sf.session_id=$1 ORDER BY f.id''',sid)
                        failures = await con.fetch('SELECT file_name,file_size,reason FROM upload_failures WHERE session_id=$1 ORDER BY id',sid)
                        cancelled = await con.fetchval("SELECT COUNT(*) FROM upload_jobs WHERE session_id=$1 AND state='cancelled'",sid)
                        uncertain = await con.fetchval("SELECT COUNT(*) FROM upload_jobs WHERE session_id=$1 AND state='uncertain'",sid)
                        token = None
                        managed_note = None
                        if locked['managed_part_id'] is not None:
                            token,managed_note = await managed_finalize(con,locked,saved,failures,cancelled,uncertain)
                        elif saved:
                            token = secrets.token_urlsafe(12)
                            bid = await con.fetchval("INSERT INTO bundles(token,owner_id,expires_at) VALUES($1,$2,NOW()+INTERVAL '100 years') RETURNING id",token,owner)
                            await con.executemany('INSERT INTO bundle_files(bundle_id,file_id) VALUES($1,$2)',[(bid,r['id']) for r in saved])
                        await con.execute('UPDATE upload_sessions SET finalized=TRUE,finalized_at=NOW(),bundle_token=$2 WHERE id=$1',sid,token)
                mb = lambda n: f'{n/(1024*1024):.2f} MB'
                lines = [f'BATCH #{sid} COMPLETE',f'Saved: {len(saved)}',f'Failed: {len(failures)}',
                    f'Cancelled: {cancelled}',f'Uncertain (check channel): {uncertain}',
                    f'Saved size: {mb(sum(r["file_size"] for r in saved))}',
                    f'Failed size: {mb(sum(r["file_size"] for r in failures))}']
                if managed_note: lines.append(managed_note)
                if token:
                    info = await safe_call(tg_app.bot.get_me)
                    lines.extend(['','Permanent link:',f'https://t.me/{info.username}?start={token}'])
                else:
                    lines.append('No link created (no saved files).')
                try:
                    await safe_call(tg_app.bot.send_message, owner, '\n'.join(lines))
                    if failures:
                        chunk='FAILED FILES:'
                        for i,r in enumerate(failures,1):
                            line=f'\n{i}. {r["file_name"][:90]} ({mb(r["file_size"])}) - {r["reason"][:120]}'
                            if len(chunk)+len(line)>3500:
                                await safe_call(tg_app.bot.send_message,owner,chunk)
                                chunk='FAILED FILES:'
                            chunk+=line
                        await safe_call(tg_app.bot.send_message,owner,chunk)
                except TelegramError:
                    logger.exception('Batch %s finalized, but notification failed. Token preserved in database.',sid)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception('Batch finalizer error')
        await asyncio.sleep(3)


async def partbatches(update, context):
    """Read-only list of upload batches associated with an owned managed part."""
    try:
        pid=int(context.args[0])
        page=int(context.args[1]) if len(context.args)>1 else 1
        if pid<1 or page<1 or page>100000: raise ValueError()
    except (IndexError,ValueError):
        await update.effective_message.reply_text('Use: /partbatches PART_ID [PAGE]\nExample: /partbatches 15')
        return
    uid=update.effective_user.id
    part=await pool.fetchrow("""SELECT p.id,p.label,s.name FROM managed_parts p
        JOIN managed_stories s ON s.id=p.story_id
        WHERE p.id=$1 AND s.owner_id=$2""",pid,uid)
    if not part:
        await update.effective_message.reply_text('Part nahi mila ya access nahi hai.')
        return
    total=await pool.fetchval('SELECT COUNT(*) FROM upload_sessions WHERE managed_part_id=$1 AND owner_id=$2',pid,uid)
    if not total:
        await update.effective_message.reply_text(f'Part {pid} ke liye koi batch record nahi mila.')
        return
    limit=10
    pages=(total+limit-1)//limit
    if page>pages:
        await update.effective_message.reply_text(f'Last page {pages} hai.')
        return
    rows=await pool.fetch("""SELECT us.id,us.active,us.finalized,us.managed_operation,us.bundle_token,
        (SELECT COUNT(*) FROM session_files sf WHERE sf.session_id=us.id) AS saved,
        (SELECT COUNT(*) FROM upload_jobs j WHERE j.session_id=us.id AND j.state IN ('pending','copying')) AS pending,
        (SELECT COUNT(*) FROM upload_failures f WHERE f.session_id=us.id) AS failed
        FROM upload_sessions us WHERE us.managed_part_id=$1 AND us.owner_id=$2
        ORDER BY us.id DESC LIMIT $3 OFFSET $4""",pid,uid,limit,(page-1)*limit)
    lines=[f'{part["name"]} | Part {pid} ({part["label"]})',f'Batch history {page}/{pages}:']
    for r in rows:
        status='OPEN' if r['active'] else ('COMPLETE' if r['finalized'] else 'PROCESSING')
        link='linked' if r['bundle_token'] else 'no batch link'
        lines.append(f'#{r["id"]} {r["managed_operation"] or "legacy"} | {status} | saved {r["saved"]} | pending {r["pending"]} | failed {r["failed"]} | {link}')
    if page<pages: lines.append(f'Next: /partbatches {pid} {page+1}')
    await update.effective_message.reply_text('\n'.join(lines))

async def missingepisodes(update, context):
    """Read-only missing episode numbers in a specified numeric range."""
    try:
        pid,lo,hi=map(int,context.args[:3])
        if pid<1 or lo<1 or hi<lo or hi-lo>9999: raise ValueError()
    except (IndexError,ValueError):
        await update.effective_message.reply_text('Use: /missingepisodes PART_ID FROM TO\nExample: /missingepisodes 16 401 500')
        return
    uid=update.effective_user.id
    part=await pool.fetchrow("""SELECT p.id FROM managed_parts p JOIN managed_stories s ON s.id=p.story_id
        WHERE p.id=$1 AND s.owner_id=$2""",pid,uid)
    if not part:
        await update.effective_message.reply_text('Part nahi mila ya access nahi hai.')
        return
    rows=await pool.fetch('SELECT episode_no FROM managed_episodes WHERE part_id=$1 AND episode_no BETWEEN $2 AND $3',pid,lo,hi)
    present={r['episode_no'] for r in rows}
    missing=[n for n in range(lo,hi+1) if n not in present]
    out=', '.join(map(str,missing)) or 'None'
    await update.effective_message.reply_text(f'Part {pid} | {lo}-{hi}\nSaved: {len(present)}\nMissing: {len(missing)}\nEpisode numbers: {out[:3300]}')

async def batchstatus(update, context):
    user = update.effective_user
    rows = await pool.fetch('''SELECT s.id,s.active,s.finalized,s.bundle_token,
        (SELECT COUNT(*) FROM upload_jobs j WHERE j.session_id=s.id AND j.state='saved') saved,
        (SELECT COUNT(*) FROM upload_jobs j WHERE j.session_id=s.id AND j.state IN ('pending','copying')) outstanding
        FROM upload_sessions s WHERE s.owner_id=$1 ORDER BY s.id DESC LIMIT 5''',user.id)
    if not rows:
        await safe_call(update.effective_message.reply_text,'Koi batch nahi mila.')
        return
    info = await safe_call(context.bot.get_me)
    lines = ['LAST 5 BATCHES']
    for r in rows:
        state = 'OPEN' if r['active'] else ('COMPLETE' if r['finalized'] else 'PROCESSING')
        lines.append(f"#{r['id']} {state} | saved {r['saved']} | pending {r['outstanding']}")
        if r['bundle_token']:
            lines.append(f"https://t.me/{info.username}?start={r['bundle_token']}")
    await safe_call(update.effective_message.reply_text,'\n'.join(lines))

async def alllinks(update, context):
    # Read-only: never modify existing links, files, or stories.
    uid = update.effective_user.id
    try:
        page = int(context.args[0]) if context.args else 1
    except (ValueError, TypeError):
        await safe_call(update.effective_message.reply_text, 'Use: /alllinks or /alllinks 2')
        return
    if page < 1 or page > 1000000:
        await safe_call(update.effective_message.reply_text, 'Invalid page number.')
        return
    per_page = 10
    total = await pool.fetchval('SELECT COUNT(*) FROM bundles WHERE owner_id=$1', uid)
    if not total:
        await safe_call(update.effective_message.reply_text, 'Koi saved sharing link nahi mili.')
        return
    pages = (total + per_page - 1) // per_page
    if page > pages:
        await safe_call(update.effective_message.reply_text, f'Last page {pages} hai. /alllinks {pages} bhejo.')
        return
    rows = await pool.fetch('''
        SELECT b.id, b.token, s.id AS batch_id, p.id AS part_id,
               p.label AS part_label, st.name AS story_name,
               (SELECT COUNT(*) FROM bundle_files bf WHERE bf.bundle_id=b.id) AS file_count
        FROM bundles b
        LEFT JOIN LATERAL (
            SELECT us.id FROM upload_sessions us
            WHERE us.bundle_token=b.token AND us.owner_id=b.owner_id
            ORDER BY us.id DESC LIMIT 1
        ) s ON TRUE
        LEFT JOIN managed_parts p ON p.bundle_id=b.id
        LEFT JOIN managed_stories st ON st.id=p.story_id
        WHERE b.owner_id=$1
        ORDER BY b.id DESC LIMIT $2 OFFSET $3
    ''', uid, per_page, (page - 1) * per_page)
    me = await safe_call(context.bot.get_me)
    lines = [f'ALL SAVED LINKS - Page {page}/{pages} | Total: {total}']
    for index, r in enumerate(rows, (page - 1) * per_page + 1):
        if r['story_name']:
            label = f"{r['story_name']} | {r['part_label']} (Part ID {r['part_id']})"
        elif r['batch_id'] is not None:
            label = f"Legacy Batch #{r['batch_id']}"
        else:
            label = f"Legacy Bundle #{r['id']}"
        lines.append(f"\n{index}. {label} | {r['file_count']} files\nhttps://t.me/{me.username}?start={r['token']}")
    if page < pages:
        lines.append(f'\nNext: /alllinks {page + 1}')
    await safe_call(update.effective_message.reply_text, '\n'.join(lines), disable_web_page_preview=True)


async def track(user_id, message_id):
    await pool.execute("INSERT INTO delivery_messages(user_id,message_id,delete_after) VALUES($1,$2,NOW()+INTERVAL '2 hours') ON CONFLICT DO NOTHING",user_id,message_id)

async def deletion_worker():
    while True:
        try:
            rows = await pool.fetch('SELECT id,user_id,message_id FROM delivery_messages WHERE delete_after<=NOW() ORDER BY delete_after LIMIT 100')
            for row in rows:
                try:
                    await safe_call(tg_app.bot.delete_message,chat_id=row['user_id'],message_id=row['message_id'])
                except (BadRequest,Forbidden):
                    pass
                except TelegramError:
                    logger.warning('Deletion delayed for %s',row['id'])
                    continue
                await pool.execute('DELETE FROM delivery_messages WHERE id=$1',row['id'])
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception('Deletion worker error')
        await asyncio.sleep(20)

async def deliver(user_id, token, context):
    bundle = await pool.fetchrow('SELECT id FROM bundles WHERE token=$1',token)
    if not bundle:
        await safe_call(context.bot.send_message,user_id,'Link invalid hai ya record nahi mila.')
        return
    managed_part = await pool.fetchval('SELECT id FROM managed_parts WHERE bundle_id=$1',bundle['id'])
    if managed_part:
        files = await pool.fetch('''SELECT f.storage_message_id,f.storage_channel_id FROM managed_episodes e
            JOIN files f ON f.id=e.file_id WHERE e.part_id=$1 ORDER BY e.episode_no''',managed_part)
    else:
        files = await pool.fetch('''SELECT f.storage_message_id,f.storage_channel_id FROM bundle_files bf
            JOIN files f ON f.id=bf.file_id WHERE bf.bundle_id=$1 ORDER BY f.id''',bundle['id'])
    if not files:
        await safe_call(context.bot.send_message,user_id,'Is link me files nahi hain.')
        return
    intro = await safe_call(context.bot.send_message,user_id,f'{len(files)} files bheji ja rahi hain. Files 2 hours baad chat se delete hongi.')
    await track(user_id,intro.message_id)
    semaphore=asyncio.Semaphore(DELIVERY_CONCURRENCY)
    async def one(item):
        async with semaphore:
            try:
                copied=await safe_call(context.bot.copy_message,chat_id=user_id,from_chat_id=item['storage_channel_id'],message_id=item['storage_message_id'],protect_content=True)
                await track(user_id,copied.message_id)
                return True
            except TelegramError:
                logger.exception('Delivery failed for %s',user_id)
                return False
    # Bounded batch avoids spawning tens of thousands of tasks.
    good=bad=0
    for i in range(0,len(files),20):
        results=await asyncio.gather(*(one(f) for f in files[i:i+20]))
        good+=sum(results)
        bad+=len(results)-sum(results)
    summary=await safe_call(context.bot.send_message,user_id,f'Delivery complete. Successful: {good}, failed: {bad}. Link permanent hai; dobara open kar sakte ho.')
    await track(user_id,summary.message_id)

async def myfiles(update, context):
    uid = update.effective_user.id
    if not await subscribed(uid):
        await join_prompt(update.effective_message)
        return
    rows = await pool.fetch("""SELECT s.name, COUNT(e.episode_no) AS n
        FROM managed_stories s LEFT JOIN managed_parts p ON p.story_id=s.id
        LEFT JOIN managed_episodes e ON e.part_id=p.id
        WHERE s.owner_id=$1 GROUP BY s.id,s.name ORDER BY lower(s.name),s.id""", uid)
    chunk = f'📚 MY STORIES | Total: {len(rows)}'
    for i,r in enumerate(rows,1):
        line = f'\n{i}. {r["name"]} — {r["n"]} Ep'
        if len(chunk)+len(line)>3400:
            await safe_call(update.effective_message.reply_text,chunk)
            chunk='📚 MY STORIES (continued)'
        chunk+=line
    await safe_call(update.effective_message.reply_text,chunk)

async def renamestory(update, context):
    raw=' '.join(context.args)
    if '|' not in raw:
        await update.effective_message.reply_text('Use: /renamestory OLD NAME | NEW NAME')
        return
    old,new=(v.strip() for v in raw.split('|',1))
    if not old or not new or len(new)>120:
        await update.effective_message.reply_text('Invalid story name.')
        return
    try:
        row=await pool.fetchrow('UPDATE managed_stories SET name=$3 WHERE owner_id=$1 AND name=$2 RETURNING id',update.effective_user.id,old,new)
    except asyncpg.UniqueViolationError:
        await update.effective_message.reply_text('New story name already exists. No changes.')
        return
    await update.effective_message.reply_text(f'Story renamed: {old} → {new}. Links unchanged.' if row else 'Story not found. Use exact old name from /myfiles.')

async def renamefile(update, context):
    raw=' '.join(context.args)
    if '|' not in raw:
        await update.effective_message.reply_text('Use: /renamefile FILE_ID | NEW NAME. IDs: /recentfiles')
        return
    left,new=(v.strip() for v in raw.split('|',1))
    try:
        fid=int(left)
        if fid<1 or not new or len(new)>255: raise ValueError()
    except ValueError:
        await update.effective_message.reply_text('Invalid file ID or name.')
        return
    # Do not change upload matching metadata while a batch is unfinished.
    row=await pool.fetchrow("""UPDATE files f SET file_name=$3 WHERE f.id=$1 AND f.owner_id=$2
        AND NOT EXISTS (SELECT 1 FROM session_files sf JOIN upload_sessions us ON us.id=sf.session_id
        WHERE sf.file_id=f.id AND (us.active OR NOT us.finalized OR us.bundle_token IS NULL))
        RETURNING f.id""",fid,update.effective_user.id,new)
    await update.effective_message.reply_text('Saved filename updated. Telegram audio filename and links unchanged.' if row else 'File not found, not owned, or batch not safely linked. No changes.')

async def recentfiles(update,context):
    user=update.effective_user
    if not await subscribed(user.id):
        await join_prompt(update.effective_message)
        return
    rows=await pool.fetch('SELECT id,file_name,file_size FROM files WHERE owner_id=$1 ORDER BY id DESC LIMIT 20',user.id)
    if not rows:
        await safe_call(update.effective_message.reply_text,'Abhi files nahi hain.')
        return
    text='Latest 20 files:\n'+ '\n'.join(f'{r["id"]}: {r["file_name"][:70]} ({r["file_size"]/(1024*1024):.2f} MB)' for r in rows)
    await safe_call(update.effective_message.reply_text,text[:3900])

async def stats(update,context):
    if update.effective_user.id not in ADMIN_IDS:
        await safe_call(update.effective_message.reply_text,'Not authorized.')
        return
    users=await pool.fetchval('SELECT COUNT(*) FROM users')
    admin=await pool.fetchval('SELECT COUNT(*) FROM files WHERE owner_id = ANY($1::bigint[])',list(ADMIN_IDS))
    others=await pool.fetchval('SELECT COUNT(*) FROM files WHERE NOT(owner_id = ANY($1::bigint[]))',list(ADMIN_IDS))
    bundles=await pool.fetchval('SELECT COUNT(*) FROM bundles')
    await safe_call(update.effective_message.reply_text,f'Users: {users}\nAdmin files: {admin}/{MAX_ADMIN_FILES}\nUser files: {others}\nBundles: {bundles}')

async def help_command(update,context):
    await safe_call(update.effective_message.reply_text,
        '/addmissing PART_ID EP - Add missing episode\n/newstory NAME - Create story\n/newpart NAME | 1-50 - New part and upload\n/searchstory NAME - All parts and links\n/addepisodes PART_ID - Add to part\n/replaceepisodes PART_ID FROM TO - Replace range\n/recoverbatch BATCH_ID PART_ID - Recover saved failed part\n/addfiles - Start legacy batch\n/batchstatus - Last 5 batches and links\n/alllinks - All saved links (pages)\n/uploadstatus - Upload progress\n/cancelupload - Cancel only pending files\n/done - Finalize batch\n/myfiles - All stories and episode counts\n/recentfiles - Latest file IDs\n/renamestory OLD | NEW - Rename story\n/renamefile ID | NEW - Rename saved filename\n/stats - Admin statistics\n\nOriginal saved files cannot be deleted with bot commands. Links have no time expiry; delivered copies auto-delete after 2 hours.')

async def errors(update,context):
    logger.error('Unhandled bot error',exc_info=context.error)

tg_app.add_handler(CommandHandler('start',start))
tg_app.add_handler(CommandHandler('newstory',newstory))
tg_app.add_handler(CommandHandler('newpart',newpart))
tg_app.add_handler(CommandHandler('searchstory',searchstory))
tg_app.add_handler(CommandHandler('addepisodes',addepisodes))
tg_app.add_handler(CommandHandler('addmissing',addmissing))
tg_app.add_handler(CommandHandler('replaceepisodes',replaceepisodes))
tg_app.add_handler(CommandHandler('recoverbatch',recoverbatch))
tg_app.add_handler(CommandHandler('addfiles',addfiles))
tg_app.add_handler(CommandHandler('cancelupload',cancelupload))
tg_app.add_handler(CommandHandler('uploadstatus',uploadstatus))
tg_app.add_handler(CommandHandler('done',done))
tg_app.add_handler(CommandHandler('batchstatus',batchstatus))
tg_app.add_handler(CommandHandler('partbatches',partbatches))
tg_app.add_handler(CommandHandler('missingepisodes',missingepisodes))
tg_app.add_handler(CommandHandler('alllinks',alllinks))
tg_app.add_handler(CommandHandler('myfiles',myfiles))
tg_app.add_handler(CommandHandler('recentfiles',recentfiles))
tg_app.add_handler(CommandHandler('renamestory',renamestory))
tg_app.add_handler(CommandHandler('renamefile',renamefile))
tg_app.add_handler(CommandHandler('stats',stats))
tg_app.add_handler(CommandHandler('help',help_command))
tg_app.add_handler(CallbackQueryHandler(verify,pattern=r'^v:'))
tg_app.add_handler(MessageHandler(filters.ChatType.PRIVATE & (filters.Document.ALL | filters.PHOTO | filters.VIDEO | filters.AUDIO | filters.ANIMATION | filters.VOICE | filters.VIDEO_NOTE),receive))
tg_app.add_error_handler(errors)

@asynccontextmanager
async def lifespan(app):
    await db_init()
    try:
        await tg_app.initialize()
        await tg_app.start()
        await tg_app.bot.set_webhook(url=f'{WEBHOOK_URL}/telegram',secret_token=WEBHOOK_SECRET,allowed_updates=Update.ALL_TYPES,drop_pending_updates=False)
        workers.extend(asyncio.create_task(upload_worker(i)) for i in range(UPLOAD_WORKERS))
        workers.append(asyncio.create_task(deletion_worker()))
        workers.append(asyncio.create_task(finalize_worker()))
        logger.info('Started with %s upload workers',UPLOAD_WORKERS)
        yield
    finally:
        for task in workers: task.cancel()
        await asyncio.gather(*workers,return_exceptions=True)
        if tg_app.running: await tg_app.stop()
        await tg_app.shutdown()
        if pool: await pool.close()

app=FastAPI(lifespan=lifespan)

@app.get('/')
async def home(): return {'status':'online','bot':'File Store Bot Advanced'}

@app.get('/health')
async def health():
    if pool is None: raise HTTPException(503,'Database unavailable')
    try: await pool.fetchval('SELECT 1')
    except Exception: raise HTTPException(503,'Database error')
    return {'status':'healthy'}

@app.post('/telegram')
async def telegram_webhook(request:Request):
    if request.headers.get('X-Telegram-Bot-Api-Secret-Token') != WEBHOOK_SECRET:
        raise HTTPException(403,'Invalid webhook secret')
    data=await request.json()
    update=Update.de_json(data,tg_app.bot)
    await tg_app.update_queue.put(update)
    return {'ok':True}
