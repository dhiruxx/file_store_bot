import os, re, logging, secrets, html, time
from difflib import SequenceMatcher
from contextlib import asynccontextmanager
from urllib.parse import urlparse
import asyncpg
from fastapi import FastAPI, Request, HTTPException
from telegram import (Update, InlineQueryResultArticle, InputTextMessageContent,
                      InlineKeyboardButton, InlineKeyboardMarkup)
from telegram.ext import (Application, CommandHandler, MessageHandler,
                          InlineQueryHandler, CallbackQueryHandler, ContextTypes, filters)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('storybot')
TOKEN = os.environ['BOT_TOKEN']
DB_URL = os.environ['DATABASE_URL']
ADMIN_ID = int(os.environ['ADMIN_ID'])
ADMIN_USERNAME = os.getenv('ADMIN_USERNAME', 'mr_dhiru_xx').lstrip('@')
BASE_URL = os.environ['RENDER_EXTERNAL_URL'].rstrip('/')
WEBHOOK_SECRET = os.environ['WEBHOOK_SECRET']
MAX_STORIES_PER_USER = 1000
pool = None
missing_reply_times = {}
botapp = Application.builder().token(TOKEN).updater(None).build()


def norm(s):
    return ' '.join(s.casefold().split())


def is_super(u):
    return bool(u and u.id == ADMIN_ID)


def contact_button():
    return InlineKeyboardMarkup([[InlineKeyboardButton('Message Admin', url=f'https://t.me/{ADMIN_USERNAME}')]])


def guide():
    return ('📚 DJ Story Search Bot\n\n'
            '1. Bot ki private chat me photo + caption + link ek message me bhejein.\n'
            '2. Us message par Reply karke /addstory Kalapani bhejein.\n'
            '3. /mystories se apni stories dekhein.\n'
            '4. Apne Telegram group me bot add karein. Group administrator /claimgroup bheje.\n'
            '5. Ab group me story ka naam likhne par us owner ki story aayegi.\n'
            '6. /search Kalapani se bhi search karein.\n\n'
            'Inline: @BotUsername Kalapani se apni collection search hoti hai.\n'
            'Missing story par request save hogi; owner story add karega toh group me alert aayega.\n'
            f'Support: @{ADMIN_USERNAME}\n'
            'Privacy details: /privacy. Bot administrator saved content review kar sakta hai. '
            'Sirf apna ya share karne ki permission wala content save karein.')


def story_text(s):
    return f"📚 {s['title']}\n🏷 {s['category']}\n📝 {s['description']}\n🔗 {s['url']}"


def missing_text(title):
    return (f'❌ Story Not Found!\n📚 {title}\n\n'
            '⏳ Story abhi save nahi hai. Owner ke add karne par group me update milega.\n'
            f'⚡ Jaldi chahiye? @{ADMIN_USERNAME}')


async def register_user(user):
    if not user:
        return
    async with pool.acquire() as c:
        await c.execute('''INSERT INTO bot_users(user_id,username,first_name)
          VALUES($1,$2,$3) ON CONFLICT(user_id) DO UPDATE SET
          username=EXCLUDED.username, first_name=EXCLUDED.first_name''',
          user.id, user.username, user.first_name)


async def owner_for_chat(chat):
    if chat.type == 'private':
        return chat.id
    async with pool.acquire() as c:
        return await c.fetchval('SELECT owner_id FROM group_owners WHERE chat_id=$1', chat.id)


# Words that are usually instructions, not part of a story title.
SEARCH_FILLER = set(('bhai mujhe please plz pls story stories ki ka ke ko se me mein mai '
    'chahiye chaiye chahie chhahiye hona honi hai he h de do dedo bhejo bhej '
    'link full complete all episode episodes ep part parts tak end start from to '
    '1 ek sare saare pura puri poori available download milegi milega').split())


def search_words(value):
    return re.findall(r'[^\W_]+', value.casefold(), flags=re.UNICODE)


def story_score(title, query):
    title_words = search_words(title)
    query_words = search_words(query)
    if not title_words or not query_words:
        return 0
    t = ' '.join(title_words)
    q = ' '.join(query_words)
    if q == t:
        return 1000
    if len(q) >= 3 and t in q:
        return 950
    if len(q) >= 3 and q in t:
        return 900

    useful = [w for w in query_words if w not in SEARCH_FILLER and len(w) >= 2]
    if not useful:
        return 0
    # Match words, including partial words and small typing errors.
    scores = []
    for word in useful:
        best = 0
        for target in title_words:
            if word == target:
                best = max(best, 1.0)
            elif len(word) >= 3 and (target.startswith(word) or word.startswith(target)):
                best = max(best, 0.90)
            elif len(word) >= 4 and len(target) >= 4:
                ratio = SequenceMatcher(None, word, target).ratio()
                if ratio >= 0.78:
                    best = max(best, ratio * 0.85)
        scores.append(best)
    matched = [v for v in scores if v >= 0.65]
    if not matched:
        return 0
    # A single distinctive word can identify a title, but not generic words.
    if len(useful) == 1:
        if len(useful[0]) < 4:
            return 0
        return int(600 * scores[0]) if scores[0] >= 0.78 else 0
    coverage = len(matched) / len(useful)
    if coverage < 0.5:
        return 0
    return int(500 * coverage + 150 * sum(matched) / len(matched))


async def lookup(owner_id, query):
    if not query.strip():
        return []
    async with pool.acquire() as c:
        rows = await c.fetch('''SELECT * FROM stories_v2
            WHERE owner_id=$1 AND deleted_at IS NULL''', owner_id)
    ranked = [(story_score(row['title'], query), row) for row in rows]
    ranked = [(score, row) for score, row in ranked if score >= 390]
    ranked.sort(key=lambda item: (-item[0], len(item[1]['title']), item[1]['title']))
    return [row for score, row in ranked[:8]]


def suggestion_markup(matches):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📚 ' + s['title'][:55], callback_data=f"storypick:{s['id']}")]
        for s in matches[:8]
    ])


async def deliver_matches(bot, chat_id, matches, reply_to=None):
    # An unambiguous match opens immediately; otherwise show choices.
    if len(matches) == 1:
        await send_story(bot, chat_id, matches[0], reply_to)
    else:
        await bot.send_message(chat_id=chat_id,
            text='🔎 Ye stories mili hain. Apni story select karein:',
            reply_to_message_id=reply_to,
            reply_markup=suggestion_markup(matches))


async def pick_story(update: Update, context: ContextTypes.DEFAULT_TYPE):
    callback = update.callback_query
    if not callback or not callback.data:
        return
    chat = callback.message.chat
    owner_id = await owner_for_chat(chat)
    if owner_id is None:
        await callback.answer('Group owner set nahi hai.', show_alert=True)
        return
    try:
        story_id = int(callback.data.split(':', 1)[1])
    except (IndexError, ValueError):
        return
    async with pool.acquire() as c:
        story = await c.fetchrow('''SELECT * FROM stories_v2 WHERE id=$1
            AND owner_id=$2 AND deleted_at IS NULL''', story_id, owner_id)
    if not story:
        await callback.answer('Story ab available nahi hai.', show_alert=True)
        return
    await callback.answer()
    await send_story(context.bot, chat.id, story)


async def send_story(bot, chat_id, s, reply_to=None):
    kwargs = {'chat_id': chat_id}
    if reply_to:
        kwargs['reply_to_message_id'] = reply_to
    # Telegram file_id is stored for photos; send as an actual photo with caption.
    if s['photo_id']:
        caption = s['caption'] or story_text(s)
        if len(caption) > 1024:
            caption = caption[:1021] + '...'
        await bot.send_photo(photo=s['photo_id'], caption=caption, **kwargs)
    elif s['source_chat_id'] and s['source_message_id']:
        try:
            await bot.copy_message(from_chat_id=s['source_chat_id'],
                                   message_id=s['source_message_id'], **kwargs)
        except Exception:
            await bot.send_message(text=s['caption'] or story_text(s), **kwargs)
    else:
        await bot.send_message(text=s['caption'] or story_text(s), **kwargs)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await register_user(update.effective_user)
    await update.effective_message.reply_text(guide(), reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton('Add Bot to Group', url=f'https://t.me/{context.bot.username}?startgroup=setup')],
        [InlineKeyboardButton('Contact Admin', url=f'https://t.me/{ADMIN_USERNAME}')]]))


async def privacy_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        'Privacy: Story titles, captions, media identifiers, links, Telegram user IDs, '
        'group IDs and search requests are stored in the bot database. '
        'Bot administrator can inspect saved story content for management. '
        'Only save content you have permission to share. '
        'Use /deletestory Title to request deletion; /canceldelete to cancel. '
        'Database backups may retain copies until their retention period ends.')


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(guide())


async def claim_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat, user = update.effective_chat, update.effective_user
    if chat.type not in ('group', 'supergroup'):
        await update.effective_message.reply_text('Ye command group me bhejein.')
        return
    member = await context.bot.get_chat_member(chat.id, user.id)
    if member.status not in ('administrator', 'creator'):
        await update.effective_message.reply_text('Sirf group administrator owner verify kar sakta hai.')
        return
    await register_user(user)
    async with pool.acquire() as c:
        async with c.transaction():
            existing = await c.fetchval('SELECT owner_id FROM group_owners WHERE chat_id=$1 FOR UPDATE', chat.id)
            if existing is not None and existing != user.id and not is_super(user):
                await update.effective_message.reply_text('Group ka owner pehle se set hai. Change ke liye Super Admin se baat karein.')
                return
            await c.execute('''INSERT INTO group_owners(chat_id,owner_id,group_title)
                VALUES($1,$2,$3) ON CONFLICT(chat_id) DO UPDATE SET
                owner_id=EXCLUDED.owner_id, group_title=EXCLUDED.group_title''',
                chat.id, user.id, chat.title or '')
    await update.effective_message.reply_text(f'✅ Group owner verified: {user.first_name} ({user.id}). Ab isi owner ki stories search hongi.')


async def set_owner(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_super(update.effective_user) or update.effective_chat.type not in ('group','supergroup'):
        return
    if len(context.args) != 1 or not context.args[0].isdigit():
        await update.message.reply_text('Use in group: /setowner 123456789 (user must /start bot first)')
        return
    owner_id = int(context.args[0])
    async with pool.acquire() as c:
        exists = await c.fetchval('SELECT 1 FROM bot_users WHERE user_id=$1', owner_id)
        if not exists:
            await update.message.reply_text('User pehle bot ko /start bheje.')
            return
        await c.execute('''INSERT INTO group_owners(chat_id,owner_id,group_title) VALUES($1,$2,$3)
           ON CONFLICT(chat_id) DO UPDATE SET owner_id=EXCLUDED.owner_id,group_title=EXCLUDED.group_title''',
           update.effective_chat.id, owner_id, update.effective_chat.title or '')
    await update.message.reply_text(f'✅ Group owner set to {owner_id}.')


async def add_story(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg, user = update.effective_message, update.effective_user
    if update.effective_chat.type != 'private':
        await msg.reply_text('Privacy ke liye story bot ki private chat me save karein.')
        return
    await register_user(user)
    title = ' '.join(context.args).strip()
    source = msg.reply_to_message
    if not title or not source:
        await msg.reply_text('Pehle photo + caption + link bhejein. Us message ko Reply karke /addstory Kalapani likhein.')
        return
    if len(title) > 100 or not (source.photo or source.text or source.document or source.video or source.animation or source.audio):
        await msg.reply_text('Title 1–100 characters ka ho aur replied message photo/text/document/video/audio ho.')
        return
    if source.from_user and source.from_user.id != user.id:
        await msg.reply_text('Apne hi bheje hue message ko reply karein.')
        return
    caption = source.caption or source.text or ''
    if len(caption) > 4000:
        await msg.reply_text('Caption bahut lamba hai (max 4000 characters).')
        return
    links = re.findall(r'https://(?:t\.me|telegram\.me)/[^\s<>]+', caption, flags=re.I)
    url = links[0].rstrip('.,)') if links else ''
    photo_id = source.photo[-1].file_id if source.photo else None
    key = norm(title)
    async with pool.acquire() as c:
        async with c.transaction():
            count = await c.fetchval('SELECT COUNT(*) FROM stories_v2 WHERE owner_id=$1 AND deleted_at IS NULL', user.id)
            exists = await c.fetchval('SELECT 1 FROM stories_v2 WHERE owner_id=$1 AND title_key=$2 AND deleted_at IS NULL', user.id, key)
            if count >= MAX_STORIES_PER_USER and not exists:
                await msg.reply_text('Tumhari 1000 story limit complete ho gayi hai.')
                return
            if exists:
                await msg.reply_text('Ye story pehle se saved hai. Existing story overwrite nahi hogi. /deletestory Title se hide kar sakte hain, ya /restorestory Title se restore kar sakte hain.')
                return
            inserted = await c.fetchval('''INSERT INTO stories_v2(owner_id,title,title_key,category,description,url,caption,photo_id,source_chat_id,source_message_id)
              VALUES($1,$2,$3,'Story',$4,$5,$6,$7,$8,$9)
              ON CONFLICT(owner_id,title_key) DO NOTHING RETURNING id''',
              user.id,title,key,caption[:500],url,caption,photo_id,source.chat_id,source.message_id)
            if inserted is None:
                await msg.reply_text('Story pehle se database me hai (shayad hidden). /restorestory Title use karein.')
                return
            pending = await c.fetch('''SELECT DISTINCT chat_id FROM requests_v2
                 WHERE owner_id=$1 AND title_key=$2 AND resolved_at IS NULL''', user.id, key)
            await c.execute('UPDATE requests_v2 SET resolved_at=now() WHERE owner_id=$1 AND title_key=$2 AND resolved_at IS NULL', user.id, key)
    sent, failed = 0, 0
    for r in pending:
        try:
            await context.bot.send_message(r['chat_id'], f'🎉 Story available: {title}\n{url}')
            sent += 1
        except Exception as e:
            log.warning('Notification failed: %s', e)
            failed += 1
    await msg.reply_text(f'✅ Story saved: {title}\n🖼 Photo: {"Yes" if photo_id else "No / original message"}\n🔔 Alerts: {sent} sent, {failed} failed')


async def search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = ' '.join(context.args).strip()
    if not q:
        await update.message.reply_text('Use: /search Kalapani')
        return
    await search_for_message(update, q)


async def search_for_message(update, q):
    if not (2 <= len(q) <= 100):
        return
    chat, user, msg = update.effective_chat, update.effective_user, update.effective_message
    owner_id = await owner_for_chat(chat)
    if owner_id is None:
        await msg.reply_text('Is group ka owner set nahi hai. Group admin /claimgroup bheje.')
        return
    matches = await lookup(owner_id, q)
    if matches:
        await deliver_matches(botapp.bot, chat.id, matches, msg.message_id)
    else:
        await register_user(user)
        async with pool.acquire() as c:
            await c.execute('''INSERT INTO requests_v2(owner_id,title_key,title,user_id,chat_id)
              VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING''', owner_id,norm(q),q,user.id,chat.id)
        await msg.reply_text(missing_text(q), reply_markup=contact_button())


async def group_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg or not msg.text or not msg.from_user or msg.from_user.is_bot:
        return
    q = msg.text.strip()
    if not (2 <= len(q) <= 160) or '\n' in q or 'http' in q.lower() or q.startswith('/'):
        return
    owner_id = await owner_for_chat(update.effective_chat)
    if owner_id is None:
        return  # First verify the group owner with /claimgroup.
    matches = await lookup(owner_id, q)
    if matches:
        await deliver_matches(context.bot, update.effective_chat.id, matches, msg.message_id)
        return

    # Reply to likely missing story names, without responding to every chat message.
    if not (2 <= len(q.split()) <= 8):
        return
    if norm(q) in {'good morning', 'good night', 'thank you', 'how are you',
                   'kya haal', 'kaise ho', 'hello everyone', 'hi everyone'}:
        return
    now = time.monotonic()
    chat_id = update.effective_chat.id
    if now - missing_reply_times.get(chat_id, 0) < 45:
        return
    missing_reply_times[chat_id] = now
    await register_user(update.effective_user)
    async with pool.acquire() as c:
        await c.execute('''INSERT INTO requests_v2(owner_id,title_key,title,user_id,chat_id)
            VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING''',
            owner_id, norm(q), q, update.effective_user.id, chat_id)
    await msg.reply_text(missing_text(q), reply_markup=contact_button())


async def inline_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    iq = update.inline_query
    q = iq.query.strip()
    if not q:
        await iq.answer([], is_personal=True, cache_time=1)
        return
    # Telegram does NOT provide the destination group id in inline queries.
    # Therefore inline search is deliberately scoped to the caller's OWN stories.
    found = await lookup(iq.from_user.id, q[:100])
    results = []
    for s in found[:8]:
        results.append(InlineQueryResultArticle(
            id=str(s['id']), title=s['title'], description=(s['caption'] or s['description'] or '')[:100],
            input_message_content=InputTextMessageContent(s['caption'] or story_text(s)),
            thumbnail_url=None))
    await iq.answer(results, is_personal=True, cache_time=1)


async def my_stories(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await register_user(update.effective_user)
    async with pool.acquire() as c:
        rows = await c.fetch('''SELECT title FROM stories_v2 WHERE owner_id=$1 AND deleted_at IS NULL ORDER BY title LIMIT 40''', update.effective_user.id)
        count = await c.fetchval('SELECT COUNT(*) FROM stories_v2 WHERE owner_id=$1 AND deleted_at IS NULL', update.effective_user.id)
    await update.message.reply_text(f'📚 Your stories: {count}/{MAX_STORIES_PER_USER}\n' +
        ('\n'.join('• '+r['title'] for r in rows) if rows else 'No stories yet.') +
        ('\n...more not shown' if count > 40 else ''))


async def delete_story(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != 'private':
        await update.effective_message.reply_text('Story delete karne ke liye bot ki private chat use karein.')
        return
    title = ' '.join(context.args).strip()
    if not title:
        await update.message.reply_text('Use: /deletestory Kalapani')
        return
    async with pool.acquire() as c:
        row = await c.fetchrow('SELECT id,title FROM stories_v2 WHERE owner_id=$1 AND title_key=$2 AND deleted_at IS NULL',
                               update.effective_user.id, norm(title))
    if not row:
        await update.message.reply_text('Story not found in your collection.')
        return
    # Two-step delete; confirmation only by the same owner and for the same story.
    context.user_data['pending_delete'] = row['id']
    await update.message.reply_text(f"Delete '{row['title']}'? Reply /confirmdelete to confirm or /canceldelete to cancel. This hides the story and keeps a recovery copy.")


async def confirm_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != 'private':
        return
    story_id = context.user_data.pop('pending_delete', None)
    if not story_id:
        await update.message.reply_text('No pending deletion. First use /deletestory Title.')
        return
    async with pool.acquire() as c:
        status = await c.execute('UPDATE stories_v2 SET deleted_at=now() WHERE id=$1 AND owner_id=$2 AND deleted_at IS NULL',
                                 story_id, update.effective_user.id)
    await update.message.reply_text('Story hidden (recoverable).' if status == 'UPDATE 1' else 'Story already deleted or unavailable.')


async def cancel_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop('pending_delete', None)
    await update.effective_message.reply_text('Deletion cancelled.')


async def restore_story(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != 'private':
        return
    title = ' '.join(context.args).strip()
    if not title:
        await update.message.reply_text('Use: /restorestory Kalapani')
        return
    async with pool.acquire() as c:
        status = await c.execute('UPDATE stories_v2 SET deleted_at=NULL WHERE owner_id=$1 AND title_key=$2 AND deleted_at IS NOT NULL',
                                 update.effective_user.id, norm(title))
    await update.message.reply_text('Story restored.' if status == 'UPDATE 1' else 'No deleted story found.')


async def requests_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with pool.acquire() as c:
        rows = await c.fetch('''SELECT title,COUNT(*) AS n FROM requests_v2
           WHERE owner_id=$1 AND resolved_at IS NULL GROUP BY title_key,title ORDER BY n DESC LIMIT 30''', update.effective_user.id)
    await update.message.reply_text('📋 Requests:\n' +
        ('\n'.join(f"{r['title']}: {r['n']}" for r in rows) if rows else 'None'))


async def all_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_super(update.effective_user):
        return
    async with pool.acquire() as c:
        rows = await c.fetch('''SELECT u.user_id,u.username,u.first_name,COUNT(s.id) AS n
            FROM bot_users u LEFT JOIN stories_v2 s ON s.owner_id=u.user_id AND s.deleted_at IS NULL
            GROUP BY u.user_id,u.username,u.first_name ORDER BY n DESC LIMIT 40''')
    await update.message.reply_text('👥 Users:\n' + ('\n'.join(
        f"{r['user_id']} @{r['username'] or '-'} — {r['n']} stories" for r in rows) or 'None'))


async def user_stories(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_super(update.effective_user):
        return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text('Use: /userstories 123456789')
        return
    owner = int(context.args[0])
    async with pool.acquire() as c:
        rows = await c.fetch('SELECT * FROM stories_v2 WHERE owner_id=$1 AND deleted_at IS NULL ORDER BY title LIMIT 30', owner)
    await update.message.reply_text(f'User {owner}: {len(rows)} shown (max 30)')
    for s in rows:
        await send_story(context.bot, update.effective_chat.id, s)


async def all_stories(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_super(update.effective_user):
        return
    async with pool.acquire() as c:
        rows = await c.fetch('SELECT owner_id,title FROM stories_v2 WHERE deleted_at IS NULL ORDER BY id DESC LIMIT 50')
        total = await c.fetchval('SELECT COUNT(*) FROM stories_v2 WHERE deleted_at IS NULL')
    await update.message.reply_text(f'📚 All stories: {total}\n' +
        ('\n'.join(f"{r['owner_id']}: {r['title']}" for r in rows) or 'None') + '\n(Last 50 shown)')


async def all_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_super(update.effective_user):
        return
    async with pool.acquire() as c:
        rows = await c.fetch('SELECT chat_id,owner_id,group_title FROM group_owners ORDER BY chat_id LIMIT 50')
    await update.message.reply_text('👥 Groups:\n' +
        ('\n'.join(f"{r['chat_id']} — owner {r['owner_id']} — {r['group_title']}" for r in rows) or 'None'))


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_super(update.effective_user):
        return
    async with pool.acquire() as c:
        u = await c.fetchval('SELECT COUNT(*) FROM bot_users')
        s = await c.fetchval('SELECT COUNT(*) FROM stories_v2 WHERE deleted_at IS NULL')
        g = await c.fetchval('SELECT COUNT(*) FROM group_owners')
        r = await c.fetchval('SELECT COUNT(*) FROM requests_v2 WHERE resolved_at IS NULL')
    await update.message.reply_text(f'📊 Users: {u}\nStories: {s}\nGroups: {g}\nPending requests: {r}')


async def help_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('Commands:\n/addstory Title (reply to message)\n/mystories\n'
        '/deletestory Title /confirmdelete /canceldelete /restorestory Title\n/privacy\n/search Title\n/requests\n/claimgroup (in group)\n'
        'Super Admin: /allusers /allstories /userstories ID /allgroups /stats /setowner ID')


async def delete_service_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg or update.effective_chat.type not in ('group', 'supergroup'):
        return
    if msg.new_chat_members or msg.left_chat_member:
        try:
            await msg.delete()
        except Exception as exc:
            log.warning('Could not delete join/leave service message: %s', exc)


for cmd, fn in [('start',start),('help',help_cmd),('privacy',privacy_cmd),('confirmdelete',confirm_delete),('canceldelete',cancel_delete),('restorestory',restore_story),('helpadmin',help_admin),
                ('claimgroup',claim_group),('setowner',set_owner),('addstory',add_story),
                ('search',search),('mystories',my_stories),('deletestory',delete_story),
                ('requests',requests_cmd),('allusers',all_users),('userstories',user_stories),
                ('allstories',all_stories),('allgroups',all_groups),('stats',stats)]:
    botapp.add_handler(CommandHandler(cmd, fn))
botapp.add_handler(InlineQueryHandler(inline_search))
botapp.add_handler(CallbackQueryHandler(pick_story, pattern=r"^storypick:[0-9]+$"))
botapp.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS, group_text))
botapp.add_handler(MessageHandler(
    filters.StatusUpdate.NEW_CHAT_MEMBERS | filters.StatusUpdate.LEFT_CHAT_MEMBER,
    delete_service_message
))


async def init_db():
    async with pool.acquire() as c:
        await c.execute('''CREATE TABLE IF NOT EXISTS bot_users (
          user_id BIGINT PRIMARY KEY,username TEXT,first_name TEXT)''')
        await c.execute('''CREATE TABLE IF NOT EXISTS group_owners (
          chat_id BIGINT PRIMARY KEY,owner_id BIGINT NOT NULL,group_title TEXT)''')
        await c.execute('''CREATE TABLE IF NOT EXISTS stories_v2 (
          id BIGSERIAL PRIMARY KEY,owner_id BIGINT NOT NULL,title TEXT NOT NULL,
          title_key TEXT NOT NULL,category TEXT NOT NULL DEFAULT 'Story',
          description TEXT NOT NULL DEFAULT '',url TEXT NOT NULL DEFAULT '',
          caption TEXT NOT NULL DEFAULT '',photo_id TEXT,
          source_chat_id BIGINT,source_message_id BIGINT,
          deleted_at TIMESTAMPTZ,
          UNIQUE(owner_id,title_key))''')
        await c.execute('ALTER TABLE stories_v2 ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ')
        await c.execute('''CREATE INDEX IF NOT EXISTS stories_v2_owner_idx ON stories_v2(owner_id)''')
        await c.execute('''CREATE TABLE IF NOT EXISTS requests_v2 (
          owner_id BIGINT NOT NULL,title_key TEXT NOT NULL,title TEXT NOT NULL,
          user_id BIGINT NOT NULL,chat_id BIGINT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY(owner_id,title_key,user_id,chat_id))''')
        await c.execute('ALTER TABLE requests_v2 ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ')
        await c.execute('''INSERT INTO bot_users(user_id,username,first_name)
           VALUES($1,$2,'Super Admin') ON CONFLICT(user_id) DO NOTHING''', ADMIN_ID, ADMIN_USERNAME)
        await c.execute('''CREATE TABLE IF NOT EXISTS migration_flags (name TEXT PRIMARY KEY)''')
        migrated = await c.fetchval("SELECT 1 FROM migration_flags WHERE name='legacy_v1'")
        if migrated:
            return
        # One-time migration from original single-admin schema.
        legacy_exists = await c.fetchval("SELECT to_regclass('public.stories') IS NOT NULL")
        if legacy_exists:
            await c.execute('''INSERT INTO stories_v2(owner_id,title,title_key,category,description,url,caption)
                SELECT $1,title,title_key,category,description,url,
                '📚 ' || title || E'\\n🏷 ' || category || E'\\n📝 ' || description || E'\\n🔗 ' || url
                FROM stories ON CONFLICT(owner_id,title_key) DO NOTHING''', ADMIN_ID)
        legacy_requests = await c.fetchval("SELECT to_regclass('public.requests') IS NOT NULL")
        if legacy_requests:
            await c.execute('''INSERT INTO requests_v2(owner_id,title_key,title,user_id,chat_id)
                SELECT $1,title_key,title,user_id,chat_id FROM requests
                ON CONFLICT DO NOTHING''', ADMIN_ID)
        await c.execute("INSERT INTO migration_flags(name) VALUES('legacy_v1') ON CONFLICT DO NOTHING")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5, ssl='require')
    await init_db()
    await botapp.initialize()
    await botapp.start()
    await botapp.bot.set_webhook(f'{BASE_URL}/telegram', secret_token=WEBHOOK_SECRET,
       allowed_updates=['message','inline_query','callback_query'], drop_pending_updates=False)
    log.info('Webhook ready')
    try:
        yield
    finally:
        await botapp.stop()
        await botapp.shutdown()
        await pool.close()


app = FastAPI(lifespan=lifespan)

@app.get('/')
async def home():
    return {'status':'ok','service':'Story Search Bot v2'}

@app.post('/telegram')
async def telegram_webhook(request: Request):
    if not secrets.compare_digest(request.headers.get('X-Telegram-Bot-Api-Secret-Token',''), WEBHOOK_SECRET):
        raise HTTPException(status_code=403, detail='Forbidden')
    update = Update.de_json(await request.json(), botapp.bot)
    await botapp.process_update(update)
    return {'ok':True}
