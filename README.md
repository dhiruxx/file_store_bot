# DJ Story Search Bot v2 — Existing bot upgrade

## What it does
- Each user can save up to 1000 stories by sending photo + caption, then replying `/addstory Story Name` in private chat.
- Group admins claim an unassigned group with `/claimgroup`. Group auto-search returns only its verified owner's stories.
- Super Admin can change a group owner with `/setowner USER_ID` inside that group.
- Inline queries (`@BotUsername query`) show **the searcher's own collection**, not group-owner stories. Telegram inline queries do not supply a destination group ID.
- `/search Story Name` creates a missing-story request when not found; plain group text only triggers on matches to avoid spam.
- Super Admin: `/allusers`, `/allstories`, `/userstories USER_ID`, `/allgroups`, `/stats`.
- The old `stories` and `requests` tables remain untouched. A one-time migration copies their records to the Super Admin's collection.

## GitHub upgrade
Replace existing `main.py` with this version and retain `requirements.txt`. Keep the existing Neon database and Render environment variables. Back up the database before deployment.

## Render
Build: `pip install -r requirements.txt`
Start: `uvicorn main:app --host 0.0.0.0 --port $PORT`
Environment variables: `BOT_TOKEN`, `DATABASE_URL`, `ADMIN_ID`, `ADMIN_USERNAME`, `RENDER_EXTERNAL_URL`, `WEBHOOK_SECRET`.
Do not post secrets publicly. Keep just **one** running deployment/webhook per bot token.

## Test
1. In private chat send `/start`.
2. Send a photo with caption including story title and a Telegram link.
3. Reply to **that photo message** with `/addstory Kalapani`.
4. Send `/mystories`, then `/search Kalapani`.
5. Add bot to group; as a group admin send `/claimgroup`.
6. Type `Kalapani` in the group. The bot should send the photo and caption.
7. In group send `/search MissingStory123`, then add that story in private chat. Check group notification.
8. For another user, start the bot, save a different story and claim a different group. Confirm collections remain isolated.

## Important limitations
- Telegram cannot show a full tutorial before the user presses Start. Set a concise BotFather About/Description; `/start` shows the full tutorial.
- Inline results are text articles. Group messages can return actual photos. Image thumbnails in inline mode need additional work with publicly accessible thumbnail URLs.
- Original non-photo messages are re-sent with `copy_message`; source messages must remain available. Photo `file_id` is saved for re-sending.
- Only users with admin status can claim an unassigned group; group ownership can be reassigned by the Super Admin.
- Super Admin can inspect saved content. Tell users this clearly.
- Render free web services may sleep; free database and hosting quotas apply. Backups are strongly recommended.
- Save and distribute only content you own or are permitted to share.
- 
