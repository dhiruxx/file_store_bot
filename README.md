# Telegram File Store Bot

## Features
- Private Telegram storage channel
- Up to 5,000 stored file records
- 50 MB per-file validation
- One share link for all files uploaded in one admin session
- Five mandatory subscription channels
- Link activates on first open and expires after two hours
- PostgreSQL metadata and persistent delivery progress
- Render Web Service + Telegram webhook

## Important limitations
- Telegram Bot API and Telegram rate limits apply.
- Free Render services can sleep and are not guaranteed 24/7.
- Files already delivered to a user's Telegram are not deleted when a share link expires.
- The bot stores file messages in your private channel using copy_message; it does not download files to Render.
- This project has not been load-tested for 5,000 simultaneous deliveries. Test gradually.

## 1. Create Telegram channels
1. Create or select a private storage channel.
2. Add your bot as an administrator in the storage channel.
3. Create/select exactly five subscription channels.
4. Add the bot as administrator in all five channels.
5. Record each channel's numeric ID and invite URL.

## 2. GitHub
Upload these files to the root of a GitHub repository:
- bot.py
- database.py (not required in this compact version; database schema is initialized by bot.py)
- config.py
- requirements.txt
- README.md

## 3. Neon
Create a PostgreSQL database and copy its connection string. Keep it secret.

## 4. Render
Create a Web Service connected to the GitHub repository.
- Runtime: Python
- Build command: `pip install -r requirements.txt`
- Start command: `uvicorn bot:app --host 0.0.0.0 --port $PORT`
- Use a free instance if available.

Set Environment Variables:
- BOT_TOKEN: BotFather token
- DATABASE_URL: Neon PostgreSQL connection string
- STORAGE_CHANNEL_ID: numeric ID of private storage channel, e.g. -100...
- ADMIN_IDS: your numeric Telegram user ID (comma-separated if multiple)
- MAX_FILES: 5000
- MAX_FILE_MB: 50
- LINK_EXPIRY_HOURS: 2
- FORCE_CHANNELS: exactly five numeric channel IDs, comma-separated
- FORCE_LINKS: exactly five invite URLs, comma-separated, same order as FORCE_CHANNELS
- WEBHOOK_SECRET: random string using letters and numbers (at least 32 characters)
- WEBHOOK_URL: public Render service URL, e.g. https://your-service.onrender.com

After saving environment variables, deploy/redeploy. Check Render logs for "Bot started as @...".

## 5. Admin usage
1. Open your bot in Telegram.
2. Send `/addfiles`.
3. Send files one by one (documents, videos, audio, photos, animations).
4. Send `/done`.
5. The bot creates one share link containing every file received in that upload session.
6. `/stats` shows stored file count, bundles, users and approximate total file size.
7. `/cancel` cancels the current upload session.

Only admins listed in ADMIN_IDS can upload files or view stats.

## 6. User usage
1. User opens the share link.
2. First open starts the two-hour expiry clock.
3. User joins the five channels.
4. User presses "I Joined - Verify".
5. Bot checks membership and queues file delivery.
6. Files arrive in the user's Telegram chat.

## 7. Test in this order
- Test one small document first.
- Test force-subscription with a test account.
- Test 10 files in one bundle.
- Test a file close to 50 MB.
- Test expiry using a temporary test setting of 1 minute, then restore 2 hours.
- Only after these pass, gradually test larger bundles.

## 8. Mobile GitHub upload
For each file:
1. Open repository in Chrome.
2. Tap Add file -> Create new file.
3. Enter the exact filename.
4. Paste the complete file contents.
5. Tap Commit changes.
Repeat for each project file.

## Security
Never publish BOT_TOKEN, DATABASE_URL, WEBHOOK_SECRET, or admin IDs in a public repository. Rotate credentials immediately if exposed.
