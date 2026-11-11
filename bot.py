import os
from pyrogram import Client, filters

# Environment variables se details uthayenge
API_ID = int(os.environ.get("API_ID", ""))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", ""))

app = Client(
    "FileStoreBot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

@app.on_message(filters.command("start"))
async def start_handler(client, message):
    # Check karein agar user ne kisi file ke link se start kiya hai
    if len(message.command) > 1:
        file_id = message.command[1]
        try:
            # Channel se file user ko bhej dein
            await client.copy_message(
                chat_id=message.chat.id,
                from_chat_id=CHANNEL_ID,
                message_id=int(file_id)
            )
        except Exception as e:
            await message.reply_text(f"❌ Error: File nahi mili ya delete ho gayi hai.")
        return

    await message.reply_text(
        "👋 Hello! Main ek File Store Bot hoon.\n\n"
        "Mujhe apne Store Channel mein koi bhi file bhejiye, main uska shareable link de dunga!"
    )

# Jab aap apne channel par file daalein, toh bot uska link bana de
@app.on_message(filters.chat(CHANNEL_ID) & (filters.document | filters.video | filters.audio | filters.photo))
async def store_file(client, message):
    file_id = message.id
    bot_username = client.me.username
    share_link = f"https://t.me/{bot_username}?start={file_id}"
    
    await message.reply_text(
        f"✅ **File Saved Successfully!**\n\n"
        f"🔗 **Share Link:**\n`{share_link}`",
        quote=True
    )

print("Bot is starting...")
app.run()
