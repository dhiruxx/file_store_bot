"""Configuration reference.
The application reads configuration from Render Environment Variables.
Do not put real bot tokens or database passwords in this file.
"""
REQUIRED_VARIABLES = [
    "BOT_TOKEN",
    "DATABASE_URL",
    "STORAGE_CHANNEL_ID",
    "ADMIN_IDS",
    "WEBHOOK_SECRET",
    "WEBHOOK_URL",
    "FORCE_CHANNELS",
    "FORCE_LINKS",
]
