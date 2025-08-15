import os
import logging
from pyrogram import Client, filters
from pyrogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from worker.tasks import encode_video_task

# ------- Configuration -------
# Load environment variables from .env file for local development
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# Explicitly read and strip environment variables for robustness
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "").strip()
ADMIN_USER_IDS = [int(uid.strip()) for uid in os.getenv("ADMIN_USER_IDS", "").split(",") if uid.strip()]

# Pyrogram Client - this is our single bot instance
# We set workdir to /tmp to work on Heroku's ephemeral filesystem
app = Client("encoder_bot", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH, workdir="/tmp")

# --- Constants ---
UNAUTHORIZED_MESSAGE = (
    "👋 Welcome!\nThis is a private bot. "
    "If you believe you should have access, please contact the administrator."
)
VIDEO_EXTENSIONS = (".mkv", ".mp4", ".webm", ".avi", ".mov", ".flv", ".wmv")

# ------- Handlers -------

@app.on_message(filters.command("start") & filters.private)
async def start_command(client: Client, message: Message):
    """Handles the /start command."""
    if message.from_user.id in ADMIN_USER_IDS:
        await message.reply_text("👋 Hello! Send me a video file to get started.")
    else:
        await message.reply_text(UNAUTHORIZED_MESSAGE)

@app.on_message((filters.video | filters.document) & filters.private)
async def handle_video(client: Client, message: Message):
    """Handles incoming video or document files."""
    if message.from_user.id not in ADMIN_USER_IDS:
        await message.reply_text(UNAUTHORIZED_MESSAGE)
        return

    file = message.video or message.document
    file_name = getattr(file, "file_name", "unknown_file.tmp")
    mime_type = getattr(file, "mime_type", "application/octet-stream")

    is_video = mime_type.startswith("video/") or file_name.lower().endswith(VIDEO_EXTENSIONS)
    if not is_video:
        await message.reply_text("🤔 This doesn't look like a video file I can process.")
        return
        
    # We pass the chat_id and message_id to the worker so it can fetch the message
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ 720p", callback_data=f"encode|720|{message.id}")],
        [
            InlineKeyboardButton("🚀 1080p", callback_data=f"encode|1080|{message.id}"),
            InlineKeyboardButton("💾 480p", callback_data=f"encode|480|{message.id}"),
        ]
    ])
    await message.reply_text(
        f"🎬 Received file: `{file_name}`\nPlease choose an output quality:",
        reply_markup=keyboard,
    )

@app.on_callback_query()
async def button_callback(client: Client, callback_query: CallbackQuery):
    """Handles button presses from the inline keyboard."""
    user_id = callback_query.from_user.id
    if user_id not in ADMIN_USER_IDS:
        await callback_query.answer("🚫 You are not authorized for this action.", show_alert=True)
        return

    try:
        action, quality, message_id = callback_query.data.split("|", 2)
    except ValueError:
        await callback_query.answer("❌ Invalid button data.", show_alert=True)
        return

    if action == "encode":
        await callback_query.answer("✅ Job sent to queue!")
        await callback_query.message.edit_text(f"⏳ Your file is now in the queue for a {quality}p encode...")
        
        # Send the job to the Celery worker
        encode_video_task.delay(
            user_chat_id=callback_query.message.chat.id,
            message_id=int(message_id),
            quality=quality,
        )

# --- Entrypoint ---
# Pyrogram will automatically handle starting its own web server for webhooks.
# This single line is all that's needed to start the bot.
app.run()
