import os
import logging
import asyncio
from urllib.parse import urlparse

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from worker.tasks import encode_video_task

# ------- Load env & logging -------
load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# ------- Explicit configuration -------
BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
APP_URL = (os.getenv("APP_URL") or "").strip()
PORT = int(os.getenv("PORT", "8443"))
# A list of common video file extensions for fallback checking
VIDEO_EXTENSIONS = (".mkv", ".mp4", ".webm", ".avi", ".mov", ".flv", ".wmv")

try:
    ADMIN_USER_IDS = [
        int(uid.strip())
        for uid in (os.getenv("ADMIN_USER_IDS") or "").split(",")
        if uid.strip()
    ]
except ValueError:
    logger.error("ADMIN_USER_IDS must be a comma-separated list of integers.")
    ADMIN_USER_IDS = []

UNAUTHORIZED_MESSAGE = (
    "👋 Welcome to the **Video Encoder Bot**!\n\n"
    "This is a private service, and your User ID is not on the authorized list. "
    "If you believe you should have access, please contact the bot administrator."
)

# ------- Validation -------
def _validate_config() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set.")
    if not APP_URL:
        raise RuntimeError("APP_URL is not set.")
    parsed = urlparse(APP_URL)
    if parsed.scheme != "https":
        logger.warning("APP_URL is not HTTPS; Telegram requires HTTPS in production.")
    if not parsed.netloc:
        raise RuntimeError("APP_URL is invalid (no host).")
    if not isinstance(PORT, int) or PORT <= 0:
        raise RuntimeError("PORT must be a positive integer.")

# ------- Handlers -------
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in ADMIN_USER_IDS:
        await update.message.reply_text(
            "👋 Hello! I'm your friendly encoding bot. Send me a video file to get started."
        )
    else:
        await update.message.reply_text(UNAUTHORIZED_MESSAGE, parse_mode="Markdown")
    logger.info("User %s (%s) used /start.", user_id, update.effective_user.username)

async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    logger.info("File handler triggered for user %s.", user_id) # <-- ADDED LOGGING

    if user_id not in ADMIN_USER_IDS:
        await update.message.reply_text(UNAUTHORIZED_MESSAGE, parse_mode="Markdown")
        return

    video_file = update.message.document or update.message.video
    if not video_file:
        # This should ideally not be reached if the filter is correct
        logger.warning("handle_video triggered but no video or document found.")
        return

    file_name = getattr(video_file, "file_name", "unknown_file")
    mime_type = getattr(video_file, "mime_type", "unknown_mime")
    logger.info("Received file. Name: '%s', MIME Type: '%s'", file_name, mime_type) # <-- ADDED LOGGING

    # This is our new, more robust check for a valid video file
    is_video = (
        mime_type.startswith("video/") or
        file_name.lower().endswith(VIDEO_EXTENSIONS)
    )

    if not is_video:
        logger.info("File '%s' is not a video. Replying to user.", file_name) # <-- ADDED LOGGING
        await update.message.reply_text(
            "🤔 This doesn't look like a video file I can process. Please send a file like .mkv, .mp4, etc."
        )
        return

    safe_name = file_name.replace("_", "\\_")
    keyboard = [
        [
            InlineKeyboardButton(
                "✅ 720p (Default)", callback_data=f"encode|720|{video_file.file_id}"
            )
        ],
        [
            InlineKeyboardButton(
                "🚀 1080p", callback_data=f"encode|1080|{video_file.file_id}"
            ),
            InlineKeyboardButton(
                "💾 480p", callback_data=f"encode|480|{video_file.file_id}"
            ),
        ],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"🎬 Received file: `{safe_name}`\n\nPlease choose an output quality:",
        reply_markup=reply_markup,
        parse_mode="Markdown",
    )

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    try:
        action, quality, file_id = query.data.split("|", 2)
    except Exception:
        await query.edit_message_text("❌ Invalid selection data.")
        logger.exception("Failed to parse callback data: %r", query.data)
        return

    if action == "encode":
        await query.edit_message_text(
            text=f"✅ Great! Queueing file for a {quality}p encode. I'll let you know when it's done!"
        )
        logger.info(
            "Queueing Celery job: user=%s, file_id=%s, quality=%s",
            query.from_user.id,
            file_id,
            quality,
        )
        encode_video_task.delay(
            user_id=query.from_user.id, file_id=file_id, quality=quality
        )

# ------- App factory -------
def build_app() -> Application:
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start_command))
    # This filter is now broader to catch all documents and videos.
    application.add_handler(
        MessageHandler(filters.VIDEO | filters.Document.ALL, handle_video)
    )
    application.add_handler(CallbackQueryHandler(button_callback))
    return application

# ------- Entrypoint -------
def main() -> None:
    _validate_config()

    try:
        asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    app = build_app()

    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=BOT_TOKEN,
        webhook_url=f"{APP_URL}/{BOT_TOKEN}",
    )

if __name__ == "__main__":
    main()
    
