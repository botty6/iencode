# bot.py
import os
import logging
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

# ---------- Load env & logging ----------
load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("video-encoder-bot")

# ---------- Explicit configuration ----------
BOT_TOKEN: str = (os.getenv("BOT_TOKEN") or "").strip()
APP_URL: str = (os.getenv("APP_URL") or "").strip()  # must be public HTTPS
PORT: int = int(os.getenv("PORT", "8443"))

try:
    ADMIN_USER_IDS = [
        int(uid.strip()) for uid in (os.getenv("ADMIN_USER_IDS") or "").split(",") if uid.strip()
    ]
except ValueError:
    logger.error("ADMIN_USER_IDS must be a comma-separated list of integers.")
    ADMIN_USER_IDS = []

UNAUTHORIZED_MESSAGE = (
    "👋 Welcome to the **Video Encoder Bot**!\n\n"
    "This is a private service, and your User ID is not on the authorized list. "
    "If you believe you should have access, please contact the bot administrator."
)

# ---------- Sanity checks (fail fast, clear logs) ----------
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

# ---------- Handlers ----------
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in ADMIN_USER_IDS:
        await update.message.reply_text("👋 Hello! Send me a video file to encode.")
    else:
        await update.message.reply_text(UNAUTHORIZED_MESSAGE, parse_mode="Markdown")
    logger.info("User %s (%s) used /start.", user_id, update.effective_user.username)

async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_USER_IDS:
        await update.message.reply_text(UNAUTHORIZED_MESSAGE, parse_mode="Markdown")
        return

    # Accept either a document with video mimetype or a native video
    video_file = update.message.document or update.message.video
    if not video_file:
        await update.message.reply_text("🤔 That doesn't look like a video. Please send a video or document.")
        return

    # Escape underscores to keep Markdown code formatting intact
    safe_name = (getattr(video_file, "file_name", None) or "video").replace("_", "\\_")

    # Use a '|' separator so underscores inside IDs never break parsing
    kb = [
        [InlineKeyboardButton("✅ 720p (Default)", callback_data=f"encode|720|{video_file.file_id}")],
        [
            InlineKeyboardButton("🚀 1080p", callback_data=f"encode|1080|{video_file.file_id}"),
            InlineKeyboardButton("💾 480p", callback_data=f"encode|480|{video_file.file_id}"),
        ],
    ]
    await update.message.reply_text(
        f"🎬 Received file: `{safe_name}`\n\nPlease choose an output quality:",
        reply_markup=InlineKeyboardMarkup(kb),
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
        logger.info("Queueing Celery job: user=%s, file_id=%s, quality=%s",
                    query.from_user.id, file_id, quality)
        # Celery task must accept keyword args
        encode_video_task.delay(user_id=query.from_user.id, file_id=file_id, quality=quality)

# ---------- Application factory ----------
def build_app() -> Application:
    application = Application.builder().token(BOT_TOKEN).build()

    # Handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(
        MessageHandler(
            # robust across PTB versions: native videos OR documents with video/*
            filters.VIDEO | filters.Document.MimeType("video/"),
            handle_video,
        )
    )
    application.add_handler(CallbackQueryHandler(button_callback))
    return application

# ---------- Entry point (no asyncio.run, no loop closing) ----------
def main() -> None:
    _validate_config()
    app = build_app()

    # Blocking call; PTB manages its own asyncio machinery internally.
    # Requires the 'webhooks' extra (tornado). See docs.
    app.run_webhook(
        listen="0.0.0.0",            # explicit host binding for Heroku/Docker
        port=PORT,                   # explicit port (Heroku provides $PORT)
        url_path=BOT_TOKEN,          # unique path → basic obscurity
        webhook_url=f"{APP_URL}/{BOT_TOKEN}",  # full public HTTPS URL
        # drop/retry defaults are fine for most cases; set explicitly if you need
    )

if __name__ == "__main__":
    # No asyncio.run(); this call blocks here until the process is stopped.
    main()
