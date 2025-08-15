import os
import logging
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters
)
from dotenv import load_dotenv
from worker.tasks import encode_video_task

# --- CONFIGURATION ---
load_dotenv()

BOT_TOKEN = os.environ.get("BOT_TOKEN")
APP_URL = os.environ.get("APP_URL")  # Must be HTTPS
PORT = int(os.environ.get("PORT", 8443))

try:
    ADMIN_USER_IDS = [
        int(uid.strip()) for uid in os.environ.get("ADMIN_USER_IDS", "").split(",") if uid.strip()
    ]
except ValueError:
    ADMIN_USER_IDS = []

UNAUTHORIZED_MESSAGE = (
    "👋 Welcome to the **Video Encoder Bot**!\n\n"
    "This is a private service, and your User ID is not on the authorized list. "
    "If you believe you should have access, please contact the bot administrator."
)

# --- LOGGING ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- COMMAND HANDLERS ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in ADMIN_USER_IDS:
        await update.message.reply_text(
            "👋 Hello! I'm your friendly encoding bot. Send me a video file to get started."
        )
    else:
        await update.message.reply_text(UNAUTHORIZED_MESSAGE, parse_mode="Markdown")
    logger.info(f"User {user_id} ({update.effective_user.username}) used /start.")

async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_USER_IDS:
        await update.message.reply_text(UNAUTHORIZED_MESSAGE, parse_mode="Markdown")
        return

    video_file = update.message.document or update.message.video
    if not video_file:
        await update.message.reply_text(
            "🤔 That doesn't look like a video file. Please send a video or document."
        )
        return

    keyboard = [
        [InlineKeyboardButton("✅ 720p (Default)", callback_data=f"encode_720_{video_file.file_id}")],
        [
            InlineKeyboardButton("🚀 1080p", callback_data=f"encode_1080_{video_file.file_id}"),
            InlineKeyboardButton("💾 480p", callback_data=f"encode_480_{video_file.file_id}")
        ],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"🎬 Received file: `{video_file.file_name}`\n\nPlease choose an output quality:",
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    # Safer parsing in case file_id contains underscores
    parts = query.data.split("_", 2)
    if len(parts) != 3:
        await query.edit_message_text("❌ Invalid selection data.")
        return

    action, quality, file_id = parts
    if action == "encode":
        await query.edit_message_text(
            text=f"✅ Great! Queueing file for a {quality}p encode. I'll let you know when it's done!"
        )
        logger.info(f"Sending job to Celery: user={query.from_user.id}, file_id={file_id}, quality={quality}")
        encode_video_task.delay(user_id=query.from_user.id, file_id=file_id, quality=quality)

# --- MAIN ---
def main():
    if not BOT_TOKEN:
        logger.critical("BOT_TOKEN environment variable is not set! Exiting.")
        return
    if not APP_URL:
        logger.critical("APP_URL environment variable is not set! Exiting.")
        return

    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(MessageHandler(filters.Document.VIDEO | filters.VIDEO, handle_video))
    application.add_handler(CallbackQueryHandler(button_callback))

    # Run webhook server (Python 3.10 automatically manages event loop)
    application.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=BOT_TOKEN,
        webhook_url=f"{APP_URL}/{BOT_TOKEN}"
    )

if __name__ == "__main__":
    main()
