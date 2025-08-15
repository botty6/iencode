import os
import logging
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
from dotenv import load_dotenv
from worker.tasks import encode_video_task

# Load .env
load_dotenv()

# Logging setup
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Explicit configs
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
APP_URL = os.getenv("APP_URL", "").strip()
PORT = int(os.getenv("PORT", "8443"))

try:
    ADMIN_USER_IDS = [int(uid.strip()) for uid in os.getenv("ADMIN_USER_IDS", "").split(",") if uid.strip()]
except ValueError:
    logger.error("ADMIN_USER_IDS must be a comma-separated list of integers.")
    ADMIN_USER_IDS = []

UNAUTHORIZED_MESSAGE = (
    "👋 Welcome to the **Video Encoder Bot**!\n\n"
    "This is a private service, and your User ID is not authorized."
)

# --- Handlers ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in ADMIN_USER_IDS:
        await update.message.reply_text("👋 Hello! Send me a video to encode.")
    else:
        await update.message.reply_text(UNAUTHORIZED_MESSAGE, parse_mode="Markdown")
    logger.info(f"/start from {user_id} ({update.effective_user.username})")

async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_USER_IDS:
        await update.message.reply_text(UNAUTHORIZED_MESSAGE, parse_mode="Markdown")
        return

    video_file = update.message.document or update.message.video
    if not video_file:
        await update.message.reply_text("🤔 Please send a video file.")
        return

    safe_filename = (video_file.file_name or "video").replace("_", "\\_")  # Escape underscores for Markdown

    keyboard = [
        [InlineKeyboardButton("✅ 720p (Default)", callback_data=f"encode|720|{video_file.file_id}")],
        [InlineKeyboardButton("🚀 1080p", callback_data=f"encode|1080|{video_file.file_id}"),
         InlineKeyboardButton("💾 480p", callback_data=f"encode|480|{video_file.file_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"🎬 Received file: `{safe_filename}`\n\nPlease choose an output quality:",
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    try:
        action, quality, file_id = query.data.split("|", 2)
    except ValueError:
        await query.edit_message_text("❌ Invalid selection data.")
        return

    if action == "encode":
        await query.edit_message_text(f"✅ Queueing for {quality}p encoding...")
        logger.info(f"Celery job: user={query.from_user.id}, quality={quality}")
        encode_video_task.delay(user_id=query.from_user.id, file_id=file_id, quality=quality)

# --- Main startup ---
async def main():
    if not BOT_TOKEN or not APP_URL:
        logger.critical("Missing BOT_TOKEN or APP_URL.")
        return

    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(MessageHandler(filters.VIDEO | filters.Document.MimeType("video/"), handle_video))
    application.add_handler(CallbackQueryHandler(button_callback))

    await application.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=BOT_TOKEN,
        webhook_url=f"{APP_URL}/{BOT_TOKEN}"
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except RuntimeError as e:
        # Fix for "asyncio.run() cannot be called from a running event loop"
        logger.warning(f"Runtime loop conflict: {e}, using alternative runner.")
        import nest_asyncio
        nest_asyncio.apply()
        asyncio.get_event_loop().run_until_complete(main())
