import os
import logging
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
from dotenv import load_dotenv
from worker.tasks import encode_video_task

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
APP_URL = os.environ.get("APP_URL")
PORT = int(os.environ.get("PORT", 8443))

try:
    ADMIN_USER_IDS = [int(user_id.strip()) for user_id in os.environ.get("ADMIN_USER_IDS", "").split(',')]
except (ValueError, AttributeError):
    logger.error("ADMIN_USER_IDS is not set correctly. Please provide a comma-separated list of numbers.")
    ADMIN_USER_IDS = []

# --- A more welcoming message for unauthorized users ---
UNAUTHORIZED_MESSAGE = (
    "👋 Welcome to the **Video Encoder Bot**!\n\n"
    "This is a private service, and your User ID is not on the authorized list. "
    "If you believe you should have access, please contact the bot administrator."
)

# --- Command Handlers ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in ADMIN_USER_IDS:
        await update.message.reply_text("👋 Hello! I'm your friendly encoding bot. Send me a video file to get started.")
    else:
        await update.message.reply_text(UNAUTHORIZED_MESSAGE, parse_mode='Markdown')
    logger.info(f"User {user_id} ({update.effective_user.username}) used /start.")

async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_USER_IDS:
        await update.message.reply_text(UNAUTHORIZED_MESSAGE, parse_mode='Markdown')
        return

    video_file = update.message.document or update.message.video
    if not video_file:
        await update.message.reply_text("🤔 That doesn't look like a video file. Please send a video or document.")
        return

    keyboard = [
        [InlineKeyboardButton("✅ 720p (Default)", callback_data=f"encode_720_{video_file.file_id}")],
        [InlineKeyboardButton("🚀 1080p", callback_data=f"encode_1080_{video_file.file_id}"),
         InlineKeyboardButton("💾 480p", callback_data=f"encode_480_{video_file.file_id}")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"🎬 Received file: `{video_file.file_name}`\n\nPlease choose an output quality:",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    action, quality, file_id = query.data.split('_')
    if action == "encode":
        await query.edit_message_text(text=f"✅ Great! Queueing file for a {quality}p encode. I'll let you know when it's done!")
        logger.info(f"Sending job to Celery: user={query.from_user.id}, file_id={file_id}, quality={quality}")
        encode_video_task.delay(user_id=query.from_user.id, file_id=file_id, quality=quality)

# --- Main Application ---

async def main():
    if not BOT_TOKEN:
        logger.critical("BOT_TOKEN environment variable is not set! Exiting.")
        return
    if not APP_URL:
        logger.critical("APP_URL environment variable is not set! Exiting.")
        return
    if not ADMIN_USER_IDS:
        logger.warning("ADMIN_USER_IDS is not set. The bot will not respond to anyone.")

    application = Application.builder().token(BOT_TOKEN).build()

    # Add handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(MessageHandler(filters.Document.VIDEO | filters.VIDEO, handle_video))
    application.add_handler(CallbackQueryHandler(button_callback))
    
    # Use the library's context manager for graceful setup and shutdown
    async with application:
        logger.info(f"Starting webhook on port {PORT}")
        await application.bot.set_webhook(url=f"{APP_URL}/{BOT_TOKEN}")
        
        # Start the webhook listener without the redundant application.start()
        await application.start_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=BOT_TOKEN
        )
        
        # Keep the application running
        stop_event = asyncio.Event()
        await stop_event.wait()


if __name__ == '__main__':
    # Use the simple, correct way to run an asyncio application
    asyncio.run(main())

