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

# Get configs from environment
BOT_TOKEN = os.environ.get("BOT_TOKEN")
try:
    ADMIN_USER_IDS = [int(user_id.strip()) for user_id in os.environ.get("ADMIN_USER_IDS", "").split(',')]
except (ValueError, AttributeError):
    logger.error("ADMIN_USER_IDS is not set correctly. Please provide a comma-separated list of numbers.")
    ADMIN_USER_IDS = []

# --- Command Handlers ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Greets the user and checks if they are authorized."""
    user_id = update.effective_user.id
    if user_id in ADMIN_USER_IDS:
        await update.message.reply_text("👋 Hello! I'm your friendly encoding bot. Send me a video file to get started.")
    else:
        await update.message.reply_text("🚫 Sorry, you are not authorized to use this bot.")
    logger.info(f"User {user_id} ({update.effective_user.username}) used /start.")

# --- Message Handlers ---

async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles incoming video files."""
    user_id = update.effective_user.id
    if user_id not in ADMIN_USER_IDS:
        await update.message.reply_text("🚫 You're not on the list! Access denied.")
        return

    video_file = update.message.document or update.message.video
    if not video_file:
        await update.message.reply_text("🤔 That doesn't look like a video file. Please send a video or document.")
        return

    # Create inline keyboard for quality options
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

# --- Callback Query Handler ---

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Parses the CallbackQuery and delegates to the right handler."""
    query = update.callback_query
    await query.answer()

    # Data is in the format "action_quality_file_id"
    action, quality, file_id = query.data.split('_')

    if action == "encode":
        # Let the user know the job has started
        await query.edit_message_text(text=f"✅ Great! Queueing `{file_id}` for a {quality}p encode. I'll let you know when it's done!")

        # Send the task to the Celery worker
        logger.info(f"Sending job to Celery: user={query.from_user.id}, file_id={file_id}, quality={quality}")
        encode_video_task.delay(
            user_id=query.from_user.id,
            file_id=file_id,
            quality=quality
        )

# --- Main Application ---

def main():
    """Starts the bot."""
    if not BOT_TOKEN:
        logger.critical("BOT_TOKEN environment variable is not set! Exiting.")
        return
    if not ADMIN_USER_IDS:
        logger.warning("ADMIN_USER_IDS is not set. The bot will not respond to anyone.")

    application = Application.builder().token(BOT_TOKEN).build()

    # Add handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(MessageHandler(filters.Document.VIDEO | filters.VIDEO, handle_video))
    application.add_handler(CallbackQueryHandler(button_callback))

    logger.info("Bot is starting up...")
    application.run_polling()

if __name__ == '__main__':
    main()
  
