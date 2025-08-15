import os
import logging
import asyncio
from aiohttp import web
from pyrogram import Client, filters
from pyrogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from worker.tasks import encode_video_task

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "").strip()
ADMIN_USER_IDS = [int(uid.strip()) for uid in os.getenv("ADMIN_USER_IDS", "").split(",") if uid.strip()]
PORT = int(os.getenv("PORT", "8080"))
APP_URL = os.getenv("APP_URL")

# --- Pyrogram Client Initialization ---
app = Client("encoder_bot", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH, workdir="/tmp")

# --- Constants ---
UNAUTHORIZED_MESSAGE = "👋 Welcome!\nThis is a private bot and you are not authorized to use it."
VIDEO_EXTENSIONS = (".mkv", ".mp4", ".webm", ".avi", ".mov", ".flv", ".wmv")

# --- Handlers ---
@app.on_message(filters.command("start") & filters.private)
async def start_command(client: Client, message: Message):
    if message.from_user.id in ADMIN_USER_IDS:
        await message.reply_text("👋 Hello! Send me a video file to get started.")
    else:
        await message.reply_text(UNAUTHORIZED_MESSAGE)

@app.on_message((filters.video | filters.document) & filters.private)
async def handle_video(client: Client, message: Message):
    if message.from_user.id not in ADMIN_USER_IDS:
        await message.reply_text(UNAUTHORIZED_MESSAGE)
        return

    file = message.video or message.document
    file_name = getattr(file, "file_name", "unknown_file.tmp")

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
        
        encode_video_task.delay(
            user_chat_id=callback_query.message.chat.id,
            message_id=int(message_id),
            quality=quality,
        )

# --- aiohttp Web Server for Heroku ---
async def webhook_handler(request: web.Request):
    """Handles incoming raw updates from Telegram and feeds them to Pyrogram."""
    try:
        await app.feed_update(await request.json())
    except Exception as e:
        logger.error("Error handling webhook update: %s", e)
    finally:
        return web.Response(status=200)

async def health_check(request: web.Request):
    """A simple endpoint to confirm the web server is running."""
    return web.Response(text="OK")

async def main():
    """Main entry point to start the bot and web server."""
    if not all([BOT_TOKEN, API_ID, API_HASH, APP_URL]):
        logger.critical("One or more critical environment variables are missing!")
        return
        
    await app.start()
    
    # Set the webhook
    webhook_url = f"{APP_URL}/{BOT_TOKEN}"
    await app.set_bot_webhook(url=webhook_url)
    logger.info(f"Webhook set to {webhook_url}")
    
    # Start the web server
    webapp = web.Application()
    webapp.add_routes([
        web.post(f"/{BOT_TOKEN}", webhook_handler),
        web.get("/", health_check)
    ])
    runner = web.AppRunner(webapp)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    logger.info(f"Starting web server on port {PORT}")
    await site.start()
    
    await asyncio.Event().wait() # Keep the main coroutine running

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped.")
        
