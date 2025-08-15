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

# --- Pyrogram Client Initialization ---
app = Client("encoder_bot", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH, workdir="/tmp")

# --- Handlers (No changes) ---
@app.on_message(filters.command("start") & filters.private)
async def start_command(client, message):
    if message.from_user.id in ADMIN_USER_IDS:
        await message.reply_text("👋 Hello! Send me a video file to get started.")
    else:
        await message.reply_text("👋 Welcome!\nThis is a private bot and you are not authorized to use it.")

@app.on_message((filters.video | filters.document) & filters.private)
async def handle_video(client, message):
    if message.from_user.id not in ADMIN_USER_IDS:
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
    await message.reply_text(f"🎬 Received file: `{file_name}`\nPlease choose an output quality:", reply_markup=keyboard)

@app.on_callback_query()
async def button_callback(client, callback_query):
    user_id = callback_query.from_user.id
    if user_id not in ADMIN_USER_IDS:
        await callback_query.answer("🚫 You are not authorized.", show_alert=True)
        return
    action, quality, message_id = callback_query.data.split("|", 2)
    if action == "encode":
        await callback_query.answer("✅ Job sent to queue!")
        await callback_query.message.edit_text(f"⏳ Your file is now in the queue for a {quality}p encode...")
        encode_video_task.delay(
            user_chat_id=callback_query.message.chat.id,
            message_id=int(message_id),
            quality=quality,
        )

# --- Main Entrypoint ---
async def main():
    """Starts the web server and the Pyrogram client together."""
    if not all([BOT_TOKEN, API_ID, API_HASH]):
        logger.critical("One or more critical environment variables are missing!")
        return

    # Start the Pyrogram client in the background.
    # .start() is non-blocking and lets the client listen for updates.
    await app.start()
    
    # Start the aiohttp web server to keep the Heroku dyno alive.
    webapp = web.Application()
    webapp.add_routes([web.get("/", lambda request: web.Response(text="OK"))])
    runner = web.AppRunner(webapp)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    logger.info(f"Starting keep-alive web server on port {PORT}")
    await site.start()

    logger.info("Bot is running!")
    # This keeps the script running indefinitely.
    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped.")
        
