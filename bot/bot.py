import os
import logging
import asyncio
import re
from collections import defaultdict
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

# --- State Management for Multi-part Uploads ---
# This will store message IDs for users who are uploading split files.
# Format: {user_id: {"message_ids": [id1, id2, ...], "timer": asyncio.Task}}
pending_parts = defaultdict(lambda: {"message_ids": [], "timer": None})


# --- Pyrogram Client Initialization ---
app = Client("encoder_bot", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH, workdir="/tmp")

async def trigger_encode_job(user_id: int, original_message: Message):
    """
    Waits for a timeout, then gathers all parts and presents the encoding options.
    """
    await asyncio.sleep(30) # Wait 30 seconds for more parts

    user_data = pending_parts.get(user_id)
    if not user_data or not user_data["message_ids"]:
        return # Should not happen, but a good safeguard

    file_count = len(user_data["message_ids"])
    await original_message.reply_text(
        f"✅ Received {file_count} parts. Please choose an output quality to merge and encode:",
        reply_markup=create_quality_keyboard(user_id)
    )
    # The job is now ready, but we wait for the user to press a button.
    # The timer's job is done.
    user_data["timer"] = None

def create_quality_keyboard(identifier):
    """Helper to create the keyboard, using user_id for multi-part or message_id for single files."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ 720p", callback_data=f"encode|720|{identifier}")],
        [
            InlineKeyboardButton("🚀 1080p", callback_data=f"encode|1080|{identifier}"),
            InlineKeyboardButton("💾 480p", callback_data=f"encode|480|{identifier}"),
        ]
    ])

# --- Handlers ---
@app.on_message(filters.command("start") & filters.private)
async def start_command(client, message):
    if message.from_user.id in ADMIN_USER_IDS:
        await message.reply_text("👋 Hello! Send me a video file or split parts to get started.")
    else:
        await message.reply_text("👋 Welcome!\nThis is a private bot and you are not authorized to use it.")

@app.on_message((filters.video | filters.document) & filters.private)
async def handle_video(client, message: Message):
    user_id = message.from_user.id
    if user_id not in ADMIN_USER_IDS:
        return
        
    file = message.video or message.document
    file_name = getattr(file, "file_name", "unknown_file.tmp")
    
    # --- NEW: Multi-part file detection logic ---
    # Regex to detect common split file patterns (e.g., .001, .part1, .part01)
    is_split_file = re.search(r'\.(part\d+|\d{3})$', file_name, re.IGNORECASE)

    if is_split_file:
        user_data = pending_parts[user_id]
        
        # Cancel any existing timer, as a new part has arrived
        if user_data["timer"]:
            user_data["timer"].cancel()
            
        # Add the new message ID to the list
        user_data["message_ids"].append(message.id)
        
        await message.reply_text(
            f"👍 Received part: `{file_name}`\n"
            f"Total parts collected: {len(user_data['message_ids'])}. "
            f"I will wait 30 seconds for the next part before asking to encode.",
            quote=True
        )
        
        # Start a new timer
        user_data["timer"] = asyncio.create_task(trigger_encode_job(user_id, message))
    else:
        # This is a single, non-split file
        keyboard = create_quality_keyboard(message.id)
        await message.reply_text(f"🎬 Received file: `{file_name}`\nPlease choose an output quality:", reply_markup=keyboard)


@app.on_callback_query(filters.regex(r"^encode"))
async def button_callback(client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    if user_id not in ADMIN_USER_IDS:
        await callback_query.answer("🚫 You are not authorized.", show_alert=True)
        return
        
    action, quality, identifier = callback_query.data.split("|", 2)
    
    message_ids = []
    
    # --- NEW: Differentiate between single file and multi-part jobs ---
    try:
        # If identifier is a number, it's a single message ID
        message_ids = [int(identifier)]
        job_type = "Single file"
    except ValueError:
        # If it's not a number, it must be the user_id for a multi-part job
        user_data = pending_parts.get(int(identifier))
        if user_data and user_data["message_ids"]:
            message_ids = sorted(user_data["message_ids"]) # Sort to ensure correct order
            job_type = f"{len(message_ids)}-part job"
            # Clear the pending parts for this user as the job is now being queued
            del pending_parts[int(identifier)]
        else:
            await callback_query.answer("Error: Could not find the file parts for this job.", show_alert=True)
            return

    if not message_ids:
        await callback_query.answer("Error: No files found for this job.", show_alert=True)
        return

    await callback_query.answer("✅ Job sent to queue!")
    await callback_query.message.edit_text(f"⏳ Your {job_type} is now in the queue for a {quality}p encode...")
    
    encode_video_task.delay(
        user_chat_id=callback_query.message.chat.id,
        list_of_message_ids=message_ids,
        quality=quality,
    )

# --- Main Entrypoint ---
async def main():
    if not all([BOT_TOKEN, API_ID, API_HASH]):
        logger.critical("One or more critical environment variables are missing!")
        return

    await app.start()
    
    webapp = web.Application()
    webapp.add_routes([web.get("/", lambda request: web.Response(text="OK"))])
    runner = web.AppRunner(webapp)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    logger.info(f"Starting keep-alive web server on port {PORT}")
    await site.start()

    logger.info("Bot is running!")
    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped.")
