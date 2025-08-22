import os
import logging
import asyncio
import re
from collections import defaultdict
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

# --- State Management ---
pending_parts = defaultdict(lambda: {"message_ids": [], "timer": None})
active_jobs = defaultdict(dict)


# --- Pyrogram Client Initialization ---
app = Client("encoder_bot", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH, workdir="/tmp")

async def trigger_encode_job(user_id: int, original_message: Message):
    await asyncio.sleep(30) 

    user_data = pending_parts.get(user_id)
    if not user_data or not user_data["message_ids"]:
        return

    file_count = len(user_data["message_ids"])
    await original_message.reply_text(
        f"✅ Received {file_count} parts. Please choose an output quality to merge and encode:",
        reply_markup=create_quality_keyboard(user_id)
    )
    user_data["timer"] = None

def create_quality_keyboard(identifier):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ 720p (Standard)", callback_data=f"encode|720|{identifier}|default")],
        [
            InlineKeyboardButton("🚀 1080p (Standard)", callback_data=f"encode|1080|{identifier}|default"),
            InlineKeyboardButton("💾 480p (Standard)", callback_data=f"encode|480|{identifier}|default"),
        ],
        [InlineKeyboardButton("⚡️ Accelerate This Job (720p High Priority)", callback_data=f"encode|720|{identifier}|high_priority")]
    ])

# --- Handlers ---
@app.on_message(filters.command("start") & filters.private)
async def start_command(client, message):
    if message.from_user.id in ADMIN_USER_IDS:
        await message.reply_text("👋 Hello! Send me a video file or split parts to get started.\nYou can use /queue to see your active jobs.")
    else:
        await message.reply_text("👋 Welcome!\nThis is a private bot and you are not authorized to use it.")

@app.on_message(filters.command("queue") & filters.private)
async def queue_command(client, message):
    user_id = message.from_user.id
    if user_id not in ADMIN_USER_IDS:
        return
    
    jobs = active_jobs.get(user_id)
    if not jobs:
        await message.reply_text("📂 Your queue is empty!")
        return
        
    queue_text = "📂 **Your Active Queue:**\n\n"
    for i, (msg_id, job) in enumerate(jobs.items()):
        queue_text += f"{i+1}️⃣ `{job['filename']}` → **{job['status']}**\n"
        
    await message.reply_text(queue_text)

@app.on_message((filters.video | filters.document) & filters.private)
async def handle_video(client, message: Message):
    user_id = message.from_user.id
    if user_id not in ADMIN_USER_IDS:
        return
        
    file = message.video or message.document
    file_name = getattr(file, "file_name", "unknown_file.tmp")
    
    is_split_file = re.search(r'\.(part\d+|\d{3})$', file_name, re.IGNORECASE)

    if is_split_file:
        user_data = pending_parts[user_id]
        if user_data["timer"]:
            user_data["timer"].cancel()
        user_data["message_ids"].append(message.id)
        
        await message.reply_text(
            f"👍 Part `{file_name}` collected. Total: {len(user_data['message_ids'])}. Waiting 30s for more parts.",
            quote=True
        )
        user_data["timer"] = asyncio.create_task(trigger_encode_job(user_id, message))
    else:
        keyboard = create_quality_keyboard(message.id)
        await message.reply_text(f"🎬 Received: `{file_name}`\nPlease choose an output quality:", reply_markup=keyboard)


@app.on_callback_query(filters.regex(r"^encode"))
async def button_callback(client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    if user_id not in ADMIN_USER_IDS:
        await callback_query.answer("🚫 You are not authorized.", show_alert=True)
        return
        
    action, quality, identifier, queue_type = callback_query.data.split("|", 3)
    
    message_ids = []
    job_type = ""
    
    try:
        message_ids = [int(identifier)]
        job_type = "Single file"
    except ValueError:
        user_data = pending_parts.get(int(identifier))
        if user_data and user_data["message_ids"]:
            message_ids = sorted(user_data["message_ids"])
            job_type = f"{len(message_ids)}-part job"
            del pending_parts[int(identifier)]
        else:
            await callback_query.answer("Error: Could not find job parts.", show_alert=True)
            return

    if not message_ids:
        await callback_query.answer("Error: No files found.", show_alert=True)
        return

    thumbnail_file_id = None
    original_filename = "unknown_file.tmp"
    try:
        first_message = await client.get_messages(user_id, message_ids[0])
        file_meta = first_message.video or first_message.document
        original_filename = getattr(file_meta, "file_name", "unknown_file.tmp")
        if first_message.video and first_message.video.thumb:
            thumbnail_file_id = first_message.video.thumb.file_id
    except Exception as e:
        logger.warning(f"Could not retrieve message details for {message_ids[0]}: {e}")

    status_message = await callback_query.message.edit_text(f"✅ Job accepted. Sending to the **{queue_type.replace('_', ' ')}** queue...")
    
    active_jobs[user_id][status_message.id] = {"filename": original_filename, "status": f"🕒 Pending in {queue_type.replace('_', ' ')}"}
    
    encode_video_task.apply_async(
        args=[user_id, status_message.id, message_ids, quality, thumbnail_file_id],
        kwargs={},
        queue=queue_type
    )

# --- Simplified Main Entrypoint ---
if __name__ == "__main__":
    if not all([BOT_TOKEN, API_ID, API_HASH, ADMIN_USER_IDS]):
        logger.critical("CRITICAL: One or more environment variables are missing!")
    else:
        logger.info("Bot is starting...")
        app.run()
        logger.info("Bot has stopped.")
