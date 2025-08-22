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
# UPDATED: Now stores task_id, original args, and more for re-queuing
active_jobs = defaultdict(dict)


# --- Pyrogram Client Initialization ---
app = Client("encoder_bot", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH, workdir="/tmp")

# ... (trigger_encode_job and create_quality_keyboard remain the same as the last complete version) ...
async def trigger_encode_job(user_id: int, original_message: Message):
    await asyncio.sleep(30) 
    user_data = pending_parts.get(user_id)
    if not user_data or not user_data["message_ids"]: return
    await original_message.reply_text( f"✅ Received {len(user_data['message_ids'])} parts. Choose quality:", reply_markup=create_quality_keyboard(user_id))
    user_data["timer"] = None

def create_quality_keyboard(identifier):
    # This is now only for NEW files, not for acceleration
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ 720p (Standard)", callback_data=f"encode|720|{identifier}|default")],
        [
            InlineKeyboardButton("🚀 1080p (Standard)", callback_data=f"encode|1080|{identifier}|default"),
            InlineKeyboardButton("💾 480p (Standard)", callback_data=f"encode|480|{identifier}|default"),
        ]
    ])

# --- Handlers ---
@app.on_message(filters.command("start") & filters.private)
async def start_command(client, message):
    if message.from_user.id in ADMIN_USER_IDS:
        await message.reply_text("👋 Hello! Send a video to start. Use /queue to manage your jobs.")
    else:
        await message.reply_text("👋 Welcome!\nThis is a private bot and you are not authorized to use it.")

@app.on_message(filters.command("queue") & filters.private)
async def queue_command(client, message):
    user_id = message.from_user.id
    if user_id not in ADMIN_USER_IDS: return
    
    jobs = active_jobs.get(user_id)
    if not jobs:
        await message.reply_text("📂 Your queue is empty!")
        return
        
    keyboard = []
    queue_text = "📂 **Your Active Queue:**\n\n"
    for i, (msg_id, job) in enumerate(jobs.items()):
        queue_text += f"{i+1}️⃣ `{job['filename']}` → **{job['status']}**\n"
        # --- NEW: Add accelerate button only for pending jobs in the default queue ---
        if job['status'] == "🕒 Pending in default":
            keyboard.append([InlineKeyboardButton(f"⚡️ Accelerate Job #{i+1}", callback_data=f"accelerate|{job['task_id']}")])
    
    await message.reply_text(queue_text, reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None)

# ... (handle_video remains the same as the last complete version) ...
@app.on_message((filters.video | filters.document) & filters.private)
async def handle_video(client, message: Message):
    user_id = message.from_user.id
    if user_id not in ADMIN_USER_IDS: return
    file = message.video or message.document
    file_name = getattr(file, "file_name", "unknown_file.tmp")
    is_split_file = re.search(r'\.(part\d+|\d{3})$', file_name, re.IGNORECASE)
    if is_split_file:
        user_data = pending_parts[user_id]
        if user_data["timer"]: user_data["timer"].cancel()
        user_data["message_ids"].append(message.id)
        await message.reply_text(f"👍 Part `{file_name}` collected. Total: {len(user_data['message_ids'])}.", quote=True)
        user_data["timer"] = asyncio.create_task(trigger_encode_job(user_id, message))
    else:
        await message.reply_text(f"🎬 Received: `{file_name}`\nPlease choose quality:", reply_markup=create_quality_keyboard(message.id))


@app.on_callback_query(filters.regex(r"^encode"))
async def button_callback(client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    if user_id not in ADMIN_USER_IDS:
        await callback_query.answer("🚫 You are not authorized.", show_alert=True)
        return
        
    action, quality, identifier, queue_type = callback_query.data.split("|", 3)
    
    # ... (message ID and file info retrieval logic is the same) ...
    message_ids, original_filename, thumbnail_file_id = [], "unknown.tmp", None
    try:
        if identifier.isdigit():
            message_ids = [int(identifier)]
        else:
            user_data = pending_parts.get(int(identifier))
            if user_data: message_ids = sorted(user_data["message_ids"])
        first_message = await client.get_messages(user_id, message_ids[0])
        file_meta = first_message.video or first_message.document
        original_filename = getattr(file_meta, "file_name", "unknown_file.tmp")
        if first_message.video and first_message.video.thumb:
            thumbnail_file_id = first_message.video.thumb.file_id
    except Exception as e:
        await callback_query.answer(f"Error getting file info: {e}", show_alert=True)
        return

    status_message = await callback_query.message.edit_text(f"✅ Job accepted. Sending to the **{queue_type.replace('_', ' ')}** queue...")
    
    task_args = [user_id, status_message.id, message_ids, quality, thumbnail_file_id]
    
    # --- NEW: Store the task_id ---
    task = encode_video_task.apply_async(args=task_args, kwargs={}, queue=queue_type)
    
    active_jobs[user_id][status_message.id] = {
        "filename": original_filename,
        "status": f"🕒 Pending in {queue_type.replace('_', ' ')}",
        "task_id": task.id,
        "task_args": task_args # Store args for re-queuing
    }

# --- NEW: Handler for the accelerate button ---
@app.on_callback_query(filters.regex(r"^accelerate"))
async def accelerate_callback(client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    if user_id not in ADMIN_USER_IDS:
        await callback_query.answer("🚫 You are not authorized.", show_alert=True)
        return

    action, task_id_to_accelerate = callback_query.data.split("|", 1)
    
    job_to_accelerate = None
    original_msg_id = None
    
    # Find the job in our active_jobs dictionary
    for msg_id, job in active_jobs.get(user_id, {}).items():
        if job.get("task_id") == task_id_to_accelerate:
            job_to_accelerate = job
            original_msg_id = msg_id
            break
            
    if not job_to_accelerate:
        await callback_query.answer("Could not find this job. It might have already started.", show_alert=True)
        return
        
    await callback_query.message.edit_text("✅ Found job! Accelerating now...")

    # 1. Revoke the original task
    encode_video_task.AsyncResult(task_id_to_accelerate).revoke()
    
    # 2. Re-submit the task to the high_priority queue
    new_task = encode_video_task.apply_async(
        args=job_to_accelerate["task_args"],
        kwargs={},
        queue='high_priority'
    )
    
    # 3. Update the job in our dictionary
    job_to_accelerate['status'] = "⚡️ Accelerated"
    job_to_accelerate['task_id'] = new_task.id
    
    await callback_query.message.edit_text(f"🚀 Job for `{job_to_accelerate['filename']}` has been moved to the high-priority queue!")


# --- Main Entrypoint ---
if __name__ == "__main__":
    if not all([BOT_TOKEN, API_ID, API_HASH, ADMIN_USER_IDS]):
        logger.critical("CRITICAL: One or more environment variables are missing!")
    else:
        logger.info("Bot is starting...")
        app.run()
