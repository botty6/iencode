import os
import logging
import asyncio
import re
from collections import defaultdict
from celery import Celery
from pyrogram import Client, filters, enums
from pyrogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from database import get_user_settings, update_user_setting, add_job, get_job, remove_job, get_user_jobs

# --- Configuration & Initializations ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "").strip()
ADMIN_USER_IDS = [int(uid.strip()) for uid in os.getenv("ADMIN_USER_IDS", "").split(",") if uid.strip()]
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

if REDIS_URL.startswith("rediss://"):
    REDIS_URL = f"{REDIS_URL}?ssl_cert_reqs=CERT_NONE"

celery_producer = Celery('producer', broker=REDIS_URL)

pending_parts = defaultdict(lambda: {"message_ids": [], "timer": None})
user_states = {} 

app = Client("encoder_bot", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH, workdir="/tmp")

# --- Helper Functions ---
async def trigger_encode_job(user_id: int, original_message: Message):
    await asyncio.sleep(30) 
    user_data = pending_parts.get(user_id)
    if not user_data or not user_data["message_ids"]: return
    await original_message.reply_text( f"✅ Received {len(user_data['message_ids'])} parts. Choose quality:", reply_markup=create_new_job_keyboard(user_id))
    user_data["timer"] = None

def create_new_job_keyboard(identifier):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ 720p (Standard)", callback_data=f"encode|720|{identifier}|default")],
        [
            InlineKeyboardButton("🚀 1080p (Standard)", callback_data=f"encode|1080|{identifier}|default"),
            InlineKeyboardButton("💾 480p (Standard)", callback_data=f"encode|480|{identifier}|default"),
        ]
    ])

# --- Main Command Handlers ---
@app.on_message(filters.command("start") & filters.private)
async def start_command(client, message):
    if message.from_user.id in ADMIN_USER_IDS:
        await message.reply_text("👋 Hello! Send me a video to start.\n\nUse /queue to manage your jobs.\nUse /settings to customize your branding.")
    else:
        await message.reply_text("👋 Welcome!\nThis is a private bot. Contact the owner to get access.")

@app.on_message(filters.command("queue") & filters.private)
async def queue_command(client, message):
    user_id = message.from_user.id
    if user_id not in ADMIN_USER_IDS: return
    
    jobs = get_user_jobs(user_id)
    if not jobs:
        await message.reply_text("📂 Your queue is empty!")
        return
        
    keyboard = []
    queue_text = "📂 **Your Active Queue:**\n\n"
    for i, job in enumerate(jobs):
        queue_text += f"{i+1}️⃣ `{job['filename']}` → **{job['status']}**\n"
        if "Pending in default" in job.get('status', ''):
            keyboard.append([InlineKeyboardButton(f"⚡️ Accelerate Job #{i+1}", callback_data=f"accelerate|{job['task_id']}")])
    
    await message.reply_text(queue_text, reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None)

@app.on_message(filters.command("settings") & filters.private)
async def settings_command(client, message):
    user_id = message.from_user.id
    if user_id not in ADMIN_USER_IDS: return
    
    settings = get_user_settings(user_id)
    text = (
        "⚙️ **Your Settings**\n\n"
        f"**Brand Name:** `{settings.get('brand_name')}`\n"
        f"**Website/Channel:** `{settings.get('website')}`\n"
        f"**Custom Thumbnail:** `{'Set' if settings.get('custom_thumbnail_id') else 'Not Set'}`\n\n"
        "Use the buttons below to change your settings."
    )
    keyboard = [
        [InlineKeyboardButton("✏️ Set Brand Name", callback_data="set_setting|brand_name")],
        [InlineKeyboardButton("🔗 Set Website", callback_data="set_setting|website")],
        [InlineKeyboardButton("🖼 Set Thumbnail", callback_data="set_setting|custom_thumbnail_id")]
    ]
    await message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

@app.on_message(filters.command("cancel") & filters.private)
async def cancel_command(client, message):
    user_id = message.from_user.id
    if user_id in user_states:
        del user_states[user_id]
        await message.reply_text("Action canceled.")

# --- Main File Handler ---
@app.on_message((filters.video | filters.document | filters.photo) & filters.private)
async def main_file_handler(client, message: Message):
    user_id = message.from_user.id
    if user_id not in ADMIN_USER_IDS: return

    # Check if user is in a state to set a preference
    if user_states.get(user_id) == "set_custom_thumbnail_id":
        if message.photo:
            update_user_setting(user_id, "custom_thumbnail_id", message.photo.file_id)
            await message.reply_text("✅ Thumbnail updated successfully!")
            del user_states[user_id]
        else:
            await message.reply_text("That's not a photo. Please send an image to set as your thumbnail, or /cancel.")
        return
    
    # Otherwise, handle it as a new video file
    if message.video or message.document:
        await handle_video(client, message)

async def handle_video(client, message: Message):
    user_id = message.from_user.id
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
        await message.reply_text(f"🎬 Received: `{file_name}`\nPlease choose quality:", reply_markup=create_new_job_keyboard(message.id))


# --- Callback Handlers ---
@app.on_callback_query(filters.regex(r"^encode"))
async def button_callback(client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    if user_id not in ADMIN_USER_IDS: return

    action, quality, identifier, queue_type = callback_query.data.split("|", 3)
    
    message_ids, original_filename, thumbnail_file_id = [], "unknown.tmp", None
    try:
        if identifier.isdigit(): message_ids = [int(identifier)]
        else:
            user_data = pending_parts.get(int(identifier))
            if user_data: message_ids = sorted(user_data["message_ids"])
        
        if not message_ids: raise ValueError("Message IDs list is empty.")

        first_message = await client.get_messages(user_id, message_ids[0])
        file_meta = first_message.video or first_message.document
        original_filename = getattr(file_meta, "file_name", "unknown.tmp")
        if first_message.video and first_message.video.thumb:
            thumbnail_file_id = first_message.video.thumb.file_id
    except Exception as e:
        logger.error(f"Error getting file info: {e}")
        await callback_query.answer(f"Error getting file info. It might have been deleted.", show_alert=True)
        return

    user_settings = get_user_settings(user_id)
    status_message = await callback_query.message.edit_text(f"✅ Job accepted...")
    
    task_args = (user_id, status_message.id, message_ids, quality, thumbnail_file_id, user_settings)
    
    task = celery_producer.send_task("worker.tasks.encode_video_task", args=task_args, queue=queue_type)
    
    add_job(task.id, user_id, original_filename, f"🕒 Pending in {queue_type}", status_message.id, task_args)


@app.on_callback_query(filters.regex(r"^accelerate"))
async def accelerate_callback(client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    if user_id not in ADMIN_USER_IDS: return

    action, task_id_to_accelerate = callback_query.data.split("|", 1)
    
    job_to_accelerate = get_job(task_id_to_accelerate)
    if not job_to_accelerate:
        await callback_query.answer("Could not find this job.", show_alert=True)
        return
        
    new_status_message = callback_query.message
    await new_status_message.edit_text("✅ Re-routing to accelerator...")

    celery_producer.control.revoke(task_id_to_accelerate)
    
    original_task_args = job_to_accelerate["task_args"]
    new_task_args = (original_task_args[0], new_status_message.id, *original_task_args[2:])
    
    new_task = celery_producer.send_task("worker.tasks.encode_video_task", args=new_task_args, queue='high_priority')
    
    remove_job(task_id_to_accelerate)
    add_job(new_task.id, user_id, job_to_accelerate['filename'], "⚡️ Accelerated", new_status_message.id, new_task_args)
    
    try:
        await client.edit_message_text(user_id, job_to_accelerate['status_message_id'], "This job has been accelerated and is now tracked in a new message.")
    except Exception as e:
        logger.warning(f"Could not edit original status message: {e}")

@app.on_callback_query(filters.regex(r"^set_setting"))
async def set_setting_callback(client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    action, key = callback_query.data.split("|", 1)

    user_states[user_id] = key
    
    prompts = {
        "brand_name": "Please send your desired brand name (e.g., `MyEnc`).",
        "website": "Please send your website or channel link (e.g., `t.me/MyChannel`).",
        "custom_thumbnail_id": "Please send the photo you want to use as your permanent thumbnail."
    }
    await callback_query.message.reply_text(f"▶️ {prompts.get(key, 'Please send the new value.')}\n\nOr send /cancel to abort.")
    await callback_query.answer()

# --- CRITICAL FIX: Removed the broken ~filters.command() ---
@app.on_message(filters.text & filters.private)
async def handle_settings_text(client, message):
    user_id = message.from_user.id
    state = user_states.get(user_id)
    # This handler will not trigger for commands because the command handlers above have priority.
    if state in ["brand_name", "website"]:
        update_user_setting(user_id, state, message.text)
        await message.reply_text(f"✅ `{state.replace('_', ' ').title()}` has been updated to: `{message.text}`")
        del user_states[user_id]
        
# --- Main Entrypoint ---
if __name__ == "__main__":
    if not all([BOT_TOKEN, API_ID, API_HASH, ADMIN_USER_IDS]):
        logger.critical("CRITICAL: One or more environment variables are missing!")
    else:
        logger.info("Bot is starting...")
        app.run()
