# iencode-main/bot/bot.py

import os
import logging
import asyncio
import re
from collections import defaultdict
from celery import Celery, chain
from pyrogram import Client, filters
from pyrogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
# --- REFACTORED: Import from top-level database.py ---
import database
from worker.tasks import download_task, encode_task

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

# --- Keyboard Helper Functions ---
def create_quality_keyboard(identifier):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💎 1080p (Full HD)", callback_data=f"quality|1080|{identifier}")],
        [InlineKeyboardButton("✅ 720p (Standard)", callback_data=f"quality|720|{identifier}")],
        [InlineKeyboardButton("💾 480p (Basic)", callback_data=f"quality|480|{identifier}")]
    ])

def create_preset_keyboard(quality, identifier):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Fast (Good)", callback_data=f"encode|{quality}|fast|{identifier}")],
        [InlineKeyboardButton("⚖️ Medium (Great)", callback_data=f"encode|{quality}|medium|{identifier}")],
        [InlineKeyboardButton("🐌 Slow (Best)", callback_data=f"encode|{quality}|slow|{identifier}")]
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
    
    jobs = database.get_user_jobs(user_id)
    if not jobs:
        await message.reply_text("📂 Your queue is empty!")
        return
        
    keyboard = []
    queue_text = "📂 **Your Active Queue:**\n\n"
    for i, job in enumerate(jobs):
        job_text = f"{i+1}️⃣ `{job['filename']}` → **{job['status']}**"
        buttons = []
        if job.get('job_data', {}).get('cpu_queue') == 'default':
            buttons.append(InlineKeyboardButton(f"⚡️ Accelerate", callback_data=f"accelerate|{job['task_id']}"))
        
        buttons.append(InlineKeyboardButton(f"❌ Cancel", callback_data=f"cancel|{job['task_id']}"))
        queue_text += job_text + "\n"
        keyboard.append(buttons)
    
    await message.reply_text(queue_text, reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None)

@app.on_message(filters.command("settings") & filters.private)
async def settings_command(client, message):
    user_id = message.from_user.id
    if user_id not in ADMIN_USER_IDS: return
    
    settings = database.get_user_settings(user_id)
    text = (f"⚙️ **Your Settings**\n\n"
            f"**Brand Name:** `{settings.get('brand_name')}`\n"
            f"**Website/Channel:** `{settings.get('website')}`\n"
            f"**Custom Thumbnail:** `{'Set' if settings.get('custom_thumbnail_id') else 'Not Set'}`\n\n"
            "Use the buttons below to change your settings.")
    keyboard = [[InlineKeyboardButton("✏️ Set Brand Name", callback_data="set_setting|brand_name")],
                [InlineKeyboardButton("🔗 Set Website", callback_data="set_setting|website")],
                [InlineKeyboardButton("🖼 Set Thumbnail", callback_data="set_setting|custom_thumbnail_id")]]
    await message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

@app.on_message(filters.command("cancel") & filters.private)
async def cancel_command(client, message):
    user_id = message.from_user.id
    if user_id in user_states:
        del user_states[user_id]
        await message.reply_text("Action canceled.")

# --- Main File Handling Logic ---
async def trigger_encode_job(user_id: int, original_message: Message):
    await asyncio.sleep(30) 
    user_data = pending_parts.get(user_id)
    if not user_data or not user_data["message_ids"]: return
    await original_message.reply_text(f"✅ Received {len(user_data['message_ids'])} parts. **Step 1: Choose Quality**",
                                      reply_markup=create_quality_keyboard(user_id))
    user_data["timer"] = None

@app.on_message((filters.video | filters.document | filters.photo) & filters.private)
async def main_file_handler(client, message: Message):
    user_id = message.from_user.id
    if user_id not in ADMIN_USER_IDS: return

    if user_states.get(user_id) == "set_custom_thumbnail_id":
        if message.photo:
            database.update_user_setting(user_id, "custom_thumbnail_id", message.photo.file_id)
            await message.reply_text("✅ Thumbnail updated successfully!")
            del user_states[user_id]
        else:
            await message.reply_text("That's not a photo. Please send an image or /cancel.")
        return
    
    if message.video or message.document:
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
            await message.reply_text(f"🎬 Received: `{file_name}`\n\n**Step 1: Choose Quality**",
                                     reply_markup=create_quality_keyboard(message.id))

# --- Callback Query Handlers ---
def start_encode_pipeline(job_data: dict):
    pipeline = chain(
        download_task.s(
            user_id=job_data['user_id'],
            status_message_id=job_data['status_message_id'],
            list_of_message_ids=job_data['message_ids'],
            quality=job_data['quality'],
            preset=job_data['preset'],
            user_settings=job_data['user_settings']
        ).set(queue='io_queue'),
        encode_task.s().set(queue=job_data['cpu_queue'])
    )
    return pipeline.apply_async()

@app.on_callback_query(filters.regex(r"^(quality|encode|accelerate|cancel|set_setting)"))
async def callback_router(client, callback_query: CallbackQuery):
    action = callback_query.data.split("|")[0]
    handlers = {"quality": quality_callback, "encode": encode_callback, "accelerate": accelerate_callback,
                "cancel": cancel_callback, "set_setting": set_setting_callback}
    handler = handlers.get(action)
    if handler: await handler(client, callback_query)

async def quality_callback(client, callback_query: CallbackQuery):
    action, quality, identifier = callback_query.data.split("|")
    await callback_query.message.edit_text(f"✅ Quality set to **{quality}p**.\n\n**Step 2: Choose Encode Preset**",
                                            reply_markup=create_preset_keyboard(quality, identifier))

async def encode_callback(client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    try:
        action, quality, preset, identifier = callback_query.data.split("|", 3)
    except ValueError:
        await callback_query.answer("Error: Invalid callback data.", show_alert=True)
        return

    message_ids = []
    try:
        if identifier.isdigit():
            message_ids = [int(identifier)]
        else:
            user_data = pending_parts.get(int(identifier))
            if user_data: message_ids = sorted(user_data["message_ids"])
        
        if not message_ids: raise ValueError("Message IDs list is empty. The job may have timed out.")
        first_message = await client.get_messages(user_id, message_ids[0])
        original_filename = getattr(first_message.video or first_message.document, "file_name", "unknown.tmp")
    except Exception as e:
        logger.error(f"Error preparing job info: {e}")
        await callback_query.message.edit_text(f"💥 **Error:** Could not retrieve file information. Please try again.")
        return

    status_message = await callback_query.message.edit_text(f"✅ Job for `{original_filename}` has been queued!")
    job_data = {"user_id": user_id, "status_message_id": status_message.id, "message_ids": message_ids,
                "quality": quality, "preset": preset, "cpu_queue": "default",
                "user_settings": database.get_user_settings(user_id)}
    
    result = start_encode_pipeline(job_data)
    database.add_job(result.id, user_id, original_filename, status_message.id, job_data)
    
    if not identifier.isdigit(): del pending_parts[int(identifier)]

async def accelerate_callback(client, callback_query: CallbackQuery):
    await callback_query.answer("⚡️ Accelerating job...", show_alert=False)
    user_id = callback_query.from_user.id
    action, task_id = callback_query.data.split("|", 1)
    job = database.get_job(task_id)
    if not job or job['user_id'] != user_id:
        await callback_query.message.edit_text("Could not find this job or you don't own it.")
        return
    celery_producer.control.revoke(task_id, terminate=False)
    job_data = job['job_data']
    job_data['cpu_queue'] = 'high_priority'
    new_result = start_encode_pipeline(job_data)
    database.remove_job(task_id)
    database.add_job(new_result.id, user_id, job['filename'], job['status_message_id'], job_data)
    await callback_query.message.edit_text("✅ Job has been moved to the high-priority accelerator queue!")

async def cancel_callback(client, callback_query: CallbackQuery):
    await callback_query.answer("❌ Cancelling job...", show_alert=False)
    user_id = callback_query.from_user.id
    action, task_id = callback_query.data.split("|", 1)
    job = database.get_job(task_id)
    if not job or job['user_id'] != user_id:
        await callback_query.message.edit_text("Could not find this job or you don't own it.")
        return
    celery_producer.control.revoke(task_id, terminate=True, signal='SIGKILL')
    database.update_job_status(task_id, "CANCELLED")
    await callback_query.message.edit_text(f"✅ Job for `{job['filename']}` has been cancelled.")
    try:
        await client.edit_message_text(user_id, job['status_message_id'], "❌ Job Cancelled by User.")
    except Exception as e:
        logger.warning(f"Could not edit original status message: {e}")

async def set_setting_callback(client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    action, key = callback_query.data.split("|", 1)
    user_states[user_id] = key
    prompts = {"brand_name": "Please send your brand name.", "website": "Please send your website link.",
               "custom_thumbnail_id": "Please send a photo."}
    await callback_query.message.reply_text(f"▶️ {prompts.get(key, 'Please send new value.')}\n\nOr send /cancel.")
    await callback_query.answer()

@app.on_message(filters.text & filters.private)
async def handle_settings_text(client, message):
    user_id = message.from_user.id
    state = user_states.get(user_id)
    if state in ["brand_name", "website"]:
        database.update_user_setting(user_id, state, message.text)
        await message.reply_text(f"✅ `{state.replace('_', ' ').title()}` updated successfully.")
        del user_states[user_id]

if __name__ == "__main__":
    if not all([BOT_TOKEN, API_ID, API_HASH, ADMIN_USER_IDS]):
        logger.critical("CRITICAL: One or more required environment variables are missing!")
    else:
        logger.info("Bot is starting...")
        app.run()
    
