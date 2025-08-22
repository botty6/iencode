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
import database
from worker.tasks import download_task, encode_task
from worker.utils import generate_standard_filename, get_video_info # NEW: Import utils

# --- Configuration & Initializations ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "").strip()
ADMIN_USER_IDS = [int(uid.strip()) for uid in os.getenv("ADMIN_USER_IDS", "").split(",") if uid.strip()]
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
THUMBNAIL_LOG_CHANNEL_ID = int(os.getenv("THUMBNAIL_LOG_CHANNEL_ID", "0"))

if REDIS_URL.startswith("rediss://"):
    REDIS_URL = f"{REDIS_URL}?ssl_cert_reqs=CERT_NONE"

celery_producer = Celery('producer', broker=REDIS_URL)
pending_parts = defaultdict(lambda: {"message_ids": [], "timer": None})
user_states = {} 
app = Client("encoder_bot", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH, workdir="/tmp")

# --- Keyboards ---
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

def create_filename_keyboard(quality, preset, identifier):
    """NEW: Keyboard for confirming or editing the filename."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Confirm and Start", callback_data=f"confirm_name|{quality}|{preset}|{identifier}")],
        [InlineKeyboardButton("✏️ Edit Filename", callback_data=f"edit_name|{quality}|{preset}|{identifier}")]
    ])

# --- Main Command Handlers & Queue Logic ---
@app.on_message(filters.command("start") & filters.private)
async def start_command(client, message):
    if message.from_user.id in ADMIN_USER_IDS:
        await message.reply_text("👋 Hello! Send me a video to start.\n\nUse /queue to manage your jobs.\nUse /settings to customize your branding.")
    else:
        await message.reply_text("👋 Welcome!\nThis is a private bot. Contact the owner to get access.")

async def show_queue(message_or_callback_query):
    user_id = message_or_callback_query.from_user.id
    jobs = database.get_user_jobs(user_id)
    if not jobs:
        text, keyboard = "📂 Your queue is empty!", None
    else:
        text, keyboard = "📂 **Your Active Queue:**\n\n", []
        for i, job in enumerate(jobs):
            job_number = i + 1
            text += f"**{job_number}️⃣ `{job['filename']}`**\n       Status: `{job['status']}`\n"
            keyboard.append([InlineKeyboardButton(f"⚙️ Manage Job #{job_number}", callback_data=f"manage|{job['task_id']}")])
        keyboard.append([InlineKeyboardButton("🗑️ Cancel All Jobs", callback_data="cancel_all|user")])
    
    if isinstance(message_or_callback_query, CallbackQuery):
        await message_or_callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None)
    else:
        await message_or_callback_query.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None)

@app.on_message(filters.command("queue") & filters.private)
async def queue_command(client, message):
    if message.from_user.id in ADMIN_USER_IDS: await show_queue(message)

@app.on_message(filters.command("settings") & filters.private)
async def settings_command(client, message):
    user_id = message.from_user.id
    if user_id not in ADMIN_USER_IDS: return
    
    settings = database.get_user_settings(user_id)
    text = (f"⚙️ **Your Settings**\n\n"
            f"**Brand Name:** `{settings.get('brand_name')}`\n"
            f"**Website/Channel:** `{settings.get('website')}`\n"
            f"**Custom Thumbnail:** `{'Set' if settings.get('custom_thumbnail_message_id') else 'Not Set'}`\n\n"
            "Use the buttons below to change your settings.")
    keyboard = [[InlineKeyboardButton("✏️ Set Brand Name", callback_data="set_setting|brand_name")],
                [InlineKeyboardButton("🔗 Set Website", callback_data="set_setting|website")],
                [InlineKeyboardButton("🖼 Set Thumbnail", callback_data="set_setting|custom_thumbnail_message_id")]]
    await message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

@app.on_message(filters.command("cancel") & filters.private)
async def cancel_command_from_user(client, message):
    if message.from_user.id in user_states:
        del user_states[message.from_user.id]
        await message.reply_text("Action canceled.")
        
# --- File Handling & Job Creation Workflow ---
@app.on_message((filters.video | filters.document | filters.photo | filters.text) & filters.private)
async def universal_message_handler(client, message: Message):
    user_id = message.from_user.id
    if user_id not in ADMIN_USER_IDS: return

    # State: Waiting for a photo to set as a custom thumbnail
    if user_states.get(user_id) == "custom_thumbnail_message_id":
        if not THUMBNAIL_LOG_CHANNEL_ID:
            await message.reply_text("❌ **Error:** `THUMBNAIL_LOG_CHANNEL_ID` is not set.")
            del user_states[user_id]
            return
        if message.photo:
            try:
                log_message = await message.forward(THUMBNAIL_LOG_CHANNEL_ID)
                database.update_user_setting(user_id, "custom_thumbnail_message_id", log_message.id)
                await message.reply_text("✅ Thumbnail updated successfully!")
            except Exception as e:
                await message.reply_text(f"❌ Could not save thumbnail. Error: {e}")
            finally: del user_states[user_id]
        else: await message.reply_text("That's not a photo. Please send an image or /cancel.")
        return
    
    # State: Waiting for text to set a setting (brand name or website)
    if isinstance(user_states.get(user_id), str) and user_states[user_id] in ["brand_name", "website"]:
        state = user_states[user_id]
        database.update_user_setting(user_id, state, message.text)
        await message.reply_text(f"✅ `{state.replace('_', ' ').title()}` updated successfully.")
        del user_states[user_id]
        return

    # State: Waiting for text to be used as a new filename
    if isinstance(user_states.get(user_id), dict) and user_states[user_id].get("state") == "set_filename":
        job_data = user_states[user_id]["job_data"]
        settings = database.get_user_settings(user_id)
        brand_name = settings.get("brand_name", "MyEnc")
        
        # Re-generate the full filename with the new user-provided base name
        final_filename_with_props = generate_standard_filename(
            message.text, # User's new filename base
            job_data["quality"], 
            brand_name, 
            job_data["video_info"]
        )
        job_data["final_filename"] = final_filename_with_props
        
        status_message = await message.reply_text(f"✅ Filename updated. Job for `{final_filename_with_props}` is starting!")
        job_data["status_message_id"] = status_message.id
        
        result = start_encode_pipeline(job_data)
        database.add_job(result.id, user_id, final_filename_with_props, status_message.id, job_data)
        del user_states[user_id]
        return
    
    # Standard case: Receiving a video/document file
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


def start_encode_pipeline(job_data: dict):
    pipeline = chain(
        download_task.s(
            user_id=job_data['user_id'],
            status_message_id=job_data['status_message_id'],
            list_of_message_ids=job_data['message_ids'],
            quality=job_data['quality'],
            preset=job_data['preset'],
            final_filename=job_data['final_filename'],
            original_thumbnail_id=job_data['original_thumbnail_id'],
            user_settings=job_data['user_settings']
        ).set(queue='io_queue'),
        encode_task.s().set(queue=job_data['cpu_queue'])
    )
    return pipeline.apply_async()

@app.on_callback_query(filters.regex(r"^(quality|encode|confirm_name|edit_name|manage|accelerate|cancel|cancel_all|set_setting|queue)"))
async def callback_router(client, callback_query: CallbackQuery):
    action = callback_query.data.split("|")[0]
    handlers = {
        "quality": quality_callback, 
        "encode": encode_callback,
        "confirm_name": confirm_filename_callback,
        "edit_name": edit_filename_callback,
        "manage": manage_job_callback,
        "accelerate": accelerate_callback, 
        "cancel": cancel_callback, 
        "cancel_all": cancel_all_callback, 
        "set_setting": set_setting_callback,
        "queue": lambda c, cb: show_queue(cb)
    }
    handler = handlers.get(action)
    if handler: await handler(client, callback_query)

async def quality_callback(client, callback_query: CallbackQuery):
    action, quality, identifier = callback_query.data.split("|")
    await callback_query.message.edit_text(f"✅ Quality set to **{quality}p**.\n\n**Step 2: Choose Encode Preset**",
                                            reply_markup=create_preset_keyboard(quality, identifier))

async def encode_callback(client, callback_query: CallbackQuery):
    """Step 2: Handles preset selection and generates the proposed filename."""
    user_id = callback_query.from_user.id
    try:
        action, quality, preset, identifier = callback_query.data.split("|", 3)
    except ValueError:
        await callback_query.answer("Error: Invalid callback data.", show_alert=True)
        return

    temp_msg = await callback_query.message.edit_text("⏳ Analyzing video to generate filename...")
    try:
        message_ids = []
        if identifier.isdigit():
            message_ids = [int(identifier)]
        else:
            user_data = pending_parts.get(int(identifier))
            if user_data: message_ids = sorted(user_data["message_ids"])
        
        if not message_ids: raise ValueError("Message IDs list is empty.")
        
        first_message = await client.get_messages(user_id, message_ids[0])
        original_filename = getattr(first_message.video or first_message.document, "file_name", "unknown.tmp")
        
        temp_dl_path = await client.download_media(first_message, file_name=f"/tmp/{first_message.id}_temp_analyze")
        video_info = get_video_info(temp_dl_path)
        os.remove(temp_dl_path)
        
        if not video_info: raise ValueError("Could not analyze video properties.")
        
        settings = database.get_user_settings(user_id)
        brand_name = settings.get("brand_name", "MyEnc")
        generated_filename = generate_standard_filename(original_filename, quality, brand_name, video_info)
        
        user_states[user_id] = {
            "state": "confirm_filename",
            "job_data": {
                "user_id": user_id, "message_ids": message_ids, "quality": quality,
                "preset": preset, "final_filename": generated_filename, "video_info": video_info,
                "original_thumbnail_id": first_message.video.thumb.file_id if first_message.video and first_message.video.thumb else None,
                "user_settings": settings
            }
        }
        await temp_msg.edit_text(
            f"**Step 3: Confirm Filename**\n\nGenerated filename:\n`{generated_filename}`",
            reply_markup=create_filename_keyboard(quality, preset, identifier)
        )
    except Exception as e:
        logger.error(f"Error in pre-analysis: {e}")
        await temp_msg.edit_text(f"💥 **Error:** Could not analyze the video to generate a filename.")
        if not identifier.isdigit(): del pending_parts[int(identifier)]


async def edit_filename_callback(client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    if user_states.get(user_id) and user_states[user_id].get("state") == "confirm_filename":
        user_states[user_id]["state"] = "set_filename"
        await callback_query.message.edit_text("✍️ OK, send me the new base filename.\n\nI will still add the correct properties. Send /cancel to abort.")
    else:
        await callback_query.answer("This action has expired. Please start again.", show_alert=True)
        await callback_query.message.delete()

async def confirm_filename_callback(client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    if user_states.get(user_id) and user_states[user_id].get("state") == "confirm_filename":
        job_data = user_states[user_id]["job_data"]
        final_filename = job_data["final_filename"]
        status_message = await callback_query.message.edit_text(f"✅ Job for `{final_filename}` has been queued!")
        
        job_data["status_message_id"] = status_message.id
        job_data["cpu_queue"] = "default"
        
        result = start_encode_pipeline(job_data)
        database.add_job(result.id, user_id, final_filename, status_message.id, job_data)
        
        del user_states[user_id]
        if not callback_query.data.split("|")[3].isdigit():
             del pending_parts[int(callback_query.data.split("|")[3])]
    else:
        await callback_query.answer("This action has expired. Please start again.", show_alert=True)
        await callback_query.message.delete()

async def manage_job_callback(client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    action, task_id = callback_query.data.split("|", 1)
    
    job = database.get_job(task_id)
    if not job or job['user_id'] != user_id:
        await callback_query.answer("This job could not be found.", show_alert=True)
        await show_queue(callback_query)
        return

    text = (f"**Managing Job:** `{job['filename']}`\n"
            f"**Status:** `{job['status']}`")
            
    keyboard = []
    action_buttons = []
    if job.get('job_data', {}).get('cpu_queue') == 'default':
        action_buttons.append(InlineKeyboardButton("⚡️ Accelerate", callback_data=f"accelerate|{task_id}"))
    
    action_buttons.append(InlineKeyboardButton("❌ Cancel", callback_data=f"cancel|{task_id}"))
    keyboard.append(action_buttons)
    keyboard.append([InlineKeyboardButton("⬅️ Back to Queue", callback_data="queue")])
    
    await callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def accelerate_callback(client, callback_query: CallbackQuery):
    await callback_query.answer("⚡️ Accelerating job...", show_alert=False)
    user_id = callback_query.from_user.id
    action, task_id = callback_query.data.split("|", 1)
    job = database.get_job(task_id)
    if not job or job['user_id'] != user_id:
        await callback_query.message.edit_text("Could not find this job.")
        return
        
    celery_producer.control.revoke(task_id, terminate=False)
    job_data = job['job_data']
    job_data['cpu_queue'] = 'high_priority'
    new_result = start_encode_pipeline(job_data)
    database.remove_job(task_id)
    database.add_job(new_result.id, user_id, job['filename'], job['status_message_id'], job_data)
    
    await callback_query.message.edit_text("✅ Job has been moved to the accelerator queue!")
    await asyncio.sleep(1)
    await show_queue(callback_query)

async def cancel_callback(client, callback_query: CallbackQuery):
    await callback_query.answer("❌ Cancelling job...", show_alert=False)
    user_id = callback_query.from_user.id
    action, task_id = callback_query.data.split("|", 1)
    job = database.get_job(task_id)
    if not job or job['user_id'] != user_id:
        await callback_query.message.edit_text("Could not find this job.")
        return
        
    celery_producer.control.revoke(task_id, terminate=True, signal='SIGKILL')
    database.update_job_status(task_id, "CANCELLED")
    
    await callback_query.message.edit_text(f"✅ Job for `{job['filename']}` has been cancelled.")
    try:
        await client.edit_message_text(user_id, job['status_message_id'], "❌ Job Cancelled by User.")
    except Exception as e:
        logger.warning(f"Could not edit original status message: {e}")
    
    await asyncio.sleep(1)
    await show_queue(callback_query)

async def cancel_all_callback(client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    await callback_query.answer("🗑️ Cancelling all jobs...", show_alert=False)
    
    jobs_to_cancel = database.get_user_jobs(user_id)
    if not jobs_to_cancel:
        await callback_query.message.edit_text("There are no active jobs to cancel.")
        return
        
    for job in jobs_to_cancel:
        celery_producer.control.revoke(job['task_id'], terminate=True, signal='SIGKILL')
        database.update_job_status(job['task_id'], "CANCELLED")
        try:
            await client.edit_message_text(user_id, job['status_message_id'], "❌ Job Cancelled by User.")
        except Exception: pass 
            
    await callback_query.message.edit_text("✅ All active jobs have been cancelled.")

async def set_setting_callback(client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    action, key = callback_query.data.split("|", 1)
    user_states[user_id] = key
    prompts = {"brand_name": "Please send your brand name.", "website": "Please send your website link.",
               "custom_thumbnail_message_id": "Please send a photo."}
    await callback_query.message.reply_text(f"▶️ {prompts.get(key, 'Please send new value.')}\n\nOr send /cancel.")
    await callback_query.answer()

if __name__ == "__main__":
    if not all([BOT_TOKEN, API_ID, API_HASH, ADMIN_USER_IDS]):
        logger.critical("CRITICAL: One or more required environment variables are missing!")
    else:
        logger.info("Bot is starting...")
        app.run()
        
