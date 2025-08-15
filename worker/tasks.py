import os
import logging
import asyncio
import subprocess
import tempfile
import time
from celery import Celery
from pyrogram import Client
from pyrogram.errors import FloodWait

# ------- Configuration -------
# ... (same as before, just add PYROGRAM_SESSION_STRING)
PYROGRAM_SESSION_STRING = os.getenv("PYROGRAM_SESSION_STRING")

# This async function now uses Pyrogram for everything
async def _run_async_task(user_chat_id: int, message_id: int, quality: str):
    # The "bot" logs in as a bot to send messages
    bot = Client("bot_session", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH, in_memory=True)
    # The "user" logs in as you to download files
    user = Client("user_session", session_string=PYROGRAM_SESSION_STRING, api_id=API_ID, api_hash=API_HASH, in_memory=True)
    
    await bot.start()
    await user.start()
    
    status_message = await bot.send_message(user_chat_id, "⚙️ Job started. Downloading file...")

    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = os.path.join(temp_dir, "input.tmp")
            
            # Use the powerful user account to get the message and download the file
            message = await user.get_messages(user_chat_id, message_id)
            
            # Download with progress callback
            async def download_progress(current, total):
                percent = round(current * 100 / total)
                if percent % 10 == 0: # Update every 10%
                    await bot.edit_message_text(user_chat_id, status_message.id, f"Downloading... {percent}%")

            await user.download_media(message, file_name=input_path, progress=download_progress)
            
            await bot.edit_message_text(user_chat_id, status_message.id, f"✅ Download complete! Starting the {quality}p encode...")
            
            # --- FFmpeg logic is the same ---
            # ... (subprocess.run(ffmpeg_command)) ...
            
            # --- Upload with progress callback ---
            async def upload_progress(current, total):
                percent = round(current * 100 / total)
                if percent % 10 == 0:
                    await bot.edit_message_text(user_chat_id, status_message.id, f"Uploading... {percent}%")

            await bot.send_document(
                user_chat_id,
                output_path,
                caption=f"Here is your {quality}p encode!",
                progress=upload_progress,
            )
            await bot.edit_message_text(user_chat_id, status_message.id, "🚀 Upload complete! Job finished.")

    except FloodWait as e:
        await bot.send_message(user_chat_id, f"⏳ Telegram is rate-limiting me. Please wait {e.value} seconds before the next job.")
    except Exception as e:
        await bot.send_message(user_chat_id, f"💥 A critical error occurred: {e}")
    finally:
        await bot.stop()
        await user.stop()

# Celery task wrapper is updated with new parameters
@celery_app.task(name="worker.tasks.encode_video_task")
def encode_video_task(user_chat_id: int, message_id: int, quality: str):
    asyncio.run(_run_async_task(user_chat_id, message_id, quality))
    
