import os
import logging
import asyncio
import subprocess
import tempfile
from celery import Celery
from pyrogram import Client
from pyrogram.errors import FloodWait

# ------- Configuration -------
# Load environment variables from .env file for local development
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO)

# Celery and Redis configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
if REDIS_URL.startswith("rediss://"):
    REDIS_URL = f"{REDIS_URL}?ssl_cert_reqs=CERT_NONE"
celery_app = Celery("tasks", broker=REDIS_URL, backend=REDIS_URL)

# Bot and User Account configuration
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH")
PYROGRAM_SESSION_STRING = os.getenv("PYROGRAM_SESSION_STRING")
BRANDING_TEXT = os.getenv("BRANDING_TEXT", "MyEnc")

# This is the main async logic for the worker task
async def _run_async_task(user_chat_id: int, message_id: int, quality: str):
    # The "bot" client logs in as a bot to send messages and files
    bot = Client("bot_session", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH, in_memory=True)
    # The "user" client logs in as a user account to download large files
    user = Client("user_session", session_string=PYROGRAM_SESSION_STRING, api_id=API_ID, api_hash=API_HASH, in_memory=True)
    
    await bot.start()
    await user.start()
    
    status_message = await bot.send_message(user_chat_id, "⚙️ Job started. Preparing to download...")

    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = os.path.join(temp_dir, "input.tmp")
            output_filename = f"encoded_video_{quality}p_{BRANDING_TEXT}.mkv"
            output_path = os.path.join(temp_dir, output_filename)
            
            # Use the powerful user account to get the message and download the file
            message = await user.get_messages(user_chat_id, message_id)
            
            # --- Download with Progress Callback ---
            async def download_progress(current, total):
                percent = round(current * 100 / total)
                # Update status message every 10% to avoid API spam
                if percent % 10 == 0:
                    try:
                        await bot.edit_message_text(user_chat_id, status_message.id, f"Downloading... {percent}%")
                    except FloodWait as e:
                        # If we get rate-limited, wait the specified time
                        await asyncio.sleep(e.value)

            await user.download_media(message, file_name=input_path, progress=download_progress)
            
            await bot.edit_message_text(user_chat_id, status_message.id, f"✅ Download complete! Starting the {quality}p encode...")
            
            # --- FFmpeg Encoding (Synchronous Blocking Call) ---
            ffmpeg_command = [
                "ffmpeg", "-i", input_path, "-c:v", "libx265", "-preset", "slow",
                "-crf", "24", "-vf", f"scale=-2:{quality}", "-c:a", "aac",
                "-b:a", "128k", "-metadata", f"encoder={BRANDING_TEXT}", "-y", output_path,
            ]
            subprocess.run(ffmpeg_command, capture_output=True, text=True, check=False)

            # --- Upload with Progress Callback ---
            async def upload_progress(current, total):
                percent = round(current * 100 / total)
                if percent % 10 == 0:
                    try:
                        await bot.edit_message_text(user_chat_id, status_message.id, f"Uploading... {percent}%")
                    except FloodWait as e:
                        await asyncio.sleep(e.value)

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
        logging.error(f"A critical error occurred in the worker task: {e}")
        await bot.send_message(user_chat_id, f"💥 A critical error occurred: {e}")
    finally:
        await bot.stop()
        await user.stop()

# --- Celery Task Wrapper ---
@celery_app.task(name="worker.tasks.encode_video_task")
def encode_video_task(user_chat_id: int, message_id: int, quality: str):
    """Synchronous Celery task that calls our async Pyrogram wrapper."""
    asyncio.run(_run_async_task(user_chat_id, message_id, quality))
    
