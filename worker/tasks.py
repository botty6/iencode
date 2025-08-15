import os
import logging
import asyncio
import subprocess
import tempfile
from celery import Celery
from pyrogram import Client
from pyrogram.errors import FloodWait
from dotenv import load_dotenv

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH")
BRANDING_TEXT = os.getenv("BRANDING_TEXT", "MyEnc")

if REDIS_URL.startswith("rediss://"):
    REDIS_URL = f"{REDIS_URL}?ssl_cert_reqs=CERT_NONE"
celery_app = Celery("tasks", broker=REDIS_URL, backend=REDIS_URL)

async def _run_async_task(user_chat_id: int, message_id: int, quality: str):
    # This is the key change: The client is now created *inside* the task.
    # It connects, does its job, and disconnects, leaving the main bot's connection untouched.
    app = Client("worker_session", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH, in_memory=True)
    
    await app.start()
    
    status_message = await app.send_message(user_chat_id, "⚙️ Job started. Preparing to download...")

    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = os.path.join(temp_dir, "input.tmp")
            output_path = os.path.join(temp_dir, f"encoded_{quality}p.mkv")
            
            message = await app.get_messages(user_chat_id, message_id)
            
            async def download_progress(current, total):
                percent = round(current * 100 / total)
                if percent % 10 == 0:
                    try:
                        await status_message.edit_text(f"Downloading... {percent}%")
                    except FloodWait as e:
                        await asyncio.sleep(e.value)

            await app.download_media(message, file_name=input_path, progress=download_progress)
            
            await status_message.edit_text(f"✅ Download complete! Starting the {quality}p encode...")
            
            ffmpeg_command = ["ffmpeg", "-i", input_path, "-c:v", "libx265", "-preset", "slow", "-crf", "24", "-vf", f"scale=-2:{quality}", "-c:a", "aac", "-b:a", "128k", "-metadata", f"encoder={BRANDING_TEXT}", "-y", output_path]
            subprocess.run(ffmpeg_command, capture_output=True, text=True, check=False)

            async def upload_progress(current, total):
                percent = round(current * 100 / total)
                if percent % 10 == 0:
                    try:
                        await status_message.edit_text(f"Uploading... {percent}%")
                    except FloodWait as e:
                        await asyncio.sleep(e.value)

            await app.send_document(user_chat_id, output_path, caption=f"Here is your {quality}p encode!", progress=upload_progress)
            await status_message.edit_text("🚀 Upload complete! Job finished.")

    except Exception as e:
        logging.error(f"A critical error occurred: {e}")
        await status_message.edit_text(f"💥 A critical error occurred: {e}")
    finally:
        await app.stop()

@celery_app.task(name="worker.tasks.encode_video_task")
def encode_video_task(user_chat_id: int, message_id: int, quality: str):
    asyncio.run(_run_async_task(user_chat_id, message_id, quality))
    
