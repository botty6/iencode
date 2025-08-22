import os
import logging
import asyncio
import subprocess
import tempfile
from celery import Celery
from pyrogram import Client
from pyrogram.errors import FloodWait
from dotenv import load_dotenv
from .utils import get_video_info # <-- IMPORT our new utility function

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
    app = Client("worker_session", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH, in_memory=True)
    
    await app.start()
    
    status_message = await app.send_message(user_chat_id, "⚙️ Job started. Preparing to download...")

    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = os.path.join(temp_dir, "input.tmp")
            
            # For now, we use a simple output name. We will implement renaming in Phase 2.
            output_path = os.path.join(temp_dir, f"encoded_output.mkv")
            
            message = await app.get_messages(user_chat_id, message_id)
            
            async def download_progress(current, total):
                percent = round(current * 100 / total)
                if percent % 10 == 0:
                    try:
                        await status_message.edit_text(f"Downloading... {percent}%")
                    except FloodWait as e:
                        await asyncio.sleep(e.value)

            await app.download_media(message, file_name=input_path, progress=download_progress)
            
            await status_message.edit_text("🔬 Download complete! Analyzing video file...")
            
            # --- NEW: Analyze video before encoding ---
            video_info = get_video_info(input_path)
            if not video_info:
                raise ValueError("Could not get video information from the file. It might be corrupt.")

            original_height = int(video_info.get("height", 0))
            target_quality = int(quality)

            # --- NEW: "No Upscaling" Logic ---
            if original_height > 0 and target_quality > original_height:
                logging.warning(f"User requested {target_quality}p, but original is {original_height}p. Capping quality to avoid upscaling.")
                target_quality = original_height # Cap the quality to the original height
            
            final_quality_str = str(target_quality)
            await status_message.edit_text(f"✅ Analysis complete! Starting the {final_quality_str}p encode...")
            
            ffmpeg_command = [
                "ffmpeg", "-i", input_path,
                "-c:v", "libx265",
                "-preset", "slow",
                "-crf", "24",
                "-vf", f"scale=-2:{final_quality_str}", # Use the potentially capped quality
                "-c:a", "aac",
                "-b:a", "128k",
                "-metadata", f"encoder={BRANDING_TEXT}",
                "-y", output_path
            ]
            
            # --- NEW: Robust FFmpeg Error Handling ---
            process = subprocess.run(ffmpeg_command, capture_output=True, text=True)

            if process.returncode != 0:
                # FFmpeg failed!
                error_log = process.stderr
                logging.error(f"FFmpeg failed! Stderr:\n{error_log}")
                # For security, we send a generic message but log the details
                raise RuntimeError("FFmpeg encountered an error during encoding. Check logs.")

            async def upload_progress(current, total):
                percent = round(current * 100 / total)
                if percent % 10 == 0:
                    try:
                        await status_message.edit_text(f"Uploading... {percent}%")
                    except FloodWait as e:
                        await asyncio.sleep(e.value)

            await app.send_document(
                user_chat_id,
                output_path,
                caption=f"Here is your {final_quality_str}p encode!",
                progress=upload_progress
            )
            await status_message.edit_text("🚀 Upload complete! Job finished.")

    except Exception as e:
        logging.error(f"A critical error occurred in task: {e}")
        # Send a user-friendly error message
        await status_message.edit_text(f"💥 An error occurred: {str(e)}")
    finally:
        await app.stop()

@celery_app.task(name="worker.tasks.encode_video_task")
def encode_video_task(user_chat_id: int, message_id: int, quality: str):
    asyncio.run(_run_async_task(user_chat_id, message_id, quality))
