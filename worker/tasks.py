import os
import logging
import asyncio
import subprocess
import tempfile
import glob
import time
from celery import Celery
from celery.exceptions import Ignore
from pyrogram import Client
from pyrogram.errors import FloodWait
from dotenv import load_dotenv
from .utils import get_video_info, generate_standard_filename, create_progress_bar, humanbytes

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH")
BRANDING_TEXT = os.getenv("BRANDING_TEXT", "MyEnc")

ENCODE_PRESET = os.getenv("ENCODE_PRESET", "slow")
ENCODE_CRF = os.getenv("ENCODE_CRF", "24")
AUDIO_BITRATE = os.getenv("AUDIO_BITRATE", "128k")

if REDIS_URL.startswith("rediss://"):
    REDIS_URL = f"{REDIS_URL}?ssl_cert_reqs=CERT_NONE"
celery_app = Celery("tasks", broker=REDIS_URL, backend=REDIS_URL)

async def send_status_update(user_chat_id: int, message_text: str):
    """A small, isolated async function to send status updates."""
    app = Client(f"status_updater_{user_chat_id}", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH, in_memory=True)
    async with app:
        try:
            await app.send_message(user_chat_id, message_text)
        except Exception as e:
            logging.error(f"Failed to send status update to {user_chat_id}: {e}")

async def _run_async_task(user_chat_id: int, list_of_message_ids: list, quality: str, thumbnail_file_id: str = None):
    app = Client("worker_session", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH, in_memory=True)
    
    await app.start()
    
    status_message = await app.send_message(user_chat_id, "⚙️ Job started. Initializing...")
    original_filename = "unknown_file.tmp"
    thumb_path = None
    last_update_time = 0

    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            first_message = await app.get_messages(user_chat_id, list_of_message_ids[0])
            file_meta = first_message.video or first_message.document
            original_filename = getattr(file_meta, "file_name", "unknown_file.tmp")
            total_size = getattr(file_meta, "file_size", 0)

            merged_input_path = os.path.join(temp_dir, "merged_input.tmp")
            
            # --- REFACTORED: Use streaming to download to fix memory issues ---
            current_size = 0
            with open(merged_input_path, "wb") as f:
                for msg_id in list_of_message_ids:
                    # Pyrogram's stream_media is memory-efficient
                    async for chunk in app.stream_media(msg_id):
                        f.write(chunk)
                        current_size += len(chunk)
                        
                        # Update progress bar, but not too frequently to avoid FloodWait
                        now = time.time()
                        if now - last_update_time > 5: # Update every 5 seconds
                            last_update_time = now
                            progress_bar = create_progress_bar(current_size, total_size)
                            text = (f"📥 **Downloading:** `{original_filename}`\n"
                                    f"{progress_bar}\n"
                                    f"`{humanbytes(current_size)}` of `{humanbytes(total_size)}`")
                            try:
                                await status_message.edit_text(text)
                            except FloodWait as e:
                                await asyncio.sleep(e.value)

            await status_message.edit_text("🔬 File ready! Analyzing...")
            
            if thumbnail_file_id:
                thumb_path = await app.download_media(thumbnail_file_id, file_name=os.path.join(temp_dir, "thumb.jpg"))

            video_info = get_video_info(merged_input_path)
            if not video_info:
                raise ValueError("Could not get video information from the file. It might be corrupt.")

            # ... (rest of the analysis and encoding logic remains the same) ...
            original_height = int(video_info.get("height", 0))
            target_quality = int(quality)

            if original_height > 0 and target_quality > original_height:
                target_quality = original_height
            
            final_quality_str = str(target_quality)
            
            output_filename = generate_standard_filename(original_filename, final_quality_str, BRANDING_TEXT)
            output_path = os.path.join(temp_dir, output_filename)

            await status_message.edit_text(f"✅ Analysis complete! Starting encode for `{output_filename}`...\n\n(Note: Encoding progress is not yet available, please be patient.)")
            
            ffmpeg_command = ["ffmpeg", "-i", merged_input_path, "-c:v", "libx265", "-preset", ENCODE_PRESET, "-crf", ENCODE_CRF, "-vf", f"scale=-2:{final_quality_str}", "-c:a", "aac", "-b:a", AUDIO_BITRATE, "-metadata", f"encoder={BRANDING_TEXT}", "-y", output_path]
            
            process = subprocess.run(ffmpeg_command, capture_output=True, text=True)

            if process.returncode != 0:
                error_log = process.stderr
                logging.error(f"FFmpeg failed! Stderr:\n{error_log}")
                raise RuntimeError("FFmpeg encountered an error during encoding. Check logs for details.")
            
            # --- ADDED: Upload progress bar ---
            async def upload_progress(current, total):
                nonlocal last_update_time
                now = time.time()
                if now - last_update_time > 5: # Update every 5 seconds
                    last_update_time = now
                    progress_bar = create_progress_bar(current, total)
                    text = (f"📤 **Uploading:** `{output_filename}`\n"
                            f"{progress_bar}\n"
                            f"`{humanbytes(current)}` of `{humanbytes(total)}`")
                    try:
                        await status_message.edit_text(text)
                    except FloodWait as e:
                        await asyncio.sleep(e.value)

            await app.send_document(
                user_chat_id,
                output_path,
                caption=f"✅ Encode Complete!\n\n`{output_filename}`",
                thumb=thumb_path,
                progress=upload_progress # Use the new progress callback
            )
            await status_message.delete() # Clean up the status message
            await app.send_message(user_chat_id, "🚀 Job finished!")

    except (ValueError, RuntimeError) as e:
        logging.error(f"Non-retryable error occurred: {e}")
        await status_message.edit_text(f"💥 A critical error occurred that cannot be retried:\n\n`{str(e)}`")
        raise Ignore()
    finally:
        await app.stop()

@celery_app.task(
    name="worker.tasks.encode_video_task",
    bind=True,
    max_retries=3,
    default_retry_delay=60
)
def encode_video_task(self, user_chat_id: int, list_of_message_ids: list, quality: str, thumbnail_file_id: str = None):
    try:
        asyncio.run(_run_async_task(user_chat_id, list_of_message_ids, quality, thumbnail_file_id))
    except Exception as e:
        logging.warning(f"Task failed. Attempt {self.request.retries + 1} of {self.max_retries}. Retrying in {self.default_retry_delay}s. Error: {e}")
        
        retry_message = (f"⚠️ A temporary error occurred with your job. Retrying in {self.default_retry_delay} seconds... (Attempt {self.request.retries + 1} of {self.max_retries})")
        asyncio.run(send_status_update(user_chat_id, retry_message))
        
        raise self.retry(exc=e)
