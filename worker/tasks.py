import os
import logging
import asyncio
import subprocess
import tempfile
from celery import Celery
from pyrogram import Client
from pyrogram.errors import FloodWait
from dotenv import load_dotenv
from .utils import get_video_info, generate_standard_filename

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH")
BRANDING_TEXT = os.getenv("BRANDING_TEXT", "MyEnc")

# --- NEW: Configurable Encoding Settings ---
# These can be changed via environment variables without touching the code.
ENCODE_PRESET = os.getenv("ENCODE_PRESET", "slow")  # e.g., ultrafast, superfast, veryfast, faster, fast, medium, slow, slower, veryslow
ENCODE_CRF = os.getenv("ENCODE_CRF", "24")          # Constant Rate Factor (CRF). Lower is better quality, higher is smaller file. 18-28 is a good range.
AUDIO_BITRATE = os.getenv("AUDIO_BITRATE", "128k")  # e.g., 96k, 128k, 192k, 256k

if REDIS_URL.startswith("rediss://"):
    REDIS_URL = f"{REDIS_URL}?ssl_cert_reqs=CERT_NONE"
celery_app = Celery("tasks", broker=REDIS_URL, backend=REDIS_URL)

async def _run_async_task(user_chat_id: int, message_id: int, quality: str):
    app = Client("worker_session", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH, in_memory=True)
    
    await app.start()
    
    status_message = await app.send_message(user_chat_id, "⚙️ Job started. Preparing to download...")
    original_filename = "unknown_file.tmp" # Default filename in case of error

    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = os.path.join(temp_dir, "input.tmp")
            
            message = await app.get_messages(user_chat_id, message_id)
            
            file_meta = message.video or message.document
            original_filename = getattr(file_meta, "file_name", "unknown_file.tmp")

            async def download_progress(current, total):
                percent = round(current * 100 / total)
                if percent % 10 == 0:
                    try:
                        await status_message.edit_text(f"Downloading `{original_filename}`... {percent}%")
                    except FloodWait as e:
                        await asyncio.sleep(e.value)

            await app.download_media(message, file_name=input_path, progress=download_progress)
            
            await status_message.edit_text("🔬 Download complete! Analyzing video file...")
            
            video_info = get_video_info(input_path)
            if not video_info:
                raise ValueError("Could not get video information from the file. It might be corrupt.")

            original_height = int(video_info.get("height", 0))
            target_quality = int(quality)

            if original_height > 0 and target_quality > original_height:
                logging.warning(f"User requested {target_quality}p, but original is {original_height}p. Capping quality to avoid upscaling.")
                target_quality = original_height
            
            final_quality_str = str(target_quality)
            
            output_filename = generate_standard_filename(original_filename, final_quality_str, BRANDING_TEXT)
            output_path = os.path.join(temp_dir, output_filename)

            await status_message.edit_text(f"✅ Analysis complete! Starting encode for `{output_filename}`...")
            
            # --- UPDATED: FFmpeg command now uses configurable settings ---
            ffmpeg_command = [
                "ffmpeg", "-i", input_path,
                "-c:v", "libx265",
                "-preset", ENCODE_PRESET,
                "-crf", ENCODE_CRF,
                "-vf", f"scale=-2:{final_quality_str}",
                "-c:a", "aac",
                "-b:a", AUDIO_BITRATE,
                "-metadata", f"encoder={BRANDING_TEXT}",
                "-y", output_path
            ]
            
            process = subprocess.run(ffmpeg_command, capture_output=True, text=True)

            if process.returncode != 0:
                error_log = process.stderr
                logging.error(f"FFmpeg failed! Stderr:\n{error_log}")
                raise RuntimeError("FFmpeg encountered an error during encoding. Check logs.")

            async def upload_progress(current, total):
                percent = round(current * 100 / total)
                if percent % 10 == 0:
                    try:
                        await status_message.edit_text(f"Uploading `{output_filename}`... {percent}%")
                    except FloodWait as e:
                        await asyncio.sleep(e.value)
            
            await app.send_document(
                user_chat_id,
                output_path,
                caption=f"✅ Encode Complete!\n\n`{output_filename}`",
                progress=upload_progress
            )
            await status_message.edit_text("🚀 Upload complete! Job finished.")

    except Exception as e:
        logging.error(f"A critical error occurred in task for message {message_id}: {e}")
        error_message = f"💥 An error occurred with your file `{original_filename}`:\n\n`{str(e)}`"
        await status_message.edit_text(error_message)
    finally:
        await app.stop()

@celery_app.task(name="worker.tasks.encode_video_task")
def encode_video_task(user_chat_id: int, message_id: int, quality: str):
    asyncio.run(_run_async_task(user_chat_id, message_id, quality))
