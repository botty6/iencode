import os
import logging
import asyncio
import subprocess
import tempfile
import glob
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

ENCODE_PRESET = os.getenv("ENCODE_PRESET", "slow")
ENCODE_CRF = os.getenv("ENCODE_CRF", "24")
AUDIO_BITRATE = os.getenv("AUDIO_BITRATE", "128k")

if REDIS_URL.startswith("rediss://"):
    REDIS_URL = f"{REDIS_URL}?ssl_cert_reqs=CERT_NONE"
celery_app = Celery("tasks", broker=REDIS_URL, backend=REDIS_URL)

# --- UPDATED: Task now accepts thumbnail_file_id ---
async def _run_async_task(user_chat_id: int, list_of_message_ids: list, quality: str, thumbnail_file_id: str = None):
    app = Client("worker_session", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH, in_memory=True)
    
    await app.start()
    
    status_message = await app.send_message(user_chat_id, "⚙️ Job started. Preparing to download files...")
    original_filename = "unknown_file.tmp"
    thumb_path = None

    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            first_message = await app.get_messages(user_chat_id, list_of_message_ids[0])
            file_meta = first_message.video or first_message.document
            original_filename = getattr(file_meta, "file_name", "unknown_file.tmp")
            
            merged_input_path = os.path.join(temp_dir, "merged_input.tmp")
            
            if len(list_of_message_ids) > 1:
                parts_dir = os.path.join(temp_dir, "parts")
                os.makedirs(parts_dir)
                
                for i, msg_id in enumerate(list_of_message_ids):
                    await status_message.edit_text(f"Downloading part {i+1}/{len(list_of_message_ids)}...")
                    part_path = os.path.join(parts_dir, f"part_{i:03d}")
                    await app.download_media(message=msg_id, file_name=part_path)
                
                await status_message.edit_text("✅ All parts downloaded! Merging files...")
                
                part_files = sorted(glob.glob(os.path.join(parts_dir, "part_*")))
                with open(merged_input_path, "wb") as merged_file:
                    for part in part_files:
                        with open(part, "rb") as f_part:
                            merged_file.write(f_part.read())
                
                input_path = merged_input_path
            else:
                await app.download_media(message=first_message, file_name=merged_input_path)
                input_path = merged_input_path

            await status_message.edit_text("🔬 File ready! Analyzing...")
            
            # --- NEW: Download thumbnail if it exists ---
            if thumbnail_file_id:
                logging.info(f"Downloading thumbnail: {thumbnail_file_id}")
                thumb_path = await app.download_media(thumbnail_file_id, file_name=os.path.join(temp_dir, "thumb.jpg"))

            video_info = get_video_info(input_path)
            if not video_info:
                raise ValueError("Could not get video information from the file. It might be corrupt.")

            original_height = int(video_info.get("height", 0))
            target_quality = int(quality)

            if original_height > 0 and target_quality > original_height:
                target_quality = original_height
            
            final_quality_str = str(target_quality)
            
            output_filename = generate_standard_filename(original_filename, final_quality_str, BRANDING_TEXT)
            output_path = os.path.join(temp_dir, output_filename)

            await status_message.edit_text(f"✅ Analysis complete! Starting encode for `{output_filename}`...")
            
            ffmpeg_command = ["ffmpeg", "-i", input_path, "-c:v", "libx265", "-preset", ENCODE_PRESET, "-crf", ENCODE_CRF, "-vf", f"scale=-2:{final_quality_str}", "-c:a", "aac", "-b:a", AUDIO_BITRATE, "-metadata", f"encoder={BRANDING_TEXT}", "-y", output_path]
            
            process = subprocess.run(ffmpeg_command, capture_output=True, text=True)

            if process.returncode != 0:
                error_log = process.stderr
                logging.error(f"FFmpeg failed! Stderr:\n{error_log}")
                raise RuntimeError("FFmpeg encountered an error during encoding. Check logs.")

            await status_message.edit_text(f"Uploading `{output_filename}`...")
            
            # --- UPDATED: Add the 'thumb' parameter to the upload call ---
            await app.send_document(
                user_chat_id,
                output_path,
                caption=f"✅ Encode Complete!\n\n`{output_filename}`",
                thumb=thumb_path # This will be None if no thumb was found
            )
            await status_message.edit_text("🚀 Upload complete! Job finished.")

    except Exception as e:
        logging.error(f"A critical error occurred in task: {e}")
        error_message = f"💥 An error occurred with your file `{original_filename}`:\n\n`{str(e)}`"
        await status_message.edit_text(error_message)
    finally:
        await app.stop()

# --- UPDATED: Celery task definition to pass the new argument ---
@celery_app.task(name="worker.tasks.encode_video_task")
def encode_video_task(user_chat_id: int, list_of_message_ids: list, quality: str, thumbnail_file_id: str = None):
    asyncio.run(_run_async_task(user_chat_id, list_of_message_ids, quality, thumbnail_file_id))
