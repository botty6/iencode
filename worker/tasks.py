import os
import logging
import asyncio
import re
import tempfile
import glob
import time
import shutil
from celery import Celery, chain
from celery.exceptions import Ignore
from kombu import Queue
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
API_HASH = os.getenv("TELEGRAM_API_HASH", "").strip()
WORKERS = int(os.getenv("UPLOAD_WORKERS", 10)) 
DOWNLOAD_CACHE_DIR = "/tmp/iencode_downloads"

os.makedirs(DOWNLOAD_CACHE_DIR, exist_ok=True)

DEFAULT_BRAND = os.getenv("BRANDING_TEXT", "MyEnc")
DEFAULT_WEBSITE = os.getenv("BRANDING_WEBSITE", "t.me/YourChannel")
ENCODE_PRESET = os.getenv("ENCODE_PRESET", "slow")
ENCODE_CRF = os.getenv("ENCODE_CRF", "24")
AUDIO_BITRATE = os.getenv("AUDIO_BITRATE", "128k")

if REDIS_URL.startswith("rediss://"):
    REDIS_URL = f"{REDIS_URL}?ssl_cert_reqs=CERT_NONE"

# --- Celery App with HYBRID Queues ---
celery_app = Celery("tasks", broker=REDIS_URL, backend=REDIS_URL)

celery_app.conf.task_queues = (
    Queue('io_queue', routing_key='io_queue'),
    Queue('default', routing_key='default'),
    Queue('high_priority', routing_key='high_priority'),
)

# --- TASK 1: I/O-Bound Download Task (runs on Gevent worker) ---
@celery_app.task(name="worker.tasks.download_task", bind=True)
def download_task(self, user_id: int, status_message_id: int, list_of_message_ids: list, quality: str, original_thumbnail_id: str, user_settings: dict):
    """Synchronous wrapper for the async download and prep logic."""
    try:
        return asyncio.run(_run_download_and_prep(self.request.id, user_id, status_message_id, list_of_message_ids, quality, original_thumbnail_id, user_settings))
    except Exception as e:
        logging.error(f"Download task {self.request.id} failed: {e}")
        raise

async def _run_download_and_prep(task_id: str, user_id: int, status_message_id: int, list_of_message_ids: list, quality: str, original_thumbnail_id: str, user_settings: dict):
    app = Client(f"dl_{task_id}", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH, workdir="/tmp", workers=WORKERS, in_memory=True)
    await app.start()
    
    status_message = await app.get_messages(user_id, status_message_id)
    last_update_time = 0
    
    job_cache_dir = os.path.join(DOWNLOAD_CACHE_DIR, task_id)
    os.makedirs(job_cache_dir, exist_ok=True)
    merged_input_path = os.path.join(job_cache_dir, "merged_input.mkv")

    try:
        messages = await app.get_messages(user_id, list_of_message_ids)
        if not isinstance(messages, list): messages = [messages]

        first_message = messages[0]
        file_meta = first_message.video or first_message.document
        original_filename = getattr(file_meta, "file_name", "unknown_file.tmp")
        
        fresh_thumbnail_id = None
        if first_message.video and first_message.video.thumb:
            fresh_thumbnail_id = first_message.video.thumb.file_id

        total_size = sum(getattr(m.video or m.document, "file_size", 0) for m in messages)
        if total_size == 0: raise ValueError("File size is 0 B.")

        start_time = time.time()
        current_size = 0
        with open(merged_input_path, "wb") as f:
            for message in messages:
                async for chunk in app.stream_media(message):
                    f.write(chunk)
                    current_size += len(chunk)
                    
                    now = time.time()
                    if now - last_update_time > 5:
                        last_update_time = now
                        elapsed = now - start_time
                        speed = current_size / elapsed if elapsed > 0 else 0
                        progress_bar = create_progress_bar(current_size, total_size)
                        text = (f"📥 **Downloading:** `{original_filename}`\n"
                                f"{progress_bar}\n"
                                f"`{humanbytes(current_size)}` of `{humanbytes(total_size)}`\n"
                                f"**Speed:** `{humanbytes(speed, speed=True)}`")
                        try:
                            await status_message.edit_text(text)
                        except FloodWait as e:
                            await asyncio.sleep(e.value)

        await status_message.edit_text("🔬 Analyzing downloaded file...")
        
        custom_thumbnail_id = user_settings.get("custom_thumbnail_id")
        
        thumb_path = None
        final_thumbnail_id = custom_thumbnail_id or fresh_thumbnail_id
        if final_thumbnail_id:
            thumb_path = await app.download_media(final_thumbnail_id, file_name=os.path.join(job_cache_dir, "thumb.jpg"))
        
        video_info = get_video_info(merged_input_path)
        if not video_info: raise ValueError("Could not get video info from the downloaded file.")
        
        return {
            "user_id": user_id, "status_message_id": status_message_id,
            "input_path": merged_input_path, "job_cache_dir": job_cache_dir,
            "original_filename": original_filename, "quality": quality,
            "thumb_path": thumb_path, "video_info": video_info,
            "user_settings": user_settings
        }
    except Exception as e:
        await status_message.edit_text(f"💥 Download/Prep Error: {e}")
        raise
    finally:
        await app.stop()

@celery_app.task(name="worker.tasks.encode_task", bind=True)
def encode_task(self, prep_data: dict):
    """Synchronous wrapper for the async encode and upload logic."""
    try:
        return asyncio.run(_run_encode_and_upload(self.request.id, prep_data))
    except Exception as e:
        logging.error(f"Encode task {self.request.id} failed: {e}")
        raise

async def _run_encode_and_upload(task_id: str, prep_data: dict):
    user_id = prep_data["user_id"]
    status_message_id = prep_data["status_message_id"]
    
    app = Client(f"ul_{task_id}", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH, workdir="/tmp", workers=WORKERS, in_memory=True)
    await app.start()

    status_message = await app.get_messages(user_id, status_message_id)
    last_update_time = 0
    job_cache_dir = prep_data["job_cache_dir"]
    
    try:
        brand_name = prep_data["user_settings"].get("brand_name", DEFAULT_BRAND)
        website = prep_data["user_settings"].get("website", DEFAULT_WEBSITE)
        
        total_duration_sec = float(prep_data["video_info"].get("duration", 0))
        original_height = int(prep_data["video_info"].get("height", 0))

        if total_duration_sec <= 0:
            raise ValueError("Video duration is invalid or zero. The file may be corrupt.")
        if original_height <= 0:
            raise ValueError("Could not determine video height. The file may be corrupt or not a video.")

        target_quality = int(prep_data["quality"])
        if original_height > 0 and target_quality > original_height:
            target_quality = original_height
        
        output_filename = generate_standard_filename(prep_data["original_filename"], str(target_quality), brand_name)
        output_path = os.path.join(job_cache_dir, output_filename)

        ffmpeg_command = [
            "ffmpeg", "-i", prep_data["input_path"],
            "-c:v", "libx265", "-preset", ENCODE_PRESET, "-crf", ENCODE_CRF,
            "-vf", f"scale=-2:{str(target_quality)}",
            "-c:a", "aac", "-b:a", AUDIO_BITRATE,
            "-metadata", f"encoder={brand_name}",
            "-metadata", f"comment=Encoded by {brand_name} | Join us: {website}",
            "-y", "-progress", "pipe:1", "-nostats", output_path
        ]
        
        process = await asyncio.create_subprocess_exec(*ffmpeg_command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

        while process.returncode is None:
            line_bytes = await process.stdout.readline()
            if not line_bytes: break
            line = line_bytes.decode('utf-8').strip()
            if "out_time_ms" in line:
                # --- START: ROBUST PROGRESS PARSING ---
                try:
                    time_str = line.split("=")[1]
                    current_time_sec = int(time_str) / 1_000_000
                except (ValueError, IndexError):
                    # Gracefully skip malformed progress lines (e.g., out_time_ms=N/A)
                    continue
                # --- END: ROBUST PROGRESS PARSING ---
                
                now = time.time()
                if now - last_update_time > 5:
                    last_update_time = now
                    progress_bar = create_progress_bar(current_time_sec, total_duration_sec)
                    text = (f"⚙️ **Encoding:** `{output_filename}`\n{progress_bar}")
                    try:
                        await status_message.edit_text(text)
                    except FloodWait as e:
                        await asyncio.sleep(e.value)
            await asyncio.sleep(0.1)

        stdout_output, stderr_output = await process.communicate()
        if process.returncode != 0: 
            error_message = stderr_output.decode('utf-8').strip()
            logging.error(f"FFmpeg failed for task {task_id}! Stderr:\n{error_message}")
            # Raise a more informative error to the user
            last_line_of_error = error_message.splitlines()[-1] if error_message else "Unknown FFmpeg error"
            raise RuntimeError(f"FFmpeg error: {last_line_of_error}")

        async def upload_progress(current, total):
            nonlocal last_update_time
            now = time.time()
            if now - last_update_time > 5:
                last_update_time = now
                progress_bar = create_progress_bar(current, total)
                text = (f"📤 **Uploading:** `{output_filename}`\n"
                        f"{progress_bar}\n"
                        f"`{humanbytes(current)}` of `{humanbytes(total)}`")
                try:
                    await status_message.edit_text(text)
                except FloodWait as e:
                    await asyncio.sleep(e.value)

        await app.send_document(user_id, output_path, caption=f"✅ Encode Complete!\n\n`{output_filename}`", thumb=prep_data["thumb_path"], progress=upload_progress)
        await status_message.delete()
        
    except Exception as e:
        await status_message.edit_text(f"💥 Encode/Upload Error: {e}")
        raise
    finally:
        await app.stop()
        if os.path.exists(job_cache_dir):
            shutil.rmtree(job_cache_dir)
