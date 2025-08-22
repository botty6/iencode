import os
import logging
import asyncio
import re
import tempfile
import glob
import time
import shutil
from celery import Celery
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

# --- Celery App with Priority Queues ---
celery_app = Celery("tasks", broker=REDIS_URL, backend=REDIS_URL)

celery_app.conf.task_queues = (
    Queue('high_priority', routing_key='high_priority'),
    Queue('default', routing_key='default'),
)
celery_app.conf.task_default_queue = 'default'
celery_app.conf.task_default_routing_key = 'default'

async def send_status_update(user_chat_id: int, message_text: str):
    app = Client(f"status_updater_{user_chat_id}", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH, in_memory=True)
    async with app:
        try:
            await app.send_message(user_chat_id, message_text)
        except Exception as e:
            logging.error(f"Failed to send status update to {user_chat_id}: {e}")

async def _run_async_task(task_id: str, user_id: int, status_message_id: int, list_of_message_ids: list, quality: str, original_thumbnail_id: str, user_settings: dict, original_task_id: str = None):
    app = Client("worker_session", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH, workdir="/tmp", workers=WORKERS)
    await app.start()
    
    status_message = await app.get_messages(user_id, status_message_id)
    last_update_time = 0

    cache_key = original_task_id or task_id
    job_cache_dir = os.path.join(DOWNLOAD_CACHE_DIR, cache_key)
    merged_input_path = os.path.join(job_cache_dir, "merged_input.mkv")
    
    try:
        messages = await app.get_messages(user_id, list_of_message_ids)
        if not isinstance(messages, list): messages = [messages]

        first_message = messages[0]
        file_meta = first_message.video or first_message.document
        original_filename = getattr(file_meta, "file_name", "unknown_file.tmp")
        
        total_size = sum(getattr(m.video or m.document, "file_size", 0) for m in messages)

        # --- NEW: Early exit for 0 byte files ---
        if total_size == 0:
            raise ValueError("File size equals to 0 B")

        if os.path.exists(merged_input_path):
            await status_message.edit_text("✅ Found cached file. Skipping download.")
        else:
            os.makedirs(job_cache_dir, exist_ok=True)
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
                            elapsed_time = now - start_time
                            speed = current_size / elapsed_time if elapsed_time > 0 else 0
                            progress_bar = create_progress_bar(current_size, total_size)
                            text = (f"📥 **Downloading:** `{original_filename}`\n"
                                    f"{progress_bar}\n"
                                    f"`{humanbytes(current_size)}` of `{humanbytes(total_size)}`\n"
                                    f"**Speed:** `{humanbytes(speed, speed=True)}`")
                            try:
                                await status_message.edit_text(text)
                            except FloodWait as e:
                                await asyncio.sleep(e.value)
        
        await status_message.edit_text("🔬 File ready! Analyzing...")
        
        brand_name = user_settings.get("brand_name", DEFAULT_BRAND)
        website = user_settings.get("website", DEFAULT_WEBSITE)
        custom_thumbnail_id = user_settings.get("custom_thumbnail_id")

        thumb_path = None
        final_thumbnail_id = custom_thumbnail_id or original_thumbnail_id
        if final_thumbnail_id:
            thumb_path = await app.download_media(final_thumbnail_id, file_name=os.path.join(job_cache_dir, "thumb.jpg"))

        video_info = get_video_info(merged_input_path)
        if not video_info:
            raise ValueError("Could not get video info. File might be corrupt.")
        
        total_duration_sec = float(video_info.get("duration", 0))
        if total_duration_sec <= 0:
            raise ValueError("Video duration is invalid. Cannot calculate encoding progress.")

        original_height = int(video_info.get("height", 0))
        target_quality = int(quality)
        if original_height > 0 and target_quality > original_height:
            target_quality = original_height
        
        final_quality_str = str(target_quality)
        
        output_filename = generate_standard_filename(original_filename, final_quality_str, brand_name)
        output_path = os.path.join(job_cache_dir, output_filename)

        ffmpeg_command = [
            "ffmpeg", "-i", merged_input_path,
            "-c:v", "libx265", "-preset", ENCODE_PRESET, "-crf", ENCODE_CRF,
            "-vf", f"scale=-2:{final_quality_str}",
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
                time_str = line.split("=")[1]
                current_time_sec = int(time_str) / 1_000_000
                
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

        stderr_output = await process.stderr.read()
        if process.returncode != 0:
            logging.error(f"FFmpeg failed! Stderr:\n{stderr_output.decode('utf-8')}")
            raise RuntimeError("FFmpeg encountered an error during encoding.")

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

        await app.send_document(user_id, output_path, caption=f"✅ Encode Complete!\n\n`{output_filename}`", thumb=thumb_path, progress=upload_progress)
        await status_message.delete()

    except (ValueError, RuntimeError) as e:
        await status_message.edit_text(f"💥 A critical, non-retryable error occurred:\n\n`{str(e)}`")
        raise Ignore() # This tells Celery to stop and NOT retry.
    finally:
        await app.stop()
        if os.path.exists(job_cache_dir):
            shutil.rmtree(job_cache_dir)
        # --- REMOVED: The erroneous remove_job(task_id) call ---

@celery_app.task(name="worker.tasks.encode_video_task", bind=True, max_retries=3, default_retry_delay=60)
def encode_video_task(self, user_id: int, status_message_id: int, list_of_message_ids: list, quality: str, original_thumbnail_id: str, user_settings: dict, original_task_id: str = None):
    try:
        task_id = self.request.id
        asyncio.run(_run_async_task(task_id, user_id, status_message_id, list_of_message_ids, quality, original_thumbnail_id, user_settings, original_task_id))
    except Ignore:
        # If our async code raised an Ignore exception, we don't retry.
        pass
    except Exception as e:
        logging.warning(f"Task {self.request.id} failed. Attempt {self.request.retries + 1}. Retrying... Error: {e}")
        retry_message = (f"⚠️ A temporary error occurred. Retrying... (Attempt {self.request.retries + 1})")
        asyncio.run(send_status_update(user_id, retry_message))
        raise self.retry(exc=e)
