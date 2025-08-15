import os
import logging
import asyncio
import subprocess
import tempfile

import httpx  # Import the HTTP library
from celery import Celery
from telegram import Bot
from telegram.constants import ParseMode
from dotenv import load_dotenv

# ------- Load env & logging -------
load_dotenv()
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)

# ------- Configuration -------
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
BOT_TOKEN = os.getenv("BOT_TOKEN")
BRANDING_TEXT = os.getenv("BRANDING_TEXT", "MyEnc")

if REDIS_URL.startswith("rediss://"):
    REDIS_URL = f"{REDIS_URL}?ssl_cert_reqs=CERT_NONE"

celery_app = Celery("tasks", broker=REDIS_URL, backend=REDIS_URL)

# ------- This async function contains all our Telegram API calls -------
async def _run_async_task(user_id: int, file_id: str, quality: str):
    bot = Bot(token=BOT_TOKEN)
    await bot.send_message(
        chat_id=user_id, text="⚙️ Your encoding job has started! Preparing to download..."
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        input_path = os.path.join(temp_dir, "input_video")
        output_filename = f"encoded_video_{quality}p_{BRANDING_TEXT}.mkv"
        output_path = os.path.join(temp_dir, output_filename)

        try:
            # 1. Get the File object which contains the download path
            logging.info("Fetching file path from Telegram...")
            file_obj = await bot.get_file(file_id)

            # 2. Construct the direct download URL and download the file
            download_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_obj.file_path}"
            logging.info("Downloading file from direct URL...")

            async with httpx.AsyncClient() as client:
                with open(input_path, "wb") as f:
                    async with client.stream("GET", download_url, timeout=60.0) as response:
                        response.raise_for_status()  # Will raise an error for 4xx/5xx responses
                        async for chunk in response.aiter_bytes():
                            f.write(chunk)

            await bot.send_message(
                chat_id=user_id,
                text=f"✅ Download complete! Starting the {quality}p encode. This might take a while... ⏳",
            )

            # 3. Run FFmpeg (This is a blocking, synchronous call)
            ffmpeg_command = [
                "ffmpeg", "-i", input_path, "-c:v", "libx265", "-preset", "slow",
                "-crf", "24", "-vf", f"scale=-2:{quality}", "-c:a", "aac",
                "-b:a", "128k", "-metadata", f"encoder={BRANDING_TEXT}", "-y", output_path,
            ]
            logging.info(f"Running FFmpeg command: {' '.join(ffmpeg_command)}")
            process = subprocess.run(ffmpeg_command, capture_output=True, text=True, check=False)

            if process.returncode != 0:
                error_message = f"😭 Oh no! Encoding failed.\n\n**FFmpeg Error:**\n```\n{process.stderr[-1000:]}\n```"
                await bot.send_message(
                    chat_id=user_id, text=error_message, parse_mode=ParseMode.MARKDOWN
                )
                return

            # 4. Upload the result
            await bot.send_message(
                chat_id=user_id, text="🎉 Success! Your file is encoded. Now uploading..."
            )
            with open(output_path, "rb") as f:
                await bot.send_document(
                    chat_id=user_id, document=f, caption=f"Here is your {quality}p encode!"
                )
            await bot.send_message(
                chat_id=user_id, text="🚀 Upload complete! Job finished."
            )

        except httpx.HTTPStatusError as e:
             logging.error(f"HTTP error while downloading file for user {user_id}: {e}")
             await bot.send_message(chat_id=user_id, text=f"💥 A critical error occurred: Failed to download the file from Telegram's servers. Please try again.")

        except Exception as e:
            logging.error(f"An error occurred in the async task part for user {user_id}: {e}")
            try:
                await bot.send_message(chat_id=user_id, text=f"💥 A critical error occurred during your job: {e}")
            except Exception as e2:
                logging.error(f"Failed to even send the error message: {e2}")

# ------- The Celery task is now just a simple, synchronous wrapper -------
@celery_app.task(name="worker.tasks.encode_video_task")
def encode_video_task(user_id: int, file_id: str, quality: str):
    """
    Synchronous Celery task that calls our async wrapper.
    """
    try:
        asyncio.run(_run_async_task(user_id, file_id, quality))
    except Exception as e:
        logging.error(f"A top-level error occurred in the encoding task for user {user_id}: {e}")
    logging.info(f"Finished job for user {user_id}, file_id {file_id}")

