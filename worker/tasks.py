import os
import logging
import asyncio
import subprocess
import tempfile
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

# ------- This new async function will contain all our Telegram API calls -------
async def _run_async_task(user_id: int, file_id: str, quality: str):
    """
    Runs all async logic for a task in a single event loop.
    """
    bot = Bot(token=BOT_TOKEN)
    
    await bot.send_message(
        chat_id=user_id, text="⚙️ Your encoding job has started! I'm downloading the file now..."
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        input_path = os.path.join(temp_dir, "input_video")
        output_filename = f"encoded_video_{quality}p_{BRANDING_TEXT}.mkv"
        output_path = os.path.join(temp_dir, output_filename)

        try:
            # 1. Download the file
            file_obj = await bot.get_file(file_id)
            await file_obj.download_to_drive(input_path)

            await bot.send_message(
                chat_id=user_id,
                text=f"✅ Download complete! Starting the {quality}p encode. This might take a while... ⏳",
            )

            # 2. Run FFmpeg (This is a blocking, synchronous call)
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

            # 3. Upload the result
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

        except Exception as e:
            logging.error(f"An error occurred in the async task part for user {user_id}: {e}")
            # Use the bot object to report the error if it's available
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
        # We call asyncio.run() only ONCE here.
        asyncio.run(_run_async_task(user_id, file_id, quality))
    except Exception as e:
        # This will catch any unexpected errors during the entire async process
        logging.error(f"A top-level error occurred in the encoding task for user {user_id}: {e}")

    logging.info(f"Finished job for user {user_id}, file_id {file_id}")
    
