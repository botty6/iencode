import os
import logging
import asyncio
import subprocess
import tempfile
from celery import Celery
from telegram import Bot
from telegram.constants import ParseMode
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- Celery and Bot Configuration ---

# Heroku provides the REDIS_URL env var for the Redis add-on
REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
BOT_TOKEN = os.environ.get("BOT_TOKEN")
BRANDING_TEXT = os.environ.get("BRANDING_TEXT", "MyEnc")

# Initialize Celery
celery_app = Celery('tasks', broker=REDIS_URL, backend=REDIS_URL)

# Initialize Telegram Bot
# We use asyncio.run() because the python-telegram-bot library is async
# but Celery tasks are synchronous by default.
try:
    bot = asyncio.run(Bot(token=BOT_TOKEN).initialize())
except Exception as e:
    logging.error(f"Failed to initialize Telegram Bot: {e}")
    bot = None

# --- Helper Functions ---

async def send_telegram_message(user_id, text, parse_mode=None):
    """Helper to send a message to a user."""
    if bot:
        try:
            await bot.send_message(chat_id=user_id, text=text, parse_mode=parse_mode)
        except Exception as e:
            logging.error(f"Failed to send message to user {user_id}: {e}")

async def send_telegram_file(user_id, file_path, caption):
    """Helper to send a file to a user."""
    if bot:
        try:
            with open(file_path, 'rb') as f:
                await bot.send_document(chat_id=user_id, document=f, caption=caption)
        except Exception as e:
            logging.error(f"Failed to send file to user {user_id}: {e}")

# --- Celery Task Definition ---

@celery_app.task(name='worker.tasks.encode_video_task')
def encode_video_task(user_id: int, file_id: str, quality: str):
    """
    The main encoding task. Downloads, encodes, and uploads a video.
    """
    asyncio.run(send_telegram_message(user_id, "⚙️ Your encoding job has started! I'm downloading the file now..."))

    with tempfile.TemporaryDirectory() as temp_dir:
        input_path = os.path.join(temp_dir, 'input_video')
        output_filename = f"encoded_video_{quality}p_{BRANDING_TEXT}.mkv"
        output_path = os.path.join(temp_dir, output_filename)

        try:
            # 1. Download the file from Telegram
            if bot:
                file_obj = asyncio.run(bot.get_file(file_id))
                asyncio.run(file_obj.download_to_drive(input_path))
            else:
                raise ConnectionError("Bot is not initialized. Cannot download file.")

            asyncio.run(send_telegram_message(user_id, f"✅ Download complete! Starting the {quality}p encode. This might take a while... ⏳"))

            # 2. Construct and run the FFmpeg command
            # This is where you would call your preset script
            # For this example, we'll use a direct FFmpeg command
            ffmpeg_command = [
                'ffmpeg',
                '-i', input_path,
                '-c:v', 'libx265',          # HEVC encoder
                '-preset', 'slow',          # Good quality/speed balance
                '-crf', '24',               # Constant Rate Factor (quality)
                '-vf', f'scale=-2:{quality}', # Scale to target height
                '-c:a', 'aac',              # Audio codec
                '-b:a', '128k',             # Audio bitrate
                '-metadata', f'encoder={BRANDING_TEXT}',
                '-y',                       # Overwrite output file
                output_path
            ]

            logging.info(f"Running FFmpeg command: {' '.join(ffmpeg_command)}")
            process = subprocess.run(ffmpeg_command, capture_output=True, text=True)

            if process.returncode != 0:
                # Encoding failed, send error log to user
                error_message = f"😭 Oh no! Encoding failed.\n\n**FFmpeg Error:**\n```\n{process.stderr[-1000:]}\n```"
                asyncio.run(send_telegram_message(user_id, error_message, parse_mode=ParseMode.MARKDOWN))
                return

            # 3. Upload the encoded file back to the user
            asyncio.run(send_telegram_message(user_id, "🎉 Success! Your file is encoded. Now uploading..."))
            asyncio.run(send_telegram_file(user_id, output_path, caption=f"Here is your {quality}p encode!\nBranded with: {BRANDING_TEXT}"))
            asyncio.run(send_telegram_message(user_id, "🚀 Upload complete! Job finished."))

        except Exception as e:
            logging.error(f"An error occurred in the encoding task for user {user_id}: {e}")
            asyncio.run(send_telegram_message(user_id, f"💥 A critical error occurred during your job: {e}"))

    logging.info(f"Finished job for user {user_id}, file_id {file_id}")
  
