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

# We no longer initialize the bot globally.
# This will be done inside each task.

# Add the SSL parameter for Heroku Redis
if REDIS_URL.startswith("rediss://"):
    REDIS_URL = f"{REDIS_URL}?ssl_cert_reqs=CERT_NONE"

# Initialize Celery
celery_app = Celery("tasks", broker=REDIS_URL, backend=REDIS_URL)

# ------- Helper Functions -------
async def send_telegram_message(bot: Bot, user_id: int, text: str, parse_mode=None):
    """Helper to send a message using a provided bot instance."""
    try:
        await bot.send_message(chat_id=user_id, text=text, parse_mode=parse_mode)
    except Exception as e:
        logging.error(f"Failed to send message to user {user_id}: {e}")

async def send_telegram_file(bot: Bot, user_id: int, file_path: str, caption: str):
    """Helper to send a file using a provided bot instance."""
    try:
        with open(file_path, "rb") as f:
            await bot.send_document(chat_id=user_id, document=f, caption=caption)
    except Exception as e:
        logging.error(f"Failed to send file to user {user_id}: {e}")

# ------- Celery Task Definition -------
@celery_app.task(name="worker.tasks.encode_video_task")
def encode_video_task(user_id: int, file_id: str, quality: str):
    """
    The main encoding task. It creates its own Bot instance to be safe.
    """
    # This is the key change: create a new Bot instance for each task.
    bot = Bot(token=BOT_TOKEN)
    
    # We must wrap our async helper calls in asyncio.run()
    asyncio.run(
        send_telegram_message(
            bot, user_id, "⚙️ Your encoding job has started! I'm downloading the file now..."
        )
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        input_path = os.path.join(temp_dir, "input_video")
        output_filename = f"encoded_video_{quality}p_{BRANDING_TEXT}.mkv"
        output_path = os.path.join(temp_dir, output_filename)

        try:
            # 1. Download the file from Telegram using our new bot instance
            file_obj = asyncio.run(bot.get_file(file_id))
            asyncio.run(file_obj.download_to_drive(input_path))

            asyncio.run(
                send_telegram_message(
                    bot,
                    user_id,
                    f"✅ Download complete! Starting the {quality}p encode. This might take a while... ⏳",
                )
            )

            # 2. Construct and run the FFmpeg command
            ffmpeg_command = [
                "ffmpeg",
                "-i", input_path,
                "-c:v", "libx265",
                "-preset", "slow",
                "-crf", "24",
                "-vf", f"scale=-2:{quality}",
                "-c:a", "aac",
                "-b:a", "128k",
                "-metadata", f"encoder={BRANDING_TEXT}",
                "-y",
                output_path,
            ]

            logging.info(f"Running FFmpeg command: {' '.join(ffmpeg_command)}")
            process = subprocess.run(ffmpeg_command, capture_output=True, text=True, check=False)

            if process.returncode != 0:
                error_message = f"😭 Oh no! Encoding failed.\n\n**FFmpeg Error:**\n```\n{process.stderr[-1000:]}\n```"
                asyncio.run(
                    send_telegram_message(bot, user_id, error_message, parse_mode=ParseMode.MARKDOWN)
                )
                return

            # 3. Upload the encoded file back to the user
            asyncio.run(
                send_telegram_message(
                    bot, user_id, "🎉 Success! Your file is encoded. Now uploading..."
                )
            )
            asyncio.run(
                send_telegram_file(
                    bot, user_id, output_path, caption=f"Here is your {quality}p encode!"
                )
            )
            asyncio.run(
                send_telegram_message(bot, user_id, "🚀 Upload complete! Job finished.")
            )

        except Exception as e:
            logging.error(f"An error occurred in the encoding task for user {user_id}: {e}")
            asyncio.run(
                send_telegram_message(bot, user_id, f"💥 A critical error occurred during your job: {e}")
            )

    logging.info(f"Finished job for user {user_id}, file_id {file_id}")
    
