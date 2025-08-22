# Telegram Video Encoder Bot

[![Deploy to Heroku](https://www.herokucdn.com/deploy/button.svg)](https://heroku.com/deploy?template=https://github.com/botty6/iencode)

# Telegram Video Encoder Bot

A powerful Telegram bot for encoding video files using FFmpeg, built with Pyrogram, Celery, and MongoDB.

## Features

-   Multi-threaded, high-speed downloads and uploads.
-   Persistent job queue and user preference storage via MongoDB.
-   Dynamic, interactive queue management (`/queue`) with job cancellation.
-   High-priority "Accelerator" lane for urgent jobs.
-   Live, 3-stage progress bars (Download, Encode, Upload).
-   Customizable branding via in-bot `/settings` command.
-   **Autonomous resource management:** Automatically scales worker performance to match the CPU cores of the deployment server.

## Deployment

### On a VPS (Recommended)

1.  **Prerequisites:** Install Python 3.10+, Redis, MongoDB, and FFmpeg on your server.
2.  **Clone the Repository:**
    ```bash
    git clone <your_repo_url>
    cd iencode-main
    ```
3.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
4.  **Configure Environment:** Create a `.env` file in the root directory and fill in your credentials:
    ```
    BOT_TOKEN=
    TELEGRAM_API_ID=
    TELEGRAM_API_HASH=
    ADMIN_USER_IDS=
    REDIS_URL=
    MONGO_URI=
    ```
5.  **Run the Bot:**
    ```bash
    python launcher.py
    ```
    The smart launcher will handle the rest, automatically configuring and starting all necessary processes based on your server's hardware. Use a tool like `tmux` or `screen` to keep it running in the background.

### On Heroku

1.  Use the "Deploy to Heroku" button or connect your GitHub repo.
2.  Set all the required environment variables in the Heroku dashboard settings.
3.  Ensure the Redis and a MongoDB add-on (like Mongo DB Atlas) are provisioned.
4.  Ensure you have one **`worker`** dyno active in the Resources tab. The `launcher.py` script will run on this single dyno and manage all processes.
