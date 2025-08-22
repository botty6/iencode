import os
from pymongo import MongoClient, errors
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

# --- Database Configuration ---
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise Exception("CRITICAL: MONGO_URI environment variable is not set!")

# --- NEW: Allow custom database name from URI ---
parsed_uri = urlparse(MONGO_URI)
db_name = parsed_uri.path.lstrip('/') or "iencode_bot" # Default to 'iencode_bot' if no path is specified

try:
    client = MongoClient(MONGO_URI)
    db = client[db_name]
    # The ismaster command is cheap and does not require auth.
    client.admin.command('ismaster')
    print("✅ MongoDB connection successful.")
except errors.ConnectionFailure as e:
    raise Exception(f"❌ Could not connect to MongoDB: {e}")


users_collection = db.users
jobs_collection = db.jobs

def get_user_settings(user_id: int):
    """Fetches a user's settings, returning global defaults if none are found."""
    user_data = users_collection.find_one({"user_id": user_id})
    # Fallback to environment variables for defaults
    default_brand = os.getenv("BRANDING_TEXT", "MyEnc")
    default_website = os.getenv("BRANDING_WEBSITE", "t.me/YourChannel")

    if not user_data:
        return {
            "brand_name": default_brand,
            "website": default_website,
            "custom_thumbnail_id": None
        }
    
    # Ensure all keys are present, falling back to defaults
    settings = user_data.get("settings", {})
    settings.setdefault("brand_name", default_brand)
    settings.setdefault("website", default_website)
    settings.setdefault("custom_thumbnail_id", None)
    return settings

def update_user_setting(user_id: int, key: str, value):
    """Updates a specific setting for a user."""
    users_collection.update_one(
        {"user_id": user_id},
        {"$set": {f"settings.{key}": value}},
        upsert=True
    )

def add_job(task_id: str, user_id: int, filename: str, status: str, status_message_id: int, task_args: tuple):
    """Adds or updates a job in the database, using task_id as the unique identifier."""
    jobs_collection.update_one(
        {"task_id": task_id},
        {"$set": {
            "user_id": user_id,
            "filename": filename,
            "status": status,
            "status_message_id": status_message_id,
            "task_args": task_args
        }},
        upsert=True
    )

def get_job(task_id: str):
    """Retrieves a job from the database by its Celery task ID."""
    return jobs_collection.find_one({"task_id": task_id})

def update_job_status(task_id: str, status: str):
    """Updates the status of an existing job."""
    jobs_collection.update_one({"task_id": task_id}, {"$set": {"status": status}})

def remove_job(task_id: str):
    """Removes a job from the database."""
    jobs_collection.delete_one({"task_id": task_id})

def get_user_jobs(user_id: int):
    """Gets all active (non-completed) jobs for a user."""
    return list(jobs_collection.find({"user_id": user_id}))
