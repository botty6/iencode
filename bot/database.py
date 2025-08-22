import os
from pymongo import MongoClient, errors
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

# --- Database Configuration ---
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise Exception("CRITICAL: MONGO_URI environment variable is not set!")

try:
    parsed_uri = urlparse(MONGO_URI)
    db_name = parsed_uri.path.lstrip('/') or "iencode_bot" 

    client = MongoClient(MONGO_URI)
    db = client[db_name]
    
    client.admin.command('ismaster')
    print(f"✅ MongoDB connection successful. Using database: '{db_name}'")

    # --- NEW: Create indexes for faster queries ---
    db.jobs.create_index("task_id", unique=True)
    db.jobs.create_index("user_id")

except errors.ConnectionFailure as e:
    raise Exception(f"❌ Could not connect to MongoDB: {e}")
except Exception as e:
    raise Exception(f"❌ An error occurred during MongoDB setup: {e}")


users_collection = db.users
jobs_collection = db.jobs

# --- User Settings Functions (Unchanged) ---

def get_user_settings(user_id: int):
    """Fetches a user's settings, returning global defaults if none are found."""
    user_data = users_collection.find_one({"user_id": user_id})
    
    default_brand = os.getenv("BRANDING_TEXT", "MyEnc")
    default_website = os.getenv("BRANDING_WEBSITE", "t.me/YourChannel")

    if not user_data:
        return {
            "brand_name": default_brand,
            "website": default_website,
            "custom_thumbnail_id": None
        }
    
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

# --- REFACTORED: Job Management Functions ---

def add_job(task_id: str, user_id: int, filename: str, status_message_id: int, job_data: dict):
    """
    Adds or updates a job, storing all its data for potential restarts or state changes.
    'job_data' should contain everything needed to run the task (e.g., message_ids, quality, preset).
    """
    jobs_collection.update_one(
        {"task_id": task_id},
        {"$set": {
            "user_id": user_id,
            "filename": filename,
            "status": "QUEUED",  # Start with a clear initial state
            "status_message_id": status_message_id,
            "job_data": job_data
        }},
        upsert=True
    )

def get_job(task_id: str):
    """Retrieves a job from the database by its Celery task ID."""
    return jobs_collection.find_one({"task_id": task_id})

def update_job_status(task_id: str, status: str):
    """
    Updates the status of an existing job (e.g., DOWNLOADING, ENCODING, FAILED, CANCELLED).
    """
    jobs_collection.update_one({"task_id": task_id}, {"$set": {"status": status}})

def remove_job(task_id: str):
    """Removes a job from the database entirely."""
    jobs_collection.delete_one({"task_id": task_id})

def get_user_jobs(user_id: int):
    """Gets all jobs for a user that are not in a final state."""
    # Exclude jobs that are finished, failed, or cancelled from the main /queue view
    final_states = ["COMPLETED", "FAILED", "CANCELLED"]
    return list(jobs_collection.find({"user_id": user_id, "status": {"$nin": final_states}}))

def cleanup_old_jobs():
    """
    NEW: Removes old, completed/failed/cancelled jobs from the database.
    This can be called periodically to keep the collection clean.
    """
    # In a real-world scenario, you might filter by date.
    # For now, we remove all jobs that are in a final state.
    final_states = ["COMPLETED", "FAILED", "CANCELLED"]
    jobs_collection.delete_many({"status": {"$in": final_states}})
    
