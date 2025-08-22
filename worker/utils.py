import subprocess
import json
import logging
import re
import os

def get_video_info(input_path: str):
    """
    Uses ffprobe to get detailed information about a video file, including a reliable duration.
    """
    ffprobe_command = [
        "ffprobe",
        "-v", "quiet",
        "-print_format", "json",
        "-show_streams",
        "-show_format",
        input_path
    ]
    
    try:
        result = subprocess.run(
            ffprobe_command,
            capture_output=True,
            text=True,
            check=True
        )
        
        info = json.loads(result.stdout)
        
        video_stream = None
        for stream in info.get("streams", []):
            if stream.get("codec_type") == "video":
                video_stream = stream
                break
                
        if not video_stream:
            logging.warning(f"No video stream found in {input_path}")
            return None

        duration = video_stream.get("duration")
        
        if not duration:
            duration = info.get("format", {}).get("duration")
            
        video_stream['duration'] = duration
        
        return video_stream
        
    except (subprocess.CalledProcessError, json.JSONDecodeError, FileNotFoundError) as e:
        logging.error(f"Error getting video info for {input_path}: {e}")
        return None

def generate_standard_filename(original_filename: str, quality: str, brand: str) -> str:
    """
    Cleans and standardizes a video filename.
    """
    clean_name = os.path.splitext(original_filename)[0]
    
    unwanted_patterns = [
        r'\[\s*EZTVx\.to\s*\]', r'\[\s*RAWR\s*\]', r'-\s*MeGusta\s*',
        r'@\w+', r'\(.?\d{4}.?\)', r'\b(1080p|720p|480p|x264|x265|h264|h265)\b',
        r'\b(WEB-DL|WEBRip|BluRay)\b'
    ]
    
    for pattern in unwanted_patterns:
        clean_name = re.sub(pattern, '', clean_name, flags=re.IGNORECASE)
        
    match = re.search(r'(S|Season)\s*(\d{1,2})\s*(E|Episode)\s*(\d{1,2})', clean_name, re.IGNORECASE)
    
    season_episode_str = ""
    if match:
        season = int(match.group(2))
        episode = int(match.group(4))
        season_episode_str = f"S{season:02d}E{episode:02d}"
        clean_name = re.sub(r'(S|Season)\s*(\d{1,2})\s*(E|Episode)\s*(\d{1,2})', '', clean_name, flags=re.IGNORECASE)

    clean_name = re.sub(r'[\._]', ' ', clean_name)
    clean_name = re.sub(r'\s+', '.', clean_name)
    clean_name = clean_name.strip('.')
    
    final_parts = [clean_name, season_episode_str, f"{quality}p", "10bit", "WEBRip", "2CH", "x265"]
    filtered_parts = [part for part in final_parts if part]
    base_name = ".".join(filtered_parts)
    
    return f"{base_name}-[{brand}].mkv"

def create_progress_bar(current, total, bar_length=20):
    """Creates a text-based progress bar string."""
    percent = float(current) * 100 / float(total)
    arrow = '█' * int(percent/100 * bar_length)
    spaces = '░' * (bar_length - len(arrow))
    return f"[{arrow}{spaces}] {percent:.2f}%"

# --- UPDATED: humanbytes can now format speed ---
def humanbytes(size, speed=False):
    """Converts bytes to a human-readable format, optionally as a speed."""
    if not size:
        return ""
    power = 1024 # Use 1024 for storage/speed
    n = 0
    power_labels = {0: '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power:
        size /= power
        n += 1
    
    suffix = 'B/s' if speed else 'B'
    return f"{size:.2f} {power_labels[n]}{suffix}"
