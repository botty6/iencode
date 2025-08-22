import subprocess
import json
import logging
import re
import os

def get_video_info(input_path: str):
    """
    REFACTORED: Uses ffprobe to get a rich set of accurate video and audio properties.
    """
    ffprobe_command = [
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_streams", "-show_format", input_path
    ]
    
    try:
        result = subprocess.run(ffprobe_command, capture_output=True, text=True, check=True)
        info = json.loads(result.stdout)
        
        video_stream = None
        audio_stream = None
        for stream in info.get("streams", []):
            if stream.get("codec_type") == "video":
                video_stream = stream
            elif stream.get("codec_type") == "audio":
                audio_stream = stream
        
        if not video_stream:
            logging.warning(f"No video stream found in {input_path}")
            return None

        # --- Property Extraction ---
        height = int(video_stream.get("height", 0))
        codec_name = video_stream.get("codec_name", "x264")
        # Check for 10-bit color. 'yuv420p10le' is a common 10-bit format for HEVC.
        is_10bit = "p10" in video_stream.get("pix_fmt", "")
        
        audio_channels = 0
        if audio_stream:
            audio_channels = int(audio_stream.get("channels", 0))

        duration_str = video_stream.get("duration") or info.get("format", {}).get("duration")
        try:
            duration = float(duration_str)
        except (ValueError, TypeError):
            duration = 0.0

        return {
            "height": height,
            "duration": duration,
            "codec_name": codec_name,
            "is_10bit": is_10bit,
            "audio_channels": audio_channels
        }
        
    except (subprocess.CalledProcessError, json.JSONDecodeError, FileNotFoundError, TypeError) as e:
        logging.error(f"Error getting comprehensive video info for {input_path}: {e}")
        return None


def generate_standard_filename(original_filename: str, quality: str, brand: str, video_info: dict) -> str:
    """
    REFACTORED: Dynamically builds a filename using accurately detected video properties.
    """
    clean_name = os.path.splitext(original_filename)[0]
    
    # More aggressive cleaning for release-style names
    unwanted_patterns = [
        r'\[.*?\]', r'\(.*?\)', r'\{.*?\}', r'@\w+',
        r'\b(1080p|720p|480p|x264|x265|h264|h265|hevc|web-dl|webrip|bluray|hdrip|bdrip)\b'
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

    clean_name = re.sub(r'[\._\-\s]+', '.', clean_name).strip('.')
    
    # --- Dynamic Property Tagging ---
    properties = []
    
    # Quality
    properties.append(f"{quality}p")
    
    # 10-bit Tag
    if video_info.get("is_10bit", False):
        properties.append("10bit")
    
    # Source Tag (Educated Guess)
    if re.search(r'\b(bluray|blu-ray|bdrip)\b', original_filename, re.IGNORECASE):
        properties.append("BluRay")
    else:
        properties.append("WEBRip") # Default to WEBRip as it's more common
        
    # Audio Channel Tag
    channels = video_info.get("audio_channels", 0)
    if channels > 0:
        properties.append(f"{channels}CH")
    
    # Codec Tag
    properties.append("x265.HEVC")
    
    # Brand Tag
    brand_tag = f"-[{brand}]"
    
    final_parts = [clean_name, season_episode_str] + properties
    base_name = ".".join(filter(None, final_parts))
    
    return f"{base_name}{brand_tag}.mkv"


def generate_thumbnail(video_path: str, job_cache_dir: str) -> str | None:
    """Generates a validated thumbnail from the video file."""
    thumb_path = os.path.join(job_cache_dir, "thumb.jpg")
    command = ["ffmpeg", "-y", "-i", video_path, "-ss", "1", "-frames:v", "1", "-vf", "scale=320:-1", thumb_path]
    try:
        subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if os.path.exists(thumb_path) and 0 < os.path.getsize(thumb_path) <= 200 * 1024:
            return thumb_path
        elif os.path.exists(thumb_path):
            os.remove(thumb_path)
        return None
    except (subprocess.CalledProcessError, FileNotFoundError, OSError) as e:
        logging.error(f"Thumbnail generation failed: {e}")
        return None

def create_progress_bar(current, total, bar_length=20):
    if total == 0: return f"[{'░' * bar_length}] 0.00%"
    percent = float(current) * 100 / float(total)
    arrow = '█' * int(percent/100 * bar_length)
    spaces = '░' * (bar_length - len(arrow))
    return f"[{arrow}{spaces}] {percent:.2f}%"

def humanbytes(size, speed=False):
    if not size: return ""
    power = 1024
    n = 0
    power_labels = {0: '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size >= power:
        size /= power
        n += 1
    suffix = 'B/s' if speed else 'B'
    return f"{size:.2f} {power_labels[n]}{suffix}"
    
