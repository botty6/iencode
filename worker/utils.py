import subprocess
import json
import logging
import re
import os

def get_video_info(input_path: str):
    """
    Uses ffprobe to get detailed information about a video file.
    
    Args:
        input_path: The path to the video file.
        
    Returns:
        A dictionary containing video stream information, or None if an error occurs.
    """
    ffprobe_command = [
        "ffprobe",
        "-v", "quiet",
        "-print_format", "json",
        "-show_streams",
        input_path
    ]
    
    try:
        result = subprocess.run(
            ffprobe_command,
            capture_output=True,
            text=True,
            check=True  # Will raise CalledProcessError if ffprobe fails
        )
        
        info = json.loads(result.stdout)
        
        # Find the primary video stream
        for stream in info.get("streams", []):
            if stream.get("codec_type") == "video":
                logging.info(f"Video info retrieved for {input_path}: {stream}")
                return stream # Return the first video stream found
                
        logging.warning(f"No video stream found in {input_path}")
        return None
        
    except (subprocess.CalledProcessError, json.JSONDecodeError, FileNotFoundError) as e:
        logging.error(f"Error getting video info for {input_path}: {e}")
        return None

def generate_standard_filename(original_filename: str, quality: str, brand: str) -> str:
    """
    Cleans and standardizes a video filename according to a professional format.
    
    Args:
        original_filename: The original name of the uploaded file.
        quality: The target quality (e.g., "720").
        brand: The branding text to append.
        
    Returns:
        A standardized filename string.
    """
    # 1. Remove file extension for initial processing
    clean_name = os.path.splitext(original_filename)[0]
    
    # 2. Define patterns for unwanted tags to remove (case-insensitive)
    unwanted_patterns = [
        r'\[\s*EZTVx\.to\s*\]',       # [ EZTVx.to ]
        r'\[\s*RAWR\s*\]',           # [ RAWR ]
        r'-\s*MeGusta\s*',          # - MeGusta
        r'@\w+',                    # @TelegramChannel
        r'\(.?\d{4}.?\)',              # (2023)
        r'\b(1080p|720p|480p|x264|x265|h264|h265)\b', # Common video tags
        r'\b(WEB-DL|WEBRip|BluRay)\b' # Common source tags
    ]
    
    for pattern in unwanted_patterns:
        clean_name = re.sub(pattern, '', clean_name, flags=re.IGNORECASE)
        
    # 3. Standardize Season/Episode format (e.g., "Season 1 Episode 1" -> "S01E01")
    # This regex looks for patterns like "S01E01", "s1e1", "1x01", "Season 1 Episode 1"
    match = re.search(
        r'(S|Season)\s*(\d{1,2})\s*(E|Episode)\s*(\d{1,2})', 
        clean_name, 
        re.IGNORECASE
    )
    
    season_episode_str = ""
    if match:
        season = int(match.group(2))
        episode = int(match.group(4))
        season_episode_str = f"S{season:02d}E{episode:02d}"
        
        # Remove the old season/episode text from the name
        clean_name = re.sub(r'(S|Season)\s*(\d{1,2})\s*(E|Episode)\s*(\d{1,2})', '', clean_name, flags=re.IGNORECASE)

    # 4. Clean up the name: replace dots, underscores with spaces, then collapse spaces
    clean_name = re.sub(r'[\._]', ' ', clean_name) # Replace . and _ with space
    clean_name = re.sub(r'\s+', '.', clean_name)   # Collapse multiple spaces to a single dot
    clean_name = clean_name.strip('.')             # Remove leading/trailing dots
    
    # 5. Assemble the final filename
    # Format: Destination.X.UK.S01E01.720p.10bit.WEBRip.2CH.x265-[Brand].mkv
    # Note: "10bit", "WEBRip", "2CH" are standardized placeholders as they can't be
    # easily derived from the source file. You can make these configurable later.
    final_parts = [
        clean_name,
        season_episode_str,
        f"{quality}p",
        "10bit",
        "WEBRip",
        "2CH",
        "x265"
    ]
    
    # Filter out any empty parts (like if there was no season/episode info)
    filtered_parts = [part for part in final_parts if part]
    
    base_name = ".".join(filtered_parts)
    
    final_filename = f"{base_name}-[{brand}].mkv"
    
    return final_filename
