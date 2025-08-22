import subprocess
import json
import logging

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
