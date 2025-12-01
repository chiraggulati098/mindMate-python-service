from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import TranscriptsDisabled

def get_transcript(video_link: str) -> str:
    video_id = video_link.split("v=")[1].split("&")[0]

    # Initialize API
    ytt_api = YouTubeTranscriptApi()
    
    # Fetch transcript
    try:
        transcript = ytt_api.fetch(video_id)
        transcript_text = " ".join([entry.text for entry in transcript.snippets])
    except TranscriptsDisabled:
        transcript_text = ""
        print(f"Error fetching transcript: Transcripts are disabled for this video.")
    except Exception as e:
        transcript_text = ""
        print(f"Error fetching transcript: {e}")

    return transcript_text