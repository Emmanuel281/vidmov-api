from baseapp.config import setting
config = setting.get_settings()

class StreamingURLGenerator:
    """
    Generator untuk membuat streaming URLs
    Digunakan oleh CRUD services untuk generate URLs
    """
    
    def __init__(self, api_base_url: str = None):
        self.api_base_url = api_base_url or config.host
    
    def generate_video_url(
        self, 
        content_id: str, 
        video_type: str, 
        language: str, 
        resolution: str
    ) -> str:
        """Generate URL untuk video streaming"""
        return (
            f"{self.api_base_url}v1/stream/video/"
            f"{content_id}/{video_type}/{language}/{resolution}"
        )
    
    def generate_poster_url(self, content_id: str, language: str) -> str:
        """Generate URL untuk poster"""
        return f"{self.api_base_url}v1/stream/poster/{content_id}/{language}"
    
    def generate_logo_url(self, brand_id: str) -> str:
        """Generate URL untuk brand logo"""
        return f"{self.api_base_url}v1/stream/logo/{brand_id}"
    
    def generate_subtitle_url(self, episode_id: str, language: str) -> str:
        """Generate URL untuk subtitle"""
        return f"{self.api_base_url}v1/stream/subtitle/{episode_id}/{language}"
    
    def generate_dubbing_url(self, episode_id: str, language: str) -> str:
        """Generate URL untuk dubbing"""
        return f"{self.api_base_url}v1/stream/dubbing/{episode_id}/{language}"
    
    def generate_episode_video_url(self, episode_id: str, resolution: str = "original") -> str:
        """Generate URL untuk episode video"""
        return f"{self.api_base_url}v1/stream/episode-video/{episode_id}/{resolution}"
    
    def generate_file_url(self, filename: str) -> str:
        """Generate generic URL untuk file by filename"""
        return f"{self.api_base_url}v1/stream/file/{filename}"