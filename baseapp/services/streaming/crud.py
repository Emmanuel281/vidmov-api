from baseapp.services.streaming.url_generator import StreamingURLGenerator

class StreamingURLMixin:
    """
    Mixin untuk CRUD classes yang perlu generate streaming URLs
    Gunakan ini di semua CRUD yang handle media files
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url_generator = StreamingURLGenerator()
    
    def process_video_items(
        self, 
        content_id: str, 
        video_items: list, 
        video_type: str
    ) -> dict:
        """
        Process video items dengan streaming URLs
        video_type: 'fyp_1' or 'fyp_2'
        """
        grouped_video = {}
        
        for video_item in video_items:
            # Determine language and resolution from metadata
            lang_key = "other"
            res_key = "original"
            
            if "metadata" in video_item and video_item["metadata"]:
                if "Language" in video_item["metadata"]:
                    lang_key = video_item["metadata"]["Language"].lower()
                if "Resolution" in video_item["metadata"]:
                    res_key = video_item["metadata"]["Resolution"].lower()
            
            # Generate streaming URL
            video_item['url'] = self.url_generator.generate_video_url(
                content_id, video_type, lang_key, res_key
            )
            
            # Build nested dict structure
            if lang_key not in grouped_video:
                grouped_video[lang_key] = {}
            
            grouped_video[lang_key][res_key] = video_item
            
            # Clean up metadata
            video_item.pop("metadata", None)
        
        return grouped_video
    
    def process_poster_items(self, content_id: str, poster_items: list) -> dict:
        """Process poster items dengan streaming URLs"""
        grouped_poster = {}
        
        for poster_item in poster_items:
            # Determine language from metadata
            lang_key = "other"
            if "metadata" in poster_item and poster_item["metadata"]:
                if "Language" in poster_item["metadata"]:
                    lang_key = poster_item["metadata"]["Language"].lower()
            
            # Generate streaming URL
            poster_item['url'] = self.url_generator.generate_poster_url(
                content_id, lang_key
            )
            
            grouped_poster[lang_key] = poster_item
            
            # Clean up metadata
            poster_item.pop("metadata", None)
        
        return grouped_poster
    
    def process_subtitle_items(self, episode_id: str, subtitle_items: list) -> dict:
        """Process subtitle items dengan streaming URLs"""
        grouped_subtitle = {}
        
        for subtitle_item in subtitle_items:
            # Determine language from metadata
            lang_key = "other"
            if "metadata" in subtitle_item and subtitle_item["metadata"]:
                if "Language" in subtitle_item["metadata"]:
                    lang_key = subtitle_item["metadata"]["Language"].lower()
            
            # Generate streaming URL
            subtitle_item['url'] = self.url_generator.generate_subtitle_url(
                episode_id, lang_key
            )
            
            grouped_subtitle[lang_key] = subtitle_item
            
            # Clean up metadata
            subtitle_item.pop("metadata", None)
        
        return grouped_subtitle
    
    def process_dubbing_items(self, episode_id: str, dubbing_items: list) -> dict:
        """Process dubbing items dengan streaming URLs"""
        grouped_dubbing = {}
        
        for dubbing_item in dubbing_items:
            # Determine language from metadata
            lang_key = "other"
            if "metadata" in dubbing_item and dubbing_item["metadata"]:
                if "Language" in dubbing_item["metadata"]:
                    lang_key = dubbing_item["metadata"]["Language"].lower()
            
            # Generate streaming URL
            dubbing_item['url'] = self.url_generator.generate_dubbing_url(
                episode_id, lang_key
            )
            
            grouped_dubbing[lang_key] = dubbing_item
            
            # Clean up metadata
            dubbing_item.pop("metadata", None)
        
        return grouped_dubbing
    
    def process_episode_videos(self, episode_id: str, video_items: list) -> dict:
        """Process episode video items dengan berbagai resolusi"""
        grouped_video = {}
        
        for video_item in video_items:
            # Determine resolution from metadata
            res_key = "original"
            if "metadata" in video_item and video_item["metadata"]:
                if "Resolution" in video_item["metadata"]:
                    res_key = video_item["metadata"]["Resolution"].lower()
            
            # Generate streaming URL for episode video
            video_item['url'] = self.url_generator.generate_episode_video_url(
                episode_id, res_key
            )
            
            grouped_video[res_key] = video_item
            
            # Clean up metadata
            video_item.pop("metadata", None)
        
        return grouped_video
    
    def add_logo_url(self, sponsor_data: dict) -> dict:
        """Add logo URL to sponsor data"""
        if sponsor_data and sponsor_data.get("logo"):
            logo_data = sponsor_data["logo"]
            brand_id = sponsor_data.get("brand_id") or sponsor_data.get("_id")
            
            if brand_id:
                sponsor_data["logo_url"] = self.url_generator.generate_logo_url(
                    str(brand_id)
                )
        
        return sponsor_data