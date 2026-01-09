from typing import Optional
from baseapp.config import mongodb
from baseapp.utils.logger import Logger
from baseapp.model.common import (
    DOCTYPE_POSTER,
    DOCTYPE_FYP_1,
    DOCTYPE_FYP_2,
    DOCTYPE_LOGO,
    DOCTYPE_VIDEO,
    DOCTYPE_SUBTITLE,
    DOCTYPE_DUBBING
)

logger = Logger("baseapp.services.streaming.resolver")

class MediaResolver:
    """
    Resolver untuk mencari filename dari MongoDB berdasarkan metadata
    """
    
    def __init__(self):
        self._mongo_context = None
        self.mongo = None
    
    def __enter__(self):
        self._mongo_context = mongodb.MongoConn()
        self.mongo = self._mongo_context.__enter__()
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        if self._mongo_context:
            return self._mongo_context.__exit__(exc_type, exc_value, traceback)
        return False
    
    def _find_file(
        self, 
        refkey_id: str, 
        doctype: str, 
        language: Optional[str] = None,
        resolution: Optional[str] = None
    ) -> Optional[str]:
        """
        Generic method untuk mencari file di _dmsfile collection
        """
        try:
            collection = self.mongo.get_database()['_dmsfile']
            
            # Build query
            query = {
                "refkey_id": refkey_id,
                "doctype": doctype
            }
            
            # Add metadata filters if provided
            if language:
                query["metadata.Language"] = language.upper()
            
            if resolution:
                query["metadata.Resolution"] = resolution.upper()
            
            # Find file
            file_doc = collection.find_one(query, {"filename": 1})
            
            if file_doc:
                return file_doc.get('filename')
            
            return None
        
        except Exception as e:
            logger.error(f"Error finding file: {str(e)}")
            return None
    
    def resolve_video_filename(
        self, 
        content_id: str, 
        video_type: str, 
        language: str, 
        resolution: str
    ) -> Optional[str]:
        """
        Resolve video filename
        video_type: 'fyp_1' or 'fyp_2'
        """
        # Determine doctype based on video_type
        if video_type == "fyp_1":
            doctype = DOCTYPE_FYP_1
        elif video_type == "fyp_2":
            doctype = DOCTYPE_FYP_2
        else:
            logger.error(f"Invalid video_type: {video_type}")
            return None
        
        return self._find_file(
            refkey_id=content_id,
            doctype=doctype,
            language=language,
            resolution=resolution
        )
    
    def resolve_poster_filename(
        self, 
        content_id: str, 
        language: str
    ) -> Optional[str]:
        """Resolve poster filename"""
        return self._find_file(
            refkey_id=content_id,
            doctype=DOCTYPE_POSTER,
            language=language
        )
    
    def resolve_logo_filename(self, brand_id: str) -> Optional[str]:
        """Resolve brand logo filename"""
        return self._find_file(
            refkey_id=brand_id,
            doctype=DOCTYPE_LOGO
        )
    
    def resolve_episode_video_filename(
        self, 
        episode_id: str, 
        resolution: str = "original"
    ) -> Optional[str]:
        """Resolve episode video filename"""
        return self._find_file(
            refkey_id=episode_id,
            doctype=DOCTYPE_VIDEO,
            resolution=resolution
        )
    
    def resolve_subtitle_filename(
        self, 
        episode_id: str, 
        language: str
    ) -> Optional[str]:
        """Resolve subtitle filename"""
        return self._find_file(
            refkey_id=episode_id,
            doctype=DOCTYPE_SUBTITLE,
            language=language
        )
    
    def resolve_dubbing_filename(
        self, 
        episode_id: str, 
        language: str
    ) -> Optional[str]:
        """Resolve dubbing filename"""
        return self._find_file(
            refkey_id=episode_id,
            doctype=DOCTYPE_DUBBING,
            language=language
        )
    
    def get_all_episode_video_resolutions(self, episode_id: str) -> dict:
        """
        Get all resolutions for episode video
        Returns: {resolution: filename}
        """
        try:
            collection = self.mongo.get_database()['_dmsfile']
            
            query = {
                "refkey_id": episode_id,
                "doctype": DOCTYPE_VIDEO
            }
            
            files = collection.find(query, {
                "filename": 1,
                "metadata.Resolution": 1
            })
            
            resolutions = {}
            for file_doc in files:
                metadata = file_doc.get('metadata', {})
                res = metadata.get('Resolution', 'original').lower()
                resolutions[res] = file_doc['filename']
            
            return resolutions
        
        except Exception as e:
            logger.error(f"Error getting episode video resolutions: {str(e)}")
            return {}
    
    def get_all_video_variants(
        self, 
        content_id: str, 
        video_type: str
    ) -> dict:
        """
        Get all video variants (all languages and resolutions)
        Returns: {language: {resolution: filename}}
        """
        try:
            collection = self.mongo.get_database()['_dmsfile']
            
            doctype = DOCTYPE_FYP_1 if video_type == "fyp_1" else DOCTYPE_FYP_2
            
            query = {
                "refkey_id": content_id,
                "doctype": doctype
            }
            
            files = collection.find(query, {
                "filename": 1,
                "metadata.Language": 1,
                "metadata.Resolution": 1
            })
            
            variants = {}
            for file_doc in files:
                metadata = file_doc.get('metadata', {})
                lang = metadata.get('Language', 'other').lower()
                res = metadata.get('Resolution', 'original').lower()
                
                if lang not in variants:
                    variants[lang] = {}
                
                variants[lang][res] = file_doc['filename']
            
            return variants
        
        except Exception as e:
            logger.error(f"Error getting video variants: {str(e)}")
            return {}