from typing import Optional, Dict, List
from datetime import timedelta
from minio.error import S3Error
from baseapp.config import setting, minio
from baseapp.utils.logger import Logger

config = setting.get_settings()
logger = Logger("baseapp.services.streaming.hls_service")


class HLSPresignedURLService:
    """
    Service for generating presigned URLs for HLS streaming
    Reusable across different APIs
    """
    
    def __init__(self, expires_seconds: int = 3600):
        """
        Initialize HLS service
        
        Args:
            expires_seconds: URL expiration time in seconds (default: 1 hour)
        """
        self.bucket_name = config.minio_bucket
        self.expires_seconds = expires_seconds
        self.expires_timedelta = timedelta(seconds=expires_seconds)
        self._minio_context = None
        self.minio = None
    
    def __enter__(self):
        self._minio_context = minio.MinioConn()
        self.minio = self._minio_context.__enter__()
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        if self._minio_context:
            return self._minio_context.__exit__(exc_type, exc_value, traceback)
        return False
    
    def get_hls_urls(
        self, 
        content_id: str,
        folder_name: Optional[str] = None
    ) -> Optional[Dict]:
        """
        Get presigned URLs for HLS video streaming
        
        Args:
            content_id: Content ID
            folder_name: Optional folder name (default: uses content_id)
        
        Returns:
            Dict with playlist_url, segments, and metadata
            None if not found
        """
        try:
            # Default folder structure: {content_id}/{folder_name}/
            if folder_name is None:
                folder_name = content_id
            
            base_path = f"{content_id}/{folder_name}"
            playlist_filename = f"{folder_name}.m3u8"
            playlist_path = f"{base_path}/{playlist_filename}"
            
            logger.info(f"[HLS] Looking for playlist: {playlist_path}")
            logger.info(f"[HLS] Bucket: {self.bucket_name}")
            
            # Check if playlist exists
            try:
                playlist_stat = self.minio.stat_object(self.bucket_name, playlist_path)
                logger.info(f"[HLS] Playlist found: {playlist_path} ({playlist_stat.size} bytes)")
            except S3Error as e:
                if e.code == 'NoSuchKey':
                    logger.warning(f"[HLS] Playlist not found: {playlist_path}")
                    logger.info(f"[HLS] Attempting to list files in folder to debug...")
                    
                    # Debug: List what's actually in the folder
                    try:
                        objects = list(self.minio.list_objects(
                            self.bucket_name,
                            prefix=f"{content_id}/",
                            recursive=True
                        ))
                        
                        if objects:
                            logger.info(f"[HLS] Found {len(objects)} files under {content_id}/:")
                            for obj in objects[:10]:  # Show first 10
                                logger.info(f"[HLS]   - {obj.object_name}")
                        else:
                            logger.warning(f"[HLS] No files found under {content_id}/")
                    except Exception as list_err:
                        logger.error(f"[HLS] Could not list files: {list_err}")
                    
                    return None
                raise
            
            # Generate presigned URL for playlist (using timedelta)
            playlist_url = self.minio.presigned_get_object(
                self.bucket_name,
                playlist_path,
                expires=self.expires_timedelta
            )
            
            # List all files in the HLS folder
            objects = self.minio.list_objects(
                self.bucket_name,
                prefix=base_path,
                recursive=True
            )
            
            segments = []
            total_size = 0
            
            for obj in objects:
                # Generate presigned URL for each file (using timedelta)
                presigned_url = self.minio.presigned_get_object(
                    self.bucket_name,
                    obj.object_name,
                    expires=self.expires_timedelta
                )
                
                file_info = {
                    "filename": obj.object_name.split('/')[-1],
                    "path": obj.object_name,
                    "url": presigned_url,
                    "size": obj.size,
                    "last_modified": obj.last_modified.isoformat() if obj.last_modified else None
                }
                
                segments.append(file_info)
                total_size += obj.size
            
            logger.info(
                f"[HLS] Generated {len(segments)} presigned URLs "
                f"(Total size: {total_size / (1024*1024):.2f} MB)"
            )
            
            return {
                "content_id": content_id,
                "folder_name": folder_name,
                "base_path": base_path,
                "playlist_url": playlist_url,
                "playlist_filename": playlist_filename,
                "segments": segments,
                "total_files": len(segments),
                "total_size_mb": round(total_size / (1024*1024), 2),
                "expires_in_seconds": self.expires_seconds,
                "expires_at": None  # Could add timestamp calculation if needed
            }
        
        except S3Error as e:
            logger.error(f"[HLS] MinIO error: {str(e)}")
            return None
        except Exception as e:
            logger.exception(f"[HLS] Unexpected error: {str(e)}")
            return None
    
    def get_hls_playlist_url_only(
        self,
        content_id: str,
        folder_name: Optional[str] = None
    ) -> Optional[str]:
        """
        Get only the presigned URL for the HLS playlist
        Faster alternative when you only need the playlist URL
        
        Args:
            content_id: Content ID
            folder_name: Optional folder name (default: uses content_id)
        
        Returns:
            Presigned playlist URL or None if not found
        """
        try:
            if folder_name is None:
                folder_name = content_id
            
            base_path = f"{content_id}/{folder_name}"
            playlist_filename = f"{folder_name}.m3u8"
            playlist_path = f"{base_path}/{playlist_filename}"
            
            # Check if exists
            try:
                self.minio.stat_object(self.bucket_name, playlist_path)
            except S3Error as e:
                if e.code == 'NoSuchKey':
                    return None
                raise
            
            # Generate presigned URL (using timedelta)
            playlist_url = self.minio.presigned_get_object(
                self.bucket_name,
                playlist_path,
                expires=self.expires_timedelta
            )
            
            return playlist_url
        
        except Exception as e:
            logger.error(f"[HLS] Error getting playlist URL: {str(e)}")
            return None
    
    def check_hls_exists(
        self,
        content_id: str,
        folder_name: Optional[str] = None
    ) -> bool:
        """
        Check if HLS files exist for given content
        
        Args:
            content_id: Content ID
            folder_name: Optional folder name
        
        Returns:
            True if playlist exists, False otherwise
        """
        try:
            if folder_name is None:
                folder_name = content_id
            
            playlist_path = f"{content_id}/{folder_name}/{folder_name}.m3u8"
            
            self.minio.stat_object(self.bucket_name, playlist_path)
            return True
        except:
            return False
    
    def get_multiple_hls_urls(
        self,
        content_ids: List[str],
        folder_names: Optional[List[str]] = None
    ) -> Dict[str, Dict]:
        """
        Get HLS URLs for multiple content IDs
        Useful for batch processing
        
        Args:
            content_ids: List of content IDs
            folder_names: Optional list of folder names (must match content_ids length)
        
        Returns:
            Dict mapping content_id to HLS URL data
        """
        results = {}
        
        if folder_names and len(folder_names) != len(content_ids):
            logger.error("folder_names length must match content_ids length")
            return results
        
        for idx, content_id in enumerate(content_ids):
            folder_name = folder_names[idx] if folder_names else None
            hls_data = self.get_hls_urls(content_id, folder_name)
            
            if hls_data:
                results[content_id] = hls_data
            else:
                results[content_id] = None
        
        return results


# Standalone function for quick use without context manager
def get_hls_presigned_urls(
    content_id: str,
    folder_name: Optional[str] = None,
    expires_seconds: int = 3600
) -> Optional[Dict]:
    """
    Standalone function to get HLS presigned URLs
    Can be imported and used anywhere
    
    Args:
        content_id: Content ID
        folder_name: Optional folder name
        expires_seconds: URL expiration time
    
    Returns:
        Dict with HLS URLs or None
    
    Example:
        from baseapp.services.streaming.hls_service import get_hls_presigned_urls
        
        urls = get_hls_presigned_urls("abc123", "abc1231", expires_seconds=7200)
        if urls:
            print(urls['playlist_url'])
    """
    with HLSPresignedURLService(expires_seconds=expires_seconds) as hls_service:
        return hls_service.get_hls_urls(content_id, folder_name)


def get_hls_playlist_url(
    content_id: str,
    folder_name: Optional[str] = None,
    expires_seconds: int = 3600
) -> Optional[str]:
    """
    Standalone function to get only the playlist URL
    Faster than getting all URLs
    
    Args:
        content_id: Content ID
        folder_name: Optional folder name
        expires_seconds: URL expiration time
    
    Returns:
        Presigned playlist URL or None
    
    Example:
        from baseapp.services.streaming.hls_service import get_hls_playlist_url
        
        url = get_hls_playlist_url("abc123", "abc1231")
        if url:
            print(f"Stream at: {url}")
    """
    with HLSPresignedURLService(expires_seconds=expires_seconds) as hls_service:
        return hls_service.get_hls_playlist_url_only(content_id, folder_name)