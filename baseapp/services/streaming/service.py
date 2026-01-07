from fastapi import HTTPException
from fastapi.responses import StreamingResponse, Response
from typing import Optional, Tuple
from minio.error import S3Error

from baseapp.config import setting, minio
from baseapp.utils.logger import Logger

config = setting.get_settings()
logger = Logger("baseapp.services.streaming.service")

class StreamingService:
    """
    Centralized service untuk streaming semua jenis file
    (video, poster, logo, subtitle, dubbing, dll)
    """
    
    def __init__(self):
        self.bucket_name = config.minio_bucket
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
    
    # ===== Helper Methods =====
    
    @staticmethod
    def get_content_type(filename: str) -> str:
        """Determine content type based on file extension"""
        ext = filename.lower().split('.')[-1]
        content_types = {
            # Video
            'mp4': 'video/mp4',
            'webm': 'video/webm',
            'ogg': 'video/ogg',
            'mov': 'video/quicktime',
            'm3u8': 'application/x-mpegURL',
            'ts': 'video/MP2T',
            'avi': 'video/x-msvideo',
            'flv': 'video/x-flv',
            'mkv': 'video/x-matroska',
            # Image
            'jpg': 'image/jpeg',
            'jpeg': 'image/jpeg',
            'png': 'image/png',
            'gif': 'image/gif',
            'webp': 'image/webp',
            'svg': 'image/svg+xml',
            # Subtitle
            'vtt': 'text/vtt',
            'srt': 'text/srt',
            # Audio (dubbing)
            'mp3': 'audio/mpeg',
            'wav': 'audio/wav',
            'aac': 'audio/aac',
            'm4a': 'audio/mp4',
        }
        return content_types.get(ext, 'application/octet-stream')
    
    @staticmethod
    def is_video_file(filename: str) -> bool:
        """Check if file is video"""
        video_extensions = ['mp4', 'webm', 'ogg', 'mov', 'm3u8', 'ts', 'avi', 'flv', 'mkv']
        ext = filename.lower().split('.')[-1]
        return ext in video_extensions
    
    @staticmethod
    def is_image_file(filename: str) -> bool:
        """Check if file is image"""
        image_extensions = ['jpg', 'jpeg', 'png', 'gif', 'webp', 'svg']
        ext = filename.lower().split('.')[-1]
        return ext in image_extensions
    
    def get_file_info(self, filename: str) -> Tuple[int, str]:
        """
        Get file size and content type from MinIO
        Returns: (file_size, content_type)
        """
        try:
            stat = self.minio.stat_object(self.bucket_name, filename)
            file_size = stat.size
            content_type = self.get_content_type(filename)
            return file_size, content_type
        except S3Error as e:
            logger.error(f"MinIO error getting file info for {filename}: {str(e)}")
            raise HTTPException(status_code=404, detail="File not found")
        except Exception as e:
            logger.error(f"Error getting file info for {filename}: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error")
    
    # ===== Streaming Methods =====
    
    def stream_file(
        self, 
        filename: str, 
        range_header: Optional[str] = None
    ) -> Response:
        """
        Main method untuk streaming file (auto-detect type)
        Mendukung Range requests untuk video seeking
        """
        try:
            # Get file info
            file_size, content_type = self.get_file_info(filename)
            
            # For images, always return full file
            if self.is_image_file(filename):
                return self._stream_full_file(filename, file_size, content_type, cache=True)
            
            # For videos and other files, support range requests
            if range_header:
                return self._stream_range(filename, file_size, content_type, range_header)
            else:
                return self._stream_full_file(filename, file_size, content_type, cache=False)
        
        except HTTPException:
            raise
        except Exception as e:
            logger.exception(f"Error streaming file {filename}: {str(e)}")
            raise HTTPException(status_code=500, detail="Streaming error")
    
    def _stream_full_file(
        self, 
        filename: str, 
        file_size: int, 
        content_type: str,
        cache: bool = False
    ) -> StreamingResponse:
        """Stream full file without range"""
        def iterfile():
            try:
                response = self.minio.get_object(self.bucket_name, filename)
                for chunk in response.stream(8192):  # 8KB chunks
                    yield chunk
            except Exception as e:
                logger.error(f"Error streaming file {filename}: {str(e)}")
                raise
        
        headers = {
            "Content-Length": str(file_size),
            "Accept-Ranges": "bytes",
            "Content-Disposition": "inline"
        }
        
        # Cache control based on file type
        if cache:
            headers["Cache-Control"] = "public, max-age=86400"  # 1 day for images
        else:
            headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        
        return StreamingResponse(
            iterfile(),
            media_type=content_type,
            headers=headers
        )
    
    def _stream_range(
        self, 
        filename: str, 
        file_size: int, 
        content_type: str, 
        range_header: str
    ) -> StreamingResponse:
        """Stream partial content with range support (for video seeking)"""
        try:
            # Parse range header: "bytes=0-1023" or "bytes=1024-"
            range_str = range_header.replace("bytes=", "")
            
            if "-" not in range_str:
                raise HTTPException(status_code=416, detail="Invalid range")
            
            start_str, end_str = range_str.split("-", 1)
            
            # Parse start and end
            start = int(start_str) if start_str else 0
            end = int(end_str) if end_str else file_size - 1
            
            # Validate range
            if start >= file_size or end >= file_size or start > end:
                raise HTTPException(
                    status_code=416,
                    detail="Requested range not satisfiable",
                    headers={"Content-Range": f"bytes */{file_size}"}
                )
            
            # Calculate content length
            content_length = end - start + 1
            
            # Stream with offset
            def iterfile():
                try:
                    response = self.minio.get_object(
                        self.bucket_name,
                        filename,
                        offset=start,
                        length=content_length
                    )
                    for chunk in response.stream(8192):
                        yield chunk
                except Exception as e:
                    logger.error(f"Error streaming range for {filename}: {str(e)}")
                    raise
            
            return StreamingResponse(
                iterfile(),
                media_type=content_type,
                status_code=206,  # Partial Content
                headers={
                    "Content-Range": f"bytes {start}-{end}/{file_size}",
                    "Content-Length": str(content_length),
                    "Accept-Ranges": "bytes",
                    "Cache-Control": "no-cache, no-store, must-revalidate",
                    "Content-Disposition": "inline"
                }
            )
        
        except HTTPException:
            raise
        except ValueError:
            raise HTTPException(status_code=416, detail="Invalid range format")
        except Exception as e:
            logger.exception(f"Error handling range request for {filename}: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error")