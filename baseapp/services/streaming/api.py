from typing import List, Optional
from fastapi import APIRouter, Query, Depends, Request, Header, HTTPException
from fastapi.responses import StreamingResponse, Response

from baseapp.config import setting
from baseapp.model.common import CurrentUser, RoleAction, Authority
from baseapp.utils.jwt import get_current_user

from baseapp.services.permission_check_service import PermissionChecker
from baseapp.services.streaming.service import StreamingService
from baseapp.services.streaming.resolver import MediaResolver
from baseapp.utils.logger import Logger
from baseapp.config import minio, setting

from baseapp.services.streaming.hls_service import HLSPresignedURLService


config = setting.get_settings()
permission_checker = PermissionChecker()
logger = Logger("baseapp.services.streaming.resolver")
router = APIRouter(prefix="/v1/stream", tags=["Streaming"])

@router.get("/video/{content_id}/{video_type}/{language}/{resolution}")
async def stream_video(
    content_id: str,
    video_type: str,
    language: str,
    resolution: str,
    request: Request,
    range: Optional[str] = Header(None)
):
    """
    Stream video dengan metadata routing
    Mendukung Range requests untuk video seeking
    """

    try:
        with MediaResolver() as resolver:
            # Resolve filename dari metadata
            filename = resolver.resolve_video_filename(
                content_id, video_type, language, resolution
            )
            
            if not filename:
                raise HTTPException(status_code=404, detail="Video not found")
            
            # Stream file
            with StreamingService() as streaming:
                return streaming.stream_file(filename, range)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error in stream_video: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/poster/{content_id}/{language}")
async def stream_poster(
    content_id: str,
    language: str,
    request: Request
):
    """Stream poster image"""
    
    try:
        with MediaResolver() as resolver:
            filename = resolver.resolve_poster_filename(content_id, language)
            
            if not filename:
                raise HTTPException(status_code=404, detail="Poster not found")
            
            with StreamingService() as streaming:
                return streaming.stream_file(filename)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error in stream_poster: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/logo/{brand_id}")
async def stream_logo(
    brand_id: str,
    request: Request
):
    """Stream brand logo"""
    
    try:
        with MediaResolver() as resolver:
            filename = resolver.resolve_logo_filename(brand_id)
            
            if not filename:
                raise HTTPException(status_code=404, detail="Logo not found")
            
            with StreamingService() as streaming:
                return streaming.stream_file(filename)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error in stream_logo: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/subtitle/{episode_id}/{language}")
async def stream_subtitle(
    episode_id: str,
    language: str,
    request: Request
):
    """Stream subtitle file"""
    
    try:
        with MediaResolver() as resolver:
            filename = resolver.resolve_subtitle_filename(episode_id, language)
            
            if not filename:
                raise HTTPException(status_code=404, detail="Subtitle not found")
            
            with StreamingService() as streaming:
                return streaming.stream_file(filename)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error in stream_subtitle: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/dubbing/{episode_id}/{language}")
async def stream_dubbing(
    episode_id: str,
    language: str,
    request: Request
):
    """Stream dubbing audio file"""
    
    try:
        with MediaResolver() as resolver:
            filename = resolver.resolve_dubbing_filename(episode_id, language)
            
            if not filename:
                raise HTTPException(status_code=404, detail="Dubbing not found")
            
            with StreamingService() as streaming:
                return streaming.stream_file(filename)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error in stream_dubbing: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/episode-video/{episode_id}/{resolution}")
async def stream_episode_video(
    episode_id: str,
    resolution: str,
    request: Request,
    range: Optional[str] = Header(None)
):
    """
    Stream episode video dengan resolution tertentu
    Mendukung Range requests untuk video seeking
    """
    
    try:
        with MediaResolver() as resolver:
            filename = resolver.resolve_episode_video_filename(episode_id, resolution)
            
            if not filename:
                raise HTTPException(status_code=404, detail="Episode video not found")
            
            with StreamingService() as streaming:
                return streaming.stream_file(filename, range)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error in stream_episode_video: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/file/{filename:path}")
async def stream_file_direct(
    filename: str,
    request: Request,
    range: Optional[str] = Header(None)
):
    """
    Direct file streaming by filename
    CAUTION: Hanya untuk internal use atau dengan proper authentication
    """
    try:
        with StreamingService() as streaming:
            return streaming.stream_file(filename, range)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error in stream_file_direct: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/hls/{content_id}/playlist.m3u8")
async def get_hls_playlist_file(
    content_id: str,
    folder_name: Optional[str] = Query(None, description="Folder name (default: same as content_id)"),
    expires: int = Query(3600, ge=300, le=86400, description="URL expiration in seconds")
):
    """
    Get the rewritten M3U8 playlist with presigned URLs for all segments
    
    This endpoint returns the actual playlist file that video players should use.
    All segment URLs in the playlist are rewritten to use presigned URLs.
    
    Path Parameters:
        content_id: The content ID
    
    Query Parameters:
        folder_name: Optional folder name
        expires: URL expiration time in seconds
    
    Response:
        M3U8 playlist file with presigned URLs
    
    Example:
        <video>
            <source src="https://yourapi.com/v1/stream/hls/4845cbb7e2384723abeb4ff09bcbf2a/playlist.m3u8?folder_name=4845cbb7e2384723abeb4ff09bcbf2a1" type="application/x-mpegURL">
        </video>
    """
    try:
        with HLSPresignedURLService(expires_seconds=expires) as hls_service:
            playlist_content = hls_service.get_hls_playlist_content(content_id, folder_name)
            
            if not playlist_content:
                raise HTTPException(
                    status_code=404,
                    detail=f"HLS playlist not found for content_id: {content_id}"
                )
            
            return Response(
                content=playlist_content,
                media_type="application/vnd.apple.mpegurl",
                headers={
                    "Content-Type": "application/vnd.apple.mpegurl",
                    "Cache-Control": "no-cache",
                    "Access-Control-Allow-Origin": "*"
                }
            )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error serving HLS playlist: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")
    

@router.get("/hls/{content_id}")
async def get_hls_stream_urls(
    content_id: str,
    folder_name: Optional[str] = Query(None, description="Folder name (default: same as content_id)"),
    expires: int = Query(3600, ge=300, le=86400, description="URL expiration in seconds (5 min - 24 hours)")
):
    """
    Get HLS streaming information including the playlist URL
    
    Returns the URL to the rewritten playlist that contains presigned segment URLs.
    Use the playlist_url in your video player.
    
    Path Parameters:
        content_id: The content ID (e.g., "4845cbb7e2384723abeb4ff09bcbf2a")
    
    Query Parameters:
        folder_name: Optional folder name. If not provided, uses content_id as folder name.
        expires: URL expiration time in seconds (default: 3600 = 1 hour)
    
    Response:
        {
            "success": true,
            "data": {
                "content_id": "4845cbb7e2384723abeb4ff09bcbf2a",
                "playlist_url": "https://yourapi.com/v1/stream/hls/.../playlist.m3u8",
                "segments": [...],
                "total_files": 6,
                "total_size_mb": 17.99,
                "expires_in_seconds": 3600
            }
        }
    
    Example:
        GET /v1/stream/hls/4845cbb7e2384723abeb4ff09bcbf2a?folder_name=4845cbb7e2384723abeb4ff09bcbf2a1
    """
    try:
        with HLSPresignedURLService(expires_seconds=expires) as hls_service:
            result = hls_service.get_hls_urls(content_id, folder_name)
            
            if not result:
                raise HTTPException(
                    status_code=404, 
                    detail=f"HLS content not found for content_id: {content_id}"
                )
            
            # Remove the playlist_content from response (it's large)
            result_without_content = {k: v for k, v in result.items() if k != 'playlist_content'}
            
            return {
                "success": True,
                "data": result_without_content
            }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error getting HLS URLs: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/hls/{content_id}/playlist")
async def get_hls_playlist_url_only(
    content_id: str,
    folder_name: Optional[str] = Query(None, description="Folder name (default: same as content_id)"),
    expires: int = Query(3600, ge=300, le=86400, description="URL expiration in seconds")
):
    """
    Get only the presigned URL for the HLS playlist (faster endpoint)
    
    Use this when you only need the playlist URL and don't need segment details.
    This is faster than the full endpoint.
    
    Path Parameters:
        content_id: The content ID
    
    Query Parameters:
        folder_name: Optional folder name
        expires: URL expiration time in seconds
    
    Response:
        {
            "success": true,
            "data": {
                "content_id": "4845cbb7e2384723abeb4ff09bcbf2a",
                "folder_name": "4845cbb7e2384723abeb4ff09bcbf2a1",
                "playlist_url": "https://minio.gai.co.id/arena/...",
                "expires_in_seconds": 3600
            }
        }
    
    Example:
        GET /v1/stream/hls/4845cbb7e2384723abeb4ff09bcbf2a/playlist
    """
    try:
        with HLSPresignedURLService(expires_seconds=expires) as hls_service:
            playlist_url = hls_service.get_hls_playlist_url_only(content_id, folder_name)
            
            if not playlist_url:
                raise HTTPException(
                    status_code=404,
                    detail=f"HLS playlist not found for content_id: {content_id}"
                )
            
            return {
                "success": True,
                "data": {
                    "content_id": content_id,
                    "folder_name": folder_name or content_id,
                    "playlist_url": playlist_url,
                    "expires_in_seconds": expires
                }
            }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error getting HLS playlist URL: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/hls/{content_id}/check")
async def check_hls_availability(
    content_id: str,
    folder_name: Optional[str] = Query(None, description="Folder name (default: same as content_id)")
):
    """
    Check if HLS content exists and is ready to stream
    
    Useful for validating before attempting to stream or showing UI indicators.
    
    Path Parameters:
        content_id: The content ID
    
    Query Parameters:
        folder_name: Optional folder name
    
    Response:
        {
            "success": true,
            "data": {
                "content_id": "4845cbb7e2384723abeb4ff09bcbf2a",
                "folder_name": "4845cbb7e2384723abeb4ff09bcbf2a1",
                "exists": true
            }
        }
    
    Example:
        GET /v1/stream/hls/4845cbb7e2384723abeb4ff09bcbf2a/check
    """
    try:
        with HLSPresignedURLService() as hls_service:
            exists = hls_service.check_hls_exists(content_id, folder_name)
            
            return {
                "success": True,
                "data": {
                    "content_id": content_id,
                    "folder_name": folder_name or content_id,
                    "exists": exists
                }
            }
    
    except Exception as e:
        logger.exception(f"Error checking HLS existence: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/hls/batch")
async def get_batch_hls_urls(
    request: Request,
    content_ids: List[str],
    folder_names: Optional[List[str]] = None,
    expires: int = Query(3600, ge=300, le=86400, description="URL expiration in seconds")
):
    """
    Get HLS URLs for multiple content IDs in a single request
    
    Useful for batch processing or loading multiple videos at once.
    More efficient than making individual requests.
    
    Request Body:
        {
            "content_ids": ["id1", "id2", "id3"],
            "folder_names": ["folder1", "folder2", "folder3"]  // optional, must match content_ids length
        }
    
    Query Parameters:
        expires: URL expiration time in seconds
    
    Response:
        {
            "success": true,
            "data": {
                "results": {
                    "id1": {...hls_data...},
                    "id2": {...hls_data...},
                    "id3": null  // if not found
                },
                "total": 3,
                "success_count": 2,
                "failed_count": 1
            }
        }
    
    Example:
        POST /v1/stream/hls/batch
        Body: {
            "content_ids": ["4845cbb7e2384723abeb4ff09bcbf2a", "abc123def456"]
        }
    """
    try:
        if folder_names and len(folder_names) != len(content_ids):
            raise HTTPException(
                status_code=400,
                detail="folder_names length must match content_ids length"
            )
        
        with HLSPresignedURLService(expires_seconds=expires) as hls_service:
            results = hls_service.get_multiple_hls_urls(content_ids, folder_names)
            
            # Count successes and failures
            success_count = sum(1 for v in results.values() if v is not None)
            failed_count = len(results) - success_count
            
            return {
                "success": True,
                "data": {
                    "results": results,
                    "total": len(content_ids),
                    "success_count": success_count,
                    "failed_count": failed_count
                }
            }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error getting batch HLS URLs: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")
    
@router.get("/hls/{content_id}/{video_type}")
async def get_hls_master_playlist(
    content_id: str,
    video_type: str,  # fyp_1, fyp_2, or episode_id
    expires: int = Query(3600, ge=300, le=86400, description="URL expiration in seconds")
):
    """
    Get adaptive bitrate master playlist URL
    
    Path Parameters:
        content_id: Content ID
        video_type: Video type (fyp_1, fyp_2, or episode_id for episodes)
    
    Example:
        GET /v1/stream/hls/e2a64aef3e844bf394aa5ab913ad51d7/fyp_1
    
    Response:
        {
            "success": true,
            "data": {
                "master_playlist_url": "https://minio.../master.m3u8",
                "variants": {
                    "hd": {...},
                    "sd": {...}
                }
            }
        }
    """
    try:
        from datetime import timedelta
        
        with minio.MinioConn() as minio_client:
            # Master playlist path
            master_path = f"{content_id}/hls/{video_type}/master.m3u8"
            
            # Check if master exists
            try:
                minio_client.stat_object(config.minio_bucket, master_path)
            except S3Error as e:
                if e.code == 'NoSuchKey':
                    raise HTTPException(
                        status_code=404,
                        detail=f"Master playlist not found for {content_id}/{video_type}"
                    )
                raise
            
            # Generate presigned URL for master
            master_url = minio_client.presigned_get_object(
                config.minio_bucket,
                master_path,
                expires=timedelta(seconds=expires)
            )
            
            # Get all variants from MongoDB
            from baseapp.config import mongodb
            with mongodb.MongoConn() as mongo:
                collection = mongo.get_database()["_hls_conversion"]
                
                variants = {}
                conversions = collection.find({
                    "content_id": content_id,
                    "video_type": video_type,
                    "status": "completed"
                })
                
                for conv in conversions:
                    resolution = conv['resolution'].lower()
                    video_info = conv.get('video_info', {})
                    
                    # Generate presigned URL for variant playlist
                    variant_path = f"{content_id}/hls/{video_type}/{resolution}/{resolution}.m3u8"
                    variant_url = minio_client.presigned_get_object(
                        config.minio_bucket,
                        variant_path,
                        expires=timedelta(seconds=expires)
                    )
                    
                    variants[resolution] = {
                        "playlist_url": variant_url,
                        "bitrate": video_info.get('bitrate', 0),
                        "resolution": f"{video_info.get('width', 0)}x{video_info.get('height', 0)}",
                        "size_mb": conv.get('segment_count', 0) * 1.0  # Approximate
                    }
            
            return {
                "success": True,
                "data": {
                    "content_id": content_id,
                    "video_type": video_type,
                    "master_playlist_url": master_url,
                    "variants": variants,
                    "expires_in_seconds": expires
                }
            }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error getting master playlist: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")