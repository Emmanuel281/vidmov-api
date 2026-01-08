from typing import List, Optional
from fastapi import APIRouter, Query, Depends, Request, Header, HTTPException

from baseapp.config import setting
from baseapp.model.common import CurrentUser, RoleAction, Authority
from baseapp.utils.jwt import get_current_user

from baseapp.services.permission_check_service import PermissionChecker
from baseapp.services.streaming.service import StreamingService
from baseapp.services.streaming.resolver import MediaResolver
from baseapp.utils.logger import Logger

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