from fastapi import APIRouter
import logging

from baseapp.test_connection import crud as test
from baseapp.model.common import ApiResponse

from baseapp.config import setting
from baseapp.utils.logger import Logger

config = setting.get_settings()
logger = Logger("baseapp.test_connection.api")

router = APIRouter(prefix="/v1/test", tags=["Test Connection"])

@router.get("/database", response_model=ApiResponse)
async def test_connection_to_database() -> ApiResponse:
    resp = test.test_connection_to_mongodb()
    return ApiResponse(status=0, message=resp)

@router.get("/redis")
async def test_connection_to_redis() -> ApiResponse:
    resp = test.test_connection_to_redis()
    return ApiResponse(status=0, message=resp)
    
@router.get("/minio")
async def test_connection_to_minio() -> ApiResponse:
    resp = test.test_connection_to_minio()
    return ApiResponse(status=0, message=resp)

@router.get("/rabbit")
async def test_connection_to_rabbit() -> ApiResponse:
    resp = test.test_connection_to_rabbit()
    return ApiResponse(status=0, message=resp)

@router.get("/redis-worker")
async def test_redis_worker() -> ApiResponse:
    resp = test.test_redis_worker()
    return ApiResponse(status=0, message=resp)

@router.get("/redis-worker-video-convert")
async def test_redis_video_worker() -> ApiResponse:
    resp = test.test_redis_video_worker()
    return ApiResponse(status=0, message=resp)

@router.get("/all", response_model=ApiResponse)
async def test_all_connections() -> ApiResponse:
    results = {}
    try:
        results['mongodb'] = test.test_connection_to_mongodb()
    except Exception as e:
        logger.error(f"MongoDB connection test failed: {e}")
        results['mongodb'] = f"Error: {str(e)}"
    
    try:
        results['redis'] = test.test_connection_to_redis()
    except Exception as e:
        logger.error(f"Redis connection test failed: {e}")
        results['redis'] = f"Error: {str(e)}"
    
    try:
        results['minio'] = test.test_connection_to_minio()
    except Exception as e:
        logger.error(f"Minio connection test failed: {e}")
        results['minio'] = f"Error: {str(e)}"
    
    return ApiResponse(status=0, message="All connection tests completed.", data=results)