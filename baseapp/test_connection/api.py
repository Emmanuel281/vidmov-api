from fastapi import APIRouter
import logging

from baseapp.test_connection import crud as test
from baseapp.model.common import ApiResponse

from baseapp.config import setting

config = setting.get_settings()
logger = logging.getLogger()

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