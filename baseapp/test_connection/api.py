from fastapi import APIRouter, Request, Depends
import logging, httpx, json, time, cbor2

from baseapp.test_connection import crud as test
from baseapp.model.common import ApiResponse, CurrentUser

from baseapp.config import setting
config = setting.get_settings()

from baseapp.utils.utility import cbor_or_json
from baseapp.utils.jwt import get_current_user

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
    
@router.get("/clickhouse")
async def test_connection_to_clickhouse() -> ApiResponse:
    resp = test.test_connection_to_clickhouse()
    return ApiResponse(status=0, message=resp)

@router.api_route("/forward/cbor/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"], response_model=ApiResponse)
async def test_api_cbor(ctx: Request, path: str) -> ApiResponse:
    # Extract incoming request details
    method = ctx.method.upper()
    headers = dict(ctx.headers)

    # Ambil query parameters dari request asli
    query_params = dict(ctx.query_params)
    forwarded_url = f"{config.host}/{path}"
    
    # Jika ada query parameters, tambahkan ke URL tujuan
    if query_params:
        encoded_query = "&".join([f"{k}={v}" for k, v in query_params.items()])
        forwarded_url += f"?{encoded_query}"

    # logger.info(f"Target URL: {forwarded_url}")

    body = await ctx.body()
    encoded_body = {}
    if body:
        # Generate `x-signature`
        minified_body = json.loads(body.decode("utf-8"))
        # Encode body ke CBOR
        encoded_body = cbor2.dumps(minified_body)
    async with httpx.AsyncClient() as client:
        try:
            # logger.debug("awal client")
            obj_headers = {"Content-Type": "application/cbor"}
            if "authorization" in headers:
                obj_headers["authorization"] = headers["authorization"]
            response = await client.request(
                method=method,
                url=forwarded_url,
                headers=obj_headers,
                data=encoded_body,
                timeout=30
            )
            # logger.debug("akhir client")
            # logger.debug(f"request headers: {obj_headers}")
            # logger.debug(f"respon status: {response.raise_for_status()}")
            # logger.debug(f"ini dia responsenya: {response.content}")
            response.raise_for_status()
            return cbor2.loads(response.content)
        except httpx.RequestError as exc:
            logger.error(f"An error occurred while requesting {exc.request.url}: {exc}")
            raise ValueError(str(exc))
        except httpx.HTTPStatusError as exc:
            logger.error(f"HTTP error occurred: {exc.response.status_code}")
            error_detail = cbor2.loads(exc.response.content)  # Decode CBOR jika response CBOR
            return error_detail
        except Exception as e:
            logger.error(f"errornya disini ya bro: {e}")
            raise ValueError(e)