import logging,uuid,time,cbor2
from fastapi import Request, FastAPI, HTTPException
from fastapi.responses import JSONResponse, Response

from baseapp.config import setting
from baseapp.model.common import ApiResponse

from baseapp.utils.utility import get_response_based_on_env

config = setting.get_settings()
logger = logging.getLogger()

class BusinessError(Exception):
    def __init__(self, message: str, code: int = 400):
        self.message = message
        self.code = code

async def http_exception_handler(request: Request, exc: HTTPException):
    message = (
        exc.detail
        if isinstance(exc.detail, str)
        else exc.detail.get("message", "Error occurred")
        if isinstance(exc.detail, dict)
        else str(exc.detail)
    )
    # Log error dengan konteks
    logger.error(
        f"HTTPError in {request.url.path}: {message}",
        extra={"status_code": exc.status_code, "headers": dict(exc.headers)},
    )
    # Gunakan helper untuk membuat response
    return _make_error_response(message, exc.status_code, request, exc.headers)
    
async def handle_exceptions(request: Request, call_next):
    try:
        return await call_next(request)
    except BusinessError as be:
        # Untuk kesalahan error bisnis
        content = ApiResponse(status=4, message=be.message)
        return get_response_based_on_env(content, status_code=be.code, request=request)
    except ValueError as ve:
        # Untuk kesalahan validasi user input
        logger.warning(f"Validation error: {str(ve)}")
        return _make_error_response(str(ve), 400, request)
    except ConnectionError as ce:
        # Untuk kesalahan koneksi ke layanan eksternal
        logger.error(f"Connection error: {str(ce)}")
        return _make_error_response("Service unavailable.", 503, request)
    except PermissionError as pe:
        # Untuk kesalahan otorisasi
        logger.warning(f"Permission denied: {str(pe)}")
        return _make_error_response("Access denied.", 403, request)
    except Exception as e:
        # Untuk semua kesalahan lainnya
        logger.exception(f"Unhandled error: {str(e)}")
        message = "Internal server error" if config.app_env == "production" else str(e)
        return _make_error_response(message, 500, request)
    
async def add_process_time_and_log(request: Request, call_next):
    if "log_id" in request.headers:
        log_id = request.headers.get("log_id")
    else:
        log_id = str(uuid.uuid4()).replace('-', '')
    request.state.log_id = log_id
    log_request = {
        "log_id": log_id,
        "method": request.method,
        "url": request.url.path,
    }
    logging.info(f"request: {log_request}")

    start_time = time.time()
    response = await call_next(request)

    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    log_response = {
        "log_id": log_id,
        "process_time": str(process_time),
        "http_status_code": response.status_code
    }
    logging.info(f"response: {log_response}")
    return response

async def force_json_middleware(request: Request, call_next):
    path_url = [
        "/v1/auth/login",
        "/v1/auth/request-otp",
        "/v1/auth/verify-otp",
        "/v1/auth/token",
        "/v1/auth/refresh-token",
        "/v1/auth/logout",
        "/v1/auth/status",

        "/v1/_dms/upload",

        "/v1/oauth/link-google-account",
        "/v1/oauth/unlink-google-account",
        "/v1/oauth/login-google-account",
    ]
    # Cek jika path termasuk yang perlu force JSON
    if request.url.path in path_url:  # Tambahkan path lain jika perlu
        request.state.force_json = True
    
    response = await call_next(request)
    return response

def _make_error_response(
    message: str, 
    status_code: int, 
    request: Request, 
    headers: dict = None
):
    content = ApiResponse(status=4, message=message)
    response = get_response_based_on_env(content, status_code, request)
    if headers:
        response.headers.update(headers)
    return response

def setup_middleware(app: FastAPI):
    app.middleware("http")(handle_exceptions)
    app.middleware("http")(force_json_middleware)
    app.middleware("http")(add_process_time_and_log)
    app.add_exception_handler(HTTPException, http_exception_handler)