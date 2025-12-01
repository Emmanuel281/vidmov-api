import logging,time
from fastapi import Request, FastAPI
from fastapi.responses import JSONResponse

from baseapp.utils.utility import generate_uuid
from baseapp.config import setting
from baseapp.model.common import ApiResponse

config = setting.get_settings()
logger = logging.getLogger()

class BusinessError(Exception):
    def __init__(self, message: str, code: int = 400):
        self.message = message
        self.code = code
    
async def handle_exceptions(request: Request, call_next):
    try:
        return await call_next(request)
    except BusinessError as be:
        # Untuk kesalahan error bisnis
        return JSONResponse(
            content=ApiResponse(status=4, message=be.message).model_dump(),
            status_code=be.code
        )
    except ValueError as ve:
        # Untuk kesalahan validasi user input
        logger.warning(f"Validation error: {str(ve)}")
        return JSONResponse(
            content=ApiResponse(status=4, message=str(ve)).model_dump(),
            status_code=400
        )
    except ConnectionError as ce:
        # Untuk kesalahan koneksi ke layanan eksternal
        logger.error(f"Connection error: {str(ce)}")
        return JSONResponse(
            content=ApiResponse(status=4, message="Service unavailable.").model_dump(),
            status_code=503
        )
    except PermissionError as pe:
        # Untuk kesalahan otorisasi
        logger.warning(f"Permission denied: {str(pe)}")
        return JSONResponse(
            content=ApiResponse(status=4, message="Access denied.").model_dump(),
            status_code=403
        )
    except Exception as e:
        # Untuk semua kesalahan lainnya
        logger.exception(f"Unhandled error: {str(e)}")
        message = "Internal server error" if config.app_env == "production" else str(e)
        return JSONResponse(
            content=ApiResponse(status=4, message=message).model_dump(),
            status_code=500
        )
    
async def add_process_time_and_log(request: Request, call_next):
    if "log_id" in request.headers:
        log_id = request.headers.get("log_id")
    else:
        log_id = generate_uuid()
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

def setup_middleware(app: FastAPI):
    app.middleware("http")(handle_exceptions)
    app.middleware("http")(add_process_time_and_log)