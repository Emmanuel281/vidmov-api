import time
from fastapi import Request, FastAPI
from fastapi.responses import JSONResponse

from baseapp.utils.utility import generate_uuid
from baseapp.config.logging import request_id_ctx, user_id_ctx, ip_address_ctx
from baseapp.config import setting
from baseapp.model.common import ApiResponse

from baseapp.utils.logger import Logger

config = setting.get_settings()
logger = Logger("baseapp.services.middleware")

class BusinessError(Exception):
    def __init__(self, message: str, code: int = 400):
        self.message = message
        self.code = code
    
async def handle_exceptions(request: Request, call_next):
    try:
        return await call_next(request)
    except BusinessError as be:
        # Untuk kesalahan error bisnis
        logger.warning(
            "Business error",
            path=request.url.path,
            method=request.method,
            error=be.message,
            status_code=be.code
        )
        return JSONResponse(
            content=ApiResponse(status=4, message=be.message).model_dump(),
            status_code=be.code
        )
    except ValueError as ve:
        # Untuk kesalahan validasi user input
        logger.warning(
            "Validation error",
            path=request.url.path,
            method=request.method,
            error=str(ve),
            error_type="ValueError"
        )
        return JSONResponse(
            content=ApiResponse(status=4, message=str(ve)).model_dump(),
            status_code=400
        )
    except ConnectionError as ce:
        # Untuk kesalahan koneksi ke layanan eksternal
        logger.error(
            "Connection error to external service",
            path=request.url.path,
            method=request.method,
            error=str(ce),
            error_type="ConnectionError"
        )
        return JSONResponse(
            content=ApiResponse(status=4, message="Service unavailable.").model_dump(),
            status_code=503
        )
    except PermissionError as pe:
        # Untuk kesalahan otorisasi
        logger.warning(
            "Permission denied",
            path=request.url.path,
            method=request.method,
            error=str(pe),
            error_type="PermissionError"
        )
        return JSONResponse(
            content=ApiResponse(status=4, message="Access denied.").model_dump(),
            status_code=403
        )
    except Exception as e:
        # Untuk semua kesalahan lainnya
        logger.log_error_with_context(e, {
            "path": request.url.path,
            "method": request.method,
            "query_params": dict(request.query_params),
            "headers": dict(request.headers),
            "error_type": type(e).__name__
        })
        message = "Internal server error" if config.app_env == "production" else str(e)
        return JSONResponse(
            content=ApiResponse(status=4, message=message).model_dump(),
            status_code=500
        )
    
async def add_process_time_and_log(request: Request, call_next):
    # 1. Ambil/Buat Log ID
    log_id = request.headers.get("log_id") or generate_uuid()
    
    # 2. SET CONTEXT (PENTING: Ini yang membuat log "pintar")
    req_token = request_id_ctx.set(log_id)
    ip_token = ip_address_ctx.set(request.client.host)
    # User ID default guest, nanti di-override di Auth layer jika login sukses
    user_token = user_id_ctx.set("guest") 

    request.state.log_id = log_id
    
    # Log request masuk (sekarang otomatis format JSON)
    logger.info(
        f"Incoming request: {request.method} {request.url.path}",
        method=request.method,
        path=request.url.path,
        query_params=str(dict(request.query_params)) if request.query_params else None,
        user_agent=request.headers.get("user-agent"),
        content_type=request.headers.get("content-type")
    )

    try:
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        
        response.headers["X-Process-Time"] = str(process_time)
        response.headers["X-Log-ID"] = log_id
        
        # Log response selesai
        logger.log_api_call(
            request.method,
            request.url.path,
            response.status_code,
            process_time * 1000,  # Convert to milliseconds
            log_id=log_id
        )
        
        return response
    except Exception as e:
        # Jika ada error saat processing request
        process_time = time.time() - start_time
        logger.error(
            "Request processing failed",
            method=request.method,
            path=request.url.path,
            duration_ms=process_time * 1000,
            error=str(e),
            error_type=type(e).__name__
        )
        raise
    finally:
        # 3. RESET CONTEXT (Agar tidak bocor ke request lain)
        request_id_ctx.reset(req_token)
        ip_address_ctx.reset(ip_token)
        user_id_ctx.reset(user_token)

def setup_middleware(app: FastAPI):
    app.middleware("http")(handle_exceptions)
    app.middleware("http")(add_process_time_and_log)
    logger.info("Middleware initialized", environment=config.app_env)