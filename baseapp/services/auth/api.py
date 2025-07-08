from typing import Optional
from fastapi import APIRouter, Request, Response, Form, Depends, Header
from datetime import datetime, timezone, timedelta
import logging
import random

from baseapp.model.common import ApiResponse, REDIS_QUEUE_BASE_KEY, TokenResponse, CurrentUser
from baseapp.config.setting import get_settings
from baseapp.config.redis import RedisConn
from baseapp.services.redis_queue import RedisQueueManager
from baseapp.utils.jwt import create_access_token, create_refresh_token, decode_jwt_token, get_current_user
from baseapp.services.auth.model import UserLoginModel, VerifyOTPRequest
from baseapp.services.auth.crud import CRUD

config = get_settings()
_crud = CRUD()
logger = logging.getLogger()
router = APIRouter(prefix="/v1/auth", tags=["Auth"])

@router.post("/login", response_model=ApiResponse)
async def login(response: Response, req: UserLoginModel, x_client_type: Optional[str] = Header(None)) -> ApiResponse:
    username = req.username
    password = req.password

    # Validasi user
    with _crud:
        user_info = _crud.validate_user(username, password)

    # Data token
    token_data = {
        "sub": username,
        "id": user_info.id,
        "roles": user_info.roles,
        "authority": user_info.authority,
        "org_id": user_info.org_id,
        "features": user_info.feature,
        "bitws": user_info.bitws
    }

    # Buat akses token dan refresh token
    access_token, expire_access_in = create_access_token(token_data)
    refresh_token, expire_refresh_in = create_refresh_token(token_data)

    # Simpan refresh token ke Redis
    with RedisConn() as redis_conn:
        redis_conn.set(
            username,
            refresh_token,
            ex=timedelta(days=expire_refresh_in),
        )

    data = {
        "access_token": access_token
    }

    if x_client_type == 'mobile':
        data["refresh_token"] = refresh_token

    # Atur cookie refresh token for web clients
    if x_client_type == 'web':
        response.set_cookie(
            key="refresh_token",
            path="/",
            value=refresh_token,
            httponly=True,
            max_age=timedelta(days=expire_refresh_in),
            secure=config.app_env == "production",  # Gunakan secure hanya di production
            samesite="Lax",  # Prevent CSRF
            domain=config.domain
        )

    # Return response berhasil
    return ApiResponse(status=0, data=data)

@router.post("/request-otp", response_model=ApiResponse)
async def request_otp(req: UserLoginModel) -> ApiResponse:
    username = req.username
    password = req.password

    # Validasi user
    with _crud:
        _crud.validate_user(username, password)

    otp = str(random.randint(100000, 999999))  # Generate random 6-digit OTP

    # Simpan refresh token ke Redis
    with RedisConn() as redis_conn:
        redis_conn.setex(f"otp:{username}", 300, otp)
    
    queue_manager = RedisQueueManager(queue_name=REDIS_QUEUE_BASE_KEY)
    queue_manager.enqueue_task({"func":"otp","email": username, "otp": otp, "subject":"Login with OTP", "body":f"Berikut kode OTP Anda: {otp}"})

    # Return response berhasil
    return ApiResponse(status=0, data={"status": "queued", "message": "OTP has been sent"})

@router.post("/verify-otp", response_model=ApiResponse)
async def verify_otp(response: Response, req: VerifyOTPRequest, x_client_type: Optional[str] = Header(None)) -> ApiResponse:
    username = req.username
    otp = req.otp

    # Validasi user
    with _crud:
        user_info = _crud.validate_user(username)

    # Simpan refresh token ke Redis
    with RedisConn() as redis_conn:
        stored_otp = redis_conn.get(f"otp:{username}")
        if stored_otp and stored_otp == otp:
            # Data token
            token_data = {
                "sub": username,
                "id": user_info.id,
                "roles": user_info.roles,
                "authority": user_info.authority,
                "org_id": user_info.org_id,
                "features": user_info.feature,
                "bitws": user_info.bitws
            }

            # Buat akses token dan refresh token
            access_token, expire_access_in = create_access_token(token_data)
            refresh_token, expire_refresh_in = create_refresh_token(token_data)

            # Simpan refresh token ke Redis
            redis_conn.set(
                username,
                refresh_token,
                ex=timedelta(days=expire_refresh_in),
            )

            # hapus otp dari redis
            redis_conn.delete(f"otp:{username}")

            data = {
                "access_token": access_token
            }

            if x_client_type == 'mobile':
                data["refresh_token"] = refresh_token

            # Atur cookie refresh token for web clients
            if x_client_type == 'web':
                response.set_cookie(
                    key="refresh_token",
                    path="/",
                    value=refresh_token,
                    httponly=True,
                    max_age=timedelta(days=expire_refresh_in),
                    secure=config.app_env == "production",  # Gunakan secure hanya di production
                    samesite="Lax",  # Prevent CSRF
                    domain=config.domain
                )
    
            # Return response berhasil
            return ApiResponse(status=0, data=data)
        else:
            raise ValueError("Invalid or expired OTP")

@router.post("/token", response_model=TokenResponse)
async def token(
    response: Response,
    ctx: Request,
    username: str = Form(...),
    password: str = Form(...),
) -> TokenResponse:
    # Validasi user
    with _crud:
        user_info = _crud.validate_user(username, password)
    logger.debug(f"User info: {user_info}")

    # Data token
    token_data = {
        "sub": username,
        "id": user_info.id,
        "roles": user_info.roles,
        "authority": user_info.authority,
        "org_id": user_info.org_id,
        "features": user_info.feature,
        "bitws": user_info.bitws
    }

    # Buat akses token dan refresh token
    access_token, expire_access_in = create_access_token(token_data)
    refresh_token, expire_refresh_in = create_refresh_token(token_data)

    # Simpan refresh token ke Redis
    with RedisConn() as redis_conn:
        redis_conn.set(
            username,
            refresh_token,
            ex=timedelta(days=expire_refresh_in),
        )

    # Hitung waktu kedaluwarsa akses token
    expired_at = datetime.now(timezone.utc) + timedelta(minutes=float(expire_access_in))

    # Atur cookie refresh token
    response.set_cookie(
        key="refresh_token",
        path="/",
        value=refresh_token,
        httponly=True,
        max_age=timedelta(days=expire_refresh_in),
        secure=config.app_env == "production",  # Gunakan secure hanya di production
        samesite="None",  # Prevent CSRF
        domain=config.domain
    )

    # Return response berhasil
    return TokenResponse(
        access_token=access_token,
        token_type="bearer",
        expired_at=expired_at.isoformat()
    )

@router.post("/refresh-token", response_model=ApiResponse)
async def refresh_token(request: Request, x_client_type: Optional[str] = Header(None)) -> ApiResponse:
    if x_client_type == 'web':
        refresh_token = request.cookies.get("refresh_token")
    else:
        refresh_token = request.headers.get("Authorization")
        if refresh_token and refresh_token.startswith("Bearer "):
            refresh_token = refresh_token.split(" ")[1]

    if not refresh_token:
        raise ValueError("Refresh token missing")
    
    # Decode refresh token
    payload = decode_jwt_token(refresh_token)
    if not payload:
        raise ValueError("Invalid refresh token")

    # Check token in Redis
    with RedisConn() as redis_conn:
        stored_token = redis_conn.get(payload["sub"])
        if stored_token != refresh_token:
            raise ValueError("Invalid refresh token")

    # Create new access token
    access_token, expire_access_in = create_access_token(payload)
    data = {
        "access_token": access_token
    }
    return ApiResponse(status=0, data=data)
    
@router.post("/logout", response_model=ApiResponse)
async def logout(request: Request, response: Response) -> ApiResponse:
    refresh_token = request.cookies.get("refresh_token")
    if not refresh_token:
        raise ValueError("Refresh token missing")
    
    # Decode refresh token
    payload = decode_jwt_token(refresh_token)
    if not payload:
        raise ValueError("Invalid refresh token")

    # Check token in Redis
    with RedisConn() as redis_conn:
        redis_conn.delete(payload["sub"])

    # Hapus cookie di klien
    response.delete_cookie("refresh_token")

    return ApiResponse(status=0, message="Logout")

@router.post("/status", response_model=ApiResponse)
async def auth_status(request: Request, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    # Convert to dict and exclude fields
    cu_data = cu.model_dump(exclude={"log_id", "ip_address", "user_agent", "token"})
    # Return response berhasil
    return ApiResponse(status=0, data=cu_data)