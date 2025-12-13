from typing import Optional
from fastapi import APIRouter, Request, Response, Depends, Header
from datetime import datetime, timezone, timedelta
import random

from baseapp.config import setting, redis
from baseapp.model.common import ApiResponse, CurrentUser
from baseapp.utils.utility import generate_uuid
from baseapp.utils.jwt import create_access_token, create_refresh_token, decode_jwt_token, get_current_user, revoke_all_refresh_tokens
from baseapp.utils.logger import Logger

from baseapp.services.redis_queue import RedisQueueManager
from baseapp.services.auth.model import UserLoginModel, VerifyOTPRequest, ClientAuthCredential
from baseapp.services.auth.crud import CRUD

config = setting.get_settings()
logger = Logger("baseapp.services.auth.api")
router = APIRouter(prefix="/v1/auth", tags=["Auth"])

@router.post("/login", response_model=ApiResponse)
async def login(response: Response, req: UserLoginModel, x_client_type: Optional[str] = Header(None)) -> ApiResponse:
    username = req.username
    password = req.password
    session_id = generate_uuid()

    # Validasi user
    with CRUD() as _crud:
        user_info = _crud.validate_user(username, password)

    # Data token
    token_data = {
        "sub": username,
        "id": user_info.id,
        "roles": user_info.roles,
        "authority": user_info.authority,
        "org_id": user_info.org_id,
        "features": user_info.feature,
        "bitws": user_info.bitws,
        "session_id": session_id
    }

    # Buat akses token dan refresh token
    access_token, expire_access_in = create_access_token(token_data)
    refresh_token, expire_refresh_in = create_refresh_token(token_data)

    # Hitung waktu kedaluwarsa akses token
    expired_at = datetime.now(timezone.utc) + timedelta(minutes=float(expire_access_in))

    # Simpan refresh token ke Redis
    redis_key = f"refresh_token:{user_info.id}:{session_id}"
    with redis.RedisConn() as redis_conn:
        redis_conn.set(
            redis_key,
            refresh_token,
            ex=timedelta(days=expire_refresh_in),
        )

    data = {
        "access_token": access_token,
        "token_type": "bearer",
        "expired_at": expired_at.isoformat()
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
    with CRUD() as _crud:
        _crud.validate_user(username, password)

    otp = str(random.randint(100000, 999999))  # Generate random 6-digit OTP

    # Simpan refresh token ke Redis
    with redis.RedisConn() as redis_conn:
        redis_conn.setex(f"otp:{username}", 300, otp)
    
        queue_manager = RedisQueueManager(redis_conn, queue_name="otp_tasks")  # Pass actual RedisConn here
        queue_manager.enqueue_task({"email": username, "otp": otp, "subject":"Login with OTP", "body":f"Berikut kode OTP Anda: {otp}"})

    # Return response berhasil
    return ApiResponse(status=0, data={"status": "queued", "message": "OTP has been sent"})

@router.post("/verify-otp", response_model=ApiResponse)
async def verify_otp(response: Response, req: VerifyOTPRequest, x_client_type: Optional[str] = Header(None)) -> ApiResponse:
    username = req.username
    otp = req.otp
    session_id = generate_uuid()

    # Validasi user
    with CRUD() as _crud:
        user_info = _crud.validate_user(username)
    
    # Simpan refresh token ke Redis
    with redis.RedisConn() as redis_conn:
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
            "bitws": user_info.bitws,
            "session_id": session_id
        }

        # Buat akses token dan refresh token
        access_token, expire_access_in = create_access_token(token_data)
        refresh_token, expire_refresh_in = create_refresh_token(token_data)

        # Simpan refresh token ke Redis
        redis_key = f"refresh_token:{user_info.id}:{session_id}"
        redis_conn.set(
            redis_key,
            refresh_token,
            ex=timedelta(days=expire_refresh_in),
        )

        # hapus otp dari redis
        redis_conn.delete(f"otp:{username}")

        # Hitung waktu kedaluwarsa akses token
        expired_at = datetime.now(timezone.utc) + timedelta(minutes=float(expire_access_in))
        data = {
            "access_token": access_token,
            "token_type": "bearer",
            "expired_at": expired_at.isoformat(),
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
    redis_key = f"refresh_token:{payload["id"]}:{payload["session_id"]}"
    with redis.RedisConn() as redis_conn:
        stored_token = redis_conn.get(redis_key)
    
    if stored_token != refresh_token:
        raise ValueError("Invalid refresh token")

    # Create new access token
    access_token, expire_access_in = create_access_token(payload)
    expired_at = datetime.now(timezone.utc) + timedelta(minutes=float(expire_access_in))
    data = {
        "access_token": access_token, 
        "token_type": "bearer",
        "expired_at": expired_at.isoformat()
    }
    return ApiResponse(status=0, data=data)
    
@router.post("/logout", response_model=ApiResponse)
async def logout(response: Response, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    # Revoke access token
    access_token = cu.token 
    payload_access_token = decode_jwt_token(access_token)
    jti = payload_access_token.get("jti")
    exp = payload_access_token.get("exp")

    # Check token in Redis
    with redis.RedisConn() as redis_conn:
        revoke_all_refresh_tokens(cu.id, redis_conn)
        if jti and exp:
            sisa_waktu_detik = exp - datetime.now(timezone.utc).timestamp()
            if sisa_waktu_detik > 0:
                # Simpan jti ke Redis dengan TTL
                redis_conn.setex(f"deny_list:{jti}", int(sisa_waktu_detik), "revoked")

    # Hapus cookie di klien
    response.delete_cookie("refresh_token")

    return ApiResponse(status=0, message="Logout")

@router.post("/status", response_model=ApiResponse)
async def auth_status(request: Request, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    # Convert to dict and exclude fields
    cu_data = cu.model_dump(exclude={"log_id", "ip_address", "user_agent", "token"})
    # Return response berhasil
    return ApiResponse(status=0, data=cu_data)

@router.post("/client-token", response_model=ApiResponse)
async def client_credential_auth(req: ClientAuthCredential) -> ApiResponse:
    client_id = req.client_id
    client_secret = req.client_secret

    # Validasi client
    with CRUD() as _crud:
        client_info = _crud.validate_client(client_id, client_secret)

    # Data token
    token_data = {
        "sub": client_id,
        "id": client_info.id,
        "org_id": client_info.org_id
    }

    # Buat akses token dan refresh token
    access_token, expire_access_in = create_access_token(token_data,60)

    # Hitung waktu kedaluwarsa akses token
    expired_at = datetime.now(timezone.utc) + timedelta(minutes=float(expire_access_in))

    data = {
        "access_token": access_token,
        "token_type": "bearer",
        "expired_at": expired_at.isoformat()
    }

    # Return response berhasil
    return ApiResponse(status=0, data=data)