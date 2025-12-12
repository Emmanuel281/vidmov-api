from fastapi import APIRouter, Request, Response, Header
from typing import Optional
from datetime import datetime, timezone, timedelta

from baseapp.config import setting, redis
from baseapp.utils.utility import generate_uuid
from baseapp.utils.jwt import create_access_token, create_refresh_token
from baseapp.model.common import ApiResponse
from baseapp.utils.logger import Logger

config = setting.get_settings()

logger = Logger("baseapp.services.register.api")

from baseapp.services.register.model import Register, ResendOtpRequest, VerifyOtp

from baseapp.services.register.crud import CRUD
from baseapp.services.auth.crud import CRUD as AuthCRUD

router = APIRouter(prefix="/v1/register", tags=["Member Registration"])
    
@router.post("", response_model=ApiResponse)
async def register_member(
    ctx: Request,
    req: Register
) -> ApiResponse:
    with CRUD() as _crud:
        _crud.set_context(
            user_id=None,
            org_id=None,
            ip_address=ctx.client.host,
            user_agent=ctx.headers.get("user-agent")
        )

        response = _crud.register_member(req)

    return ApiResponse(status=0, message="Register Member", data=response)

@router.post("/resend_otp", response_model=ApiResponse)
async def register_member(
    ctx: Request,
    req: ResendOtpRequest
) -> ApiResponse:
    with CRUD() as _crud:
        _crud.set_context(
            user_id=None,
            org_id=None,
            ip_address=ctx.client.host,
            user_agent=ctx.headers.get("user-agent")
        )

        response = _crud.resend_otp(req)

    return ApiResponse(status=0, message="Resend OTP", data=response)

@router.post("/verify", response_model=ApiResponse)
async def verify(ctx: Request, response: Response, req: VerifyOtp, x_client_type: Optional[str] = Header(None)) -> ApiResponse:
    session_id = generate_uuid()

    with CRUD() as _crud:
        _crud.set_context(
            user_id=None,
            org_id=None,
            ip_address=ctx.client.host,
            user_agent=ctx.headers.get("user-agent")
        )
        verify_member = _crud.verify_member(req)
    
    # validasi user
    with AuthCRUD() as _auth_crud:
        user_info = _auth_crud.validate_user(verify_member.user.username)

    # Data token
    token_data = {
        "sub": verify_member.user.username,
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