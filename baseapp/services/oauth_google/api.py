import base64
from datetime import datetime, timedelta, timezone
import json
from urllib import parse
import requests
from fastapi import APIRouter, Response, Query, Depends
from fastapi.responses import RedirectResponse

from baseapp.config import setting, redis
from baseapp.model.common import ApiResponse, CurrentUser
from baseapp.utils.utility import generate_uuid
from baseapp.utils.jwt import create_access_token, create_refresh_token, get_current_user
from baseapp.utils.logger import Logger

from baseapp.services.oauth_google.model import GoogleToken
from baseapp.services.oauth_google.crud import CRUD
from baseapp.services.auth.crud import CRUD as UserCrud

config = setting.get_settings()
logger = Logger("baseapp.services.oauth_google.api")
router = APIRouter(prefix="/v1/oauth", tags=["Oauth"])

# FUNCTION REFRESH TOKEN GOOGLE 
def refreshToken(session):
    postData = parse.urlencode({
        "refresh_token": session['refresh_token'],
        "client_id": config.google_client_id,
        "client_secret": config.google_client_secret,
        "redirect_uri": config.google_redirect_uri,
        "grant_type": 'refresh_token'
    })

    path = f'/token'

    options = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': f"{len(postData)}"
    }
    try:
        res = requests.post(f'https://oauth2.googleapis.com:443{path}', data=postData, headers=options)
    except requests.exceptions.RequestException as e:  # This is the correct syntax
        raise SystemExit(e)

    getRes = res.json()
    return getRes

@router.get("/google/callback")
async def auth_google_callback(
    code: str = Query(None),
    error: str = Query(None),
    state: str = Query(None)
):
    flutter_redirect = parse.unquote(state)
    state_data = json.loads(base64.b64decode(flutter_redirect).decode('utf-8'))
    flutter_redirect = parse.unquote(state_data.get('redirect'))

    if error:
        params = parse.urlencode({'error': error})
        return RedirectResponse(url=f"{flutter_redirect}?{params}")
    
    if not code:
        params = parse.urlencode({'error': 'Authorization code required'})
        return RedirectResponse(url=f"{flutter_redirect}?{params}")
    
    try:
        logger.debug("API LOGIN GOOGLE")
        logger.debug(f"Redirect Flutter {flutter_redirect}")
        
        # 1. Dapatkan token dari Google
        with CRUD() as _crud:
            resGoogle = _crud.login_google(code)

        if not "access_token" in resGoogle:
            params = parse.urlencode({'error': 'Failed to get access token'})
            return RedirectResponse(url=f"{flutter_redirect}?{params}")

        oauth_data = {
            'access_token': resGoogle['access_token']
        }

        logger.debug(f"Request link google account: {oauth_data}")

        params = parse.urlencode(oauth_data)
        return RedirectResponse(url=f"{flutter_redirect}?{params}")
    except Exception as e:
        params = parse.urlencode({'error': str(e)})
        return RedirectResponse(url=f"{flutter_redirect}?{params}")

@router.put("/link-google-account", response_model=ApiResponse)
async def link_google_account(req: GoogleToken, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    with CRUD() as _crud:
        _crud.set_context(
            user_id=cu.id,
            org_id=cu.org_id,
            ip_address=cu.ip_address,  # Jika ada
            user_agent=cu.user_agent   # Jika ada
        )
        response = _crud.link_to_google(req)
    return ApiResponse(status=0, message="Your account has been linked to a Google account.", data=response)

@router.delete("/unlink-google-account", response_model=ApiResponse)
async def unlink_google_account(cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    with CRUD() as _crud:
        _crud.set_context(
            user_id=cu.id,
            org_id=cu.org_id,
            ip_address=cu.ip_address,  # Jika ada
            user_agent=cu.user_agent   # Jika ada
        )
        response = _crud.unlink_to_google()
    return ApiResponse(status=0, message="Google account was removed from your account.", data=response)

@router.post("/login-google-account", response_model=ApiResponse)
async def login_google_account(response: Response, req: GoogleToken) -> ApiResponse:
    # Validasi user
    with CRUD() as _crud:
        user = _crud.get_by_google_id(req)
    
    with UserCrud() as _user_crud:
        user_info = _user_crud.validate_user(user["username"])
    
    # Simpan refresh token ke Redis
    # Data token
    token_data = {
        "sub": user["username"],
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
    session_id = generate_uuid()
    redis_key = f"refresh_token:{user_info.id}:{session_id}"
    with redis.RedisConn() as redis_conn:
        redis_conn.set(
            redis_key,
            refresh_token,
            ex=timedelta(days=expire_refresh_in),
        )
        
    # Hitung waktu kedaluwarsa akses token
    expired_at = datetime.now(timezone.utc) + timedelta(minutes=float(expire_access_in))
    data = {
        "access_token": access_token,
        "token_type": "bearer",
        "expired_at": expired_at.isoformat(),
    }

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
    return ApiResponse(status=0, data=data)