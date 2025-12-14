from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, Union
from fastapi import Depends, HTTPException, Request
from fastapi.security import OAuth2PasswordBearer
from jose import ExpiredSignatureError, JWTError, jwt

from baseapp.utils.utility import generate_uuid
from baseapp.model.common import CurrentUser, CurrentClient
from baseapp.config.setting import get_settings
from baseapp.config.logging import user_id_ctx
from baseapp.config.redis import RedisConn
from baseapp.utils.logger import Logger

config = get_settings()
logger = Logger("baseapp.utils.jwt")
jwt_secret_key = config.jwt_secret_key
jwt_algorithm = config.jwt_algorithm
jwt_access_expired_in = int(config.jwt_access_expired_in)
jwt_refresh_expired_in = int(config.jwt_refresh_expired_in)

def create_access_token(data: dict, expire_in: int = 60) -> tuple:
    to_encode = data.copy()
    _expire_in = expire_in if expire_in else jwt_access_expired_in
    expire = datetime.now(timezone.utc) + timedelta(minutes=_expire_in)
    to_encode.update({"exp": expire, "jti": generate_uuid()})
    token = jwt.encode(to_encode, jwt_secret_key, algorithm=jwt_algorithm)
    return token, _expire_in

def create_refresh_token(data: dict, expire_in: int = 7) -> tuple:
    to_encode = data.copy()
    _expire_in = expire_in if expire_in else jwt_refresh_expired_in
    expire = datetime.now(timezone.utc) + timedelta(days=_expire_in)
    to_encode.update({"exp": expire})
    token = jwt.encode(to_encode, jwt_secret_key, algorithm=jwt_algorithm)
    return token, _expire_in

def decode_jwt_token(token: str) -> Dict[str, Any]:
    return jwt.decode(token, jwt_secret_key, algorithms=[jwt_algorithm])

def credentials_exception(message: str):
    return HTTPException(
        status_code=401,
        detail=message,
        headers={"WWW-Authenticate": "Bearer"},
    )

Actor = Union[CurrentUser, CurrentClient]
def _get_current_user(ctx: Request, token: str = Depends(OAuth2PasswordBearer(tokenUrl="v1/auth/token"))) -> Actor:
    try:
        credentials = decode_jwt_token(token)
        user_id_ctx.set(str(credentials["id"]))
    except ExpiredSignatureError as err:
        error_message = f"_get_current_user - Log ID: , Error Code: 4, Error Message: {err=}, {type(err)=}"
        logger.error(error_message)
        raise credentials_exception(message="Token expired")
    
    except JWTError as err:
        error_message = f"_get_current_user - Log ID: , Error Code: 4, Error Message: {err=}, {type(err)=}"
        logger.error(error_message)
        raise credentials_exception(message="Could not validate credentials")

    jti = credentials.get("jti")
    if jti:
        # Cek apakah JTI ada di dalam deny list Redis
        with RedisConn() as redis_conn:
            if redis_conn.exists(f"deny_list:{jti}"):
                # Jika ada, berarti token sudah dicabut. Tolak akses.
                raise credentials_exception(message="Token has been revoked")

    if "authority" in credentials:
        return CurrentUser(
            id=credentials["id"],
            name=credentials["sub"],
            roles=credentials["roles"],
            org_id=credentials["org_id"],
            token=token,
            authority=credentials["authority"],
            features=credentials["features"],
            bitws=credentials["bitws"],
            log_id=ctx.state.log_id,
            ip_address=ctx.client.host,
            user_agent=ctx.headers.get("user-agent")
        )
    else:
        return CurrentClient(
            id=credentials["id"],
            client_id=credentials["sub"],
            org_id=credentials["org_id"],
            token=token,
            log_id=ctx.state.log_id,
            ip_address=ctx.client.host,
            user_agent=ctx.headers.get("user-agent")
        )

def get_current_user(ctx: Request, token: str = Depends(OAuth2PasswordBearer(tokenUrl="v1/auth/token"))) -> Actor:
    return _get_current_user(ctx, token)

def get_current_user_optional(ctx: Request, token: Optional[str] = Depends(OAuth2PasswordBearer(tokenUrl="v1/auth/token", auto_error=False))) -> Optional[CurrentUser]:
    if token is None:
        return None
    return _get_current_user(ctx, token)

def _perform_revoke_token(redis_conn, user_id: str):
    """Mencari dan menghapus semua refresh token milik satu user."""
    keys_to_delete = []
    cursor = 0
    pattern = f"refresh_token:{user_id}:*"
    
    while True:
        cursor, keys = redis_conn.scan(cursor=cursor, match=pattern, count=100)
        if keys:
            keys_to_delete.extend(keys)
        
        if cursor == 0:
            break
    
    if keys_to_delete:
        redis_conn.delete(*keys_to_delete)
        logger.info(f"{len(keys_to_delete)} refresh token(s) for user {user_id} have been revoked.")

def revoke_all_refresh_tokens(user_id: str, conn=None):
    """Mencabut semua refresh token untuk user tertentu."""
    if conn:
        _perform_revoke_token(conn, user_id)
    else:
        with RedisConn() as redis_conn:
            _perform_revoke_token(redis_conn, user_id)
    logger.info(f"All refresh tokens for user {user_id} have been revoked.")