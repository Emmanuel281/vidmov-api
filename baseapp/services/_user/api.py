from fastapi import APIRouter, Query, Depends, Response
from typing import Optional, List
from datetime import datetime, timezone

from baseapp.config import setting, redis
from baseapp.model.common import ApiResponse, CurrentUser, Status, UpdateStatus, RoleAction
from baseapp.utils.jwt import get_current_user, decode_jwt_token, revoke_all_refresh_tokens

from baseapp.services.permission_check_service import PermissionChecker
from baseapp.services._user import model
from baseapp.services._user.crud import CRUD


config = setting.get_settings()
permission_checker = PermissionChecker()
router = APIRouter(prefix="/v1/_user", tags=["User"])

@router.post("/create", response_model=ApiResponse)
async def create(req: model.User, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    
    with CRUD() as _crud:
        if not permission_checker.has_permission(cu.roles, "_user", RoleAction.ADD.value, mongo_conn=_crud.mongo):  # 2 untuk izin simpan baru
            raise PermissionError("Access denied")
        _crud.set_context(
            user_id=cu.id,
            org_id=cu.org_id,
            ip_address=cu.ip_address,  # Jika ada
            user_agent=cu.user_agent   # Jika ada
        )

        response = _crud.create(req)
    return ApiResponse(status=0, message="Data created", data=response)
    
@router.put("/update/{user_id}", response_model=ApiResponse)
async def update_by_admin(user_id: str, req: model.UpdateByAdmin, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    
    with CRUD() as _crud:
        if not permission_checker.has_permission(cu.roles, "_user", RoleAction.EDIT.value, mongo_conn=_crud.mongo):  # 4 untuk izin simpan perubahan
            raise PermissionError("Access denied")
        _crud.set_context(
            user_id=cu.id,
            org_id=cu.org_id,
            ip_address=cu.ip_address,  # Jika ada
            user_agent=cu.user_agent   # Jika ada
        )

        response = _crud.update_all_by_admin(user_id,req)
    return ApiResponse(status=0, message="Data updated", data=response)

@router.delete("/delete/{user_id}", response_model=ApiResponse)
async def update_status(user_id: str, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    
    with CRUD() as _crud:
        if not permission_checker.has_permission(cu.roles, "_user", RoleAction.EDIT.value, mongo_conn=_crud.mongo):  # 4 untuk izin simpan perubahan
            raise PermissionError("Access denied")
        _crud.set_context(
            user_id=cu.id,
            org_id=cu.org_id,
            ip_address=cu.ip_address,  # Jika ada
            user_agent=cu.user_agent   # Jika ada
        )
        
        # Buat instance model langsung
        manual_data = UpdateStatus(
            status=Status.DELETED  # nilai yang Anda tentukan
        )
        response = _crud.update_status(user_id,manual_data)
    return ApiResponse(status=0, message="Data deleted", data=response)

@router.put("/change_password", response_model=ApiResponse)
async def update_change_password(req: model.ChangePassword, response: Response, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    
    with CRUD() as _crud:
        if not (permission_checker.has_permission(cu.roles, "_user", RoleAction.EDIT.value, mongo_conn=_crud.mongo) or permission_checker.has_permission(cu.roles, "_myprofile", RoleAction.EDIT.value, mongo_conn=_crud.mongo)):  # 4 untuk izin simpan perubahan
            raise PermissionError("Access denied")
        _crud.set_context(
            user_id=cu.id,
            org_id=cu.org_id,
            ip_address=cu.ip_address,  # Jika ada
            user_agent=cu.user_agent   # Jika ada
        )

        result = _crud.change_password(req)

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

    return ApiResponse(status=0, message="Password has change", data=result)

@router.put("/reset_password/{user_id}", response_model=ApiResponse)
async def update_reset_passowrd(user_id: str, req: model.ResetPassword, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    
    with CRUD() as _crud:
        if not permission_checker.has_permission(cu.roles, "_user", RoleAction.EDIT.value, mongo_conn=_crud.mongo):  # 4 untuk izin simpan perubahan
            raise PermissionError("Access denied")
        _crud.set_context(
            user_id=cu.id,
            org_id=cu.org_id,
            ip_address=cu.ip_address,  # Jika ada
            user_agent=cu.user_agent   # Jika ada
        )
        
        response = _crud.reset_password(user_id,req)
    revoke_all_refresh_tokens(user_id)
    return ApiResponse(status=0, message="Password has change", data=response)

@router.get("", response_model=ApiResponse)
async def get_all_data(
        page: int = Query(1, ge=1, description="Page number"),
        per_page: int = Query(10, ge=1, le=100, description="Items per page"),
        sort_field: str = Query("_id", description="Field to sort by"),
        sort_order: str = Query("asc", pattern="^(asc|desc)$", description="Sort order: 'asc' or 'desc'"),
        username: Optional[str] = Query(None, description="Filter by username"),
        username_contains: Optional[str] = Query(None, description="Name contains (case insensitive)"),
        email: Optional[str] = Query(None, description="Filter by email"),
        email_contains: Optional[str] = Query(None, description="Filter by email (case insensitive)"),
        role: Optional[str] = Query(None, description="Filter by role ID"),
        roles: Optional[List[str]] = Query(None, description="Filter by multiple role IDs"),
        status: Optional[str] = Query(None, description="Filter by status"),
        cu: CurrentUser = Depends(get_current_user)
    ) -> ApiResponse:

    with CRUD() as _crud:
        if not permission_checker.has_permission(cu.roles, "_user", RoleAction.VIEW.value, mongo_conn=_crud.mongo):  # 1 untuk izin baca
            raise PermissionError("Access denied")
        _crud.set_context(
            user_id=cu.id,
            org_id=cu.org_id,
            ip_address=cu.ip_address,  # Jika ada
            user_agent=cu.user_agent   # Jika ada
        )
        
        # Build filters dynamically
        filters = {}
        
        # default filter by organization id
        if cu.org_id:
            filters["org_id"] = cu.org_id

        # addtional when filter running
        if username:
            filters["username"] = username
        elif username_contains:
            filters["username"] = {"$regex": f".*{username_contains}.*", "$options": "i"}

        if email:
            filters["email"] = email
        elif email_contains:
            filters["email"] = {"$regex": f".*{email_contains}.*", "$options": "i"}

        if status:
            filters["status"] = status
        
        # Filter by single role
        if role:
            filters["roles"] = role
        
        # Filter by multiple roles
        if roles:
            filters["roles"] = roles  # Akan diubah ke $in dalam CRUD

        # Call CRUD function
        response = _crud.get_all(
            filters=filters,
            page=page,
            per_page=per_page,
            sort_field=sort_field,
            sort_order=sort_order,
        )
    return ApiResponse(status=0, message="Data loaded", data=response["data"], pagination=response["pagination"])
    
@router.get("/find/{user_id}", response_model=ApiResponse)
async def find_by_id(user_id: str, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    
    with CRUD() as _crud:
        if not (permission_checker.has_permission(cu.roles, "_user", RoleAction.VIEW.value, mongo_conn=_crud.mongo) or permission_checker.has_permission(cu.roles, "_myprofile", 1, mongo_conn=_crud.mongo)):  # 1 untuk izin baca
            raise PermissionError("Access denied")
        _crud.set_context(
            user_id=cu.id,
            org_id=cu.org_id,
            ip_address=cu.ip_address,  # Jika ada
            user_agent=cu.user_agent   # Jika ada
        )
        response = _crud.get_by_id(user_id)
    return ApiResponse(status=0, message="Data found", data=response)