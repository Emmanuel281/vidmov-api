from fastapi import APIRouter, Query, Depends

from baseapp.model.common import ApiResponse, CurrentUser, DMSOperationType
from baseapp.utils.jwt import get_current_user

from baseapp.config import setting
config = setting.get_settings()

from baseapp.services._dms.upload.model import MoveToTrash

from baseapp.services._dms.browse.crud import CRUD
_crud = CRUD()

from baseapp.services.permission_check_service import PermissionChecker
permission_checker = PermissionChecker()

router = APIRouter(prefix="/v1/_dms/browse", tags=["DMS - Browse"])

@router.get("/key/{refkey_table}/{refkey_id}", response_model=ApiResponse)
async def browse_by_key(
        refkey_table: str, refkey_id: str,
        cu: CurrentUser = Depends(get_current_user)
    ) -> ApiResponse:

    if not permission_checker.has_permission(cu.roles, "_dmsbrowse", 1):  # 1 untuk izin baca
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

    if refkey_table:
        filters["refkey_table"] = refkey_table

    if refkey_id:
        filters["refkey_id"] = refkey_id

    # Call CRUD function
    response = _crud.browse_by_key(
        filters=filters
    )
    return ApiResponse(status=0, message="Data loaded", data=response["data"])

@router.get("/folder/{pid}", response_model=ApiResponse)
async def list_folder(
        pid: str,
        cu: CurrentUser = Depends(get_current_user)
    ) -> ApiResponse:

    if not permission_checker.has_permission(cu.roles, "_dmsbrowse", 1):  # 1 untuk izin baca
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

    if pid != "root":
        filters["pid"] = pid
    else:
        filters["level"] = 1

    # Call CRUD function
    response = _crud.list_folder(
        filters=filters
    )
    return ApiResponse(status=0, message="Data loaded", data=response["data"])

@router.get("/explore/{folder_id}", response_model=ApiResponse)
async def list_file_by_folder_id(
        folder_id: str, 
        page: int = Query(1, ge=1, description="Page number"),
        per_page: int = Query(10, ge=1, le=100, description="Items per page"),
        sort_field: str = Query("_id", description="Field to sort by"),
        sort_order: str = Query("asc", regex="^(asc|desc)$", description="Sort order: 'asc' or 'desc'"),
        cu: CurrentUser = Depends(get_current_user)
    ) -> ApiResponse:

    if not permission_checker.has_permission(cu.roles, "_dmsbrowse", 1):  # 1 untuk izin baca
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

    if folder_id:
        if folder_id == "delete":
            filters["is_deleted"] = 1
        else:
            filters["folder_id"] = folder_id
            # Tambahkan filter untuk is_deleted = 0 atau tidak ada
            filters["$or"] = [
                {"is_deleted": 0},
                {"is_deleted": {"$exists": False}}
            ]

    # Call CRUD function
    response = _crud.list_file(
        filters=filters,
        page=page,
        per_page=per_page,
        sort_field=sort_field,
        sort_order=sort_order,
    )
    return ApiResponse(status=0, message="Data loaded", data=response["data"], pagination=response["pagination"])

@router.get("/storage", response_model=ApiResponse)
async def storage(
        cu: CurrentUser = Depends(get_current_user)
    ) -> ApiResponse:

    if not permission_checker.has_permission(cu.roles, "_dmsbrowse", 1):  # 1 untuk izin baca
        raise PermissionError("Access denied")

    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )

    # Call CRUD function
    response = _crud.check_storage()
    return ApiResponse(status=0, message="Data loaded", data=response)

@router.put("/sent_file/{operation_type}/{file_id}", response_model=ApiResponse)
async def set_status_file(operation_type: DMSOperationType, file_id: str, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    """Update file status (move to trash or restore)"""
    if not permission_checker.has_permission(cu.roles, "_dmsbrowse", 4):  # 4 untuk izin simpan perubahan
        raise PermissionError("Access denied")
    
    # Perbarui konteks pengguna untuk AuditTrail
    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )

    # Determine operation
    is_deleted = 1 if operation_type == DMSOperationType.TO_TRASH else 0
    operation = MoveToTrash(is_deleted=is_deleted)

    response = _crud.move_to_trash_restore(file_id,operation)
    return ApiResponse(status=0, message="Data updated", data=response)

@router.delete("/delete_file/{file_id}", response_model=ApiResponse)
async def delete_by_id(file_id: str, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    if not permission_checker.has_permission(cu.roles, "_dmsbrowse", 8):  # 8 untuk izin hapus
        raise PermissionError("Access denied")
    
    # Perbarui konteks pengguna untuk AuditTrail
    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )
    
    response = _crud.delete_file_by_id(file_id)
    return ApiResponse(status=0, message="File deleted", data=response)

@router.delete("/delete_folder/{folder_id}", response_model=ApiResponse)
async def delete_by_id(folder_id: str, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    if not permission_checker.has_permission(cu.roles, "_dmsbrowse", 8):  # 8 untuk izin hapus
        raise PermissionError("Access denied")
    
    # Perbarui konteks pengguna untuk AuditTrail
    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )
    
    response = _crud.delete_folder_by_id(folder_id)
    return ApiResponse(status=0, message="Folder deleted", data=response)