from fastapi import APIRouter, Query, Depends, Request
from typing import Optional, List, Dict

from baseapp.model.common import ApiResponse, CurrentUser, StatusContent, TypeContent, SubscriptionTier, Status
from baseapp.utils.jwt import get_current_user
from baseapp.utils.utility import cbor_or_json, parse_request_body

from baseapp.config import setting
config = setting.get_settings()

from baseapp.services._enum.model import Enum, EnumUpdate

from baseapp.services._enum.crud import CRUD
_crud = CRUD()

from baseapp.services.permission_check_service import PermissionChecker
permission_checker = PermissionChecker()

router = APIRouter(prefix="/v1/_enum", tags=["Enum"])

def format_enum_to_json(enum_class: Enum) -> List[Dict[str, str]]:
    """Fungsi bantuan untuk mengubah kelas Enum menjadi format JSON yang diinginkan."""
    # Menggunakan list comprehension untuk membuat daftar kamus
    # Setiap item dalam daftar akan memiliki 'name' dan 'value' dari anggota enum.
    return [{"name": member.name, "value": member.value} for member in enum_class]

HARDCODED_ENUMS = {
    "Status": Status,
    "StatusContent": StatusContent,
    "TypeContent": TypeContent,
    "SubscriptionTier": SubscriptionTier,
}

@router.post("/create", response_model=ApiResponse)
@cbor_or_json
async def create(req: Request, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    if not permission_checker.has_permission(cu.roles, "_enum", 2):  # 2 untuk izin simpan baru
        raise PermissionError("Access denied")
    
    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )
    
    req = await parse_request_body(req, Enum)
    response = _crud.create(req)
    return ApiResponse(status=0, message="Data created", data=response)
    
@router.put("/update/{enum_id}", response_model=ApiResponse)
@cbor_or_json
async def update_by_id(enum_id: str, req: Request, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    if not permission_checker.has_permission(cu.roles, "_enum", 4):  # 4 untuk izin simpan perubahan
        raise PermissionError("Access denied")
    
    # Perbarui konteks pengguna untuk AuditTrail
    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )

    req = await parse_request_body(req, EnumUpdate)
    response = _crud.update_by_id(enum_id,req)
    return ApiResponse(status=0, message="Data updated", data=response)

@router.get("", response_model=ApiResponse)
@cbor_or_json
async def get_all_data(
        page: int = Query(1, ge=1, description="Page number"),
        per_page: int = Query(10, ge=1, le=100, description="Items per page"),
        sort_field: str = Query("_id", description="Field to sort by"),
        sort_order: str = Query("asc", regex="^(asc|desc)$", description="Sort order: 'asc' or 'desc'"),
        app_name: Optional[str] = Query(None, description="Filter by app name"),
        module: Optional[str] = Query(None, description="Filter by module"),
        cu: CurrentUser = Depends(get_current_user)
    ) -> ApiResponse:

    if not permission_checker.has_permission(cu.roles, "_enum", 1):  # 1 untuk izin baca
        raise PermissionError("Access denied")
    
    # Perbarui konteks pengguna untuk AuditTrail
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

    if app_name:
        filters["app"] = app_name
    
    if module:
        filters["mod"] = module
        if module == "_enum" or module == "dmsDataType" or module == "GENRE": 
            del filters["org_id"]

    # Call CRUD function
    response = _crud.get_all(
        filters=filters,
        page=page,
        per_page=per_page,
        sort_field=sort_field,
        sort_order=sort_order,
    )
    return ApiResponse(status=0, message="Data loaded", data=response["data"], pagination=response["pagination"])
    
@router.get("/find/{enum_id}", response_model=ApiResponse)
@cbor_or_json
async def find_by_id(enum_id: str, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    if not permission_checker.has_permission(cu.roles, "_enum", 1):  # 1 untuk izin baca
        raise PermissionError("Access denied")
    
    # Perbarui konteks pengguna untuk AuditTrail
    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )

    if enum_id in HARDCODED_ENUMS:
        enum_class = HARDCODED_ENUMS[enum_id]
        response = format_enum_to_json(enum_class)
    else:
        response = _crud.get_by_id(enum_id)
    return ApiResponse(status=0, message="Data found", data=response)
    
@router.delete("/delete/{enum_id}", response_model=ApiResponse)
@cbor_or_json
async def delete_by_id(enum_id: str, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    if not permission_checker.has_permission(cu.roles, "_enum", 8):  # 8 untuk izin hapus
        raise PermissionError("Access denied")
    
    # Perbarui konteks pengguna untuk AuditTrail
    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )
    
    response = _crud.delete_by_id(enum_id)
    return ApiResponse(status=0, message="Document deleted", data=response)

