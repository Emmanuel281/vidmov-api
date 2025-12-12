from fastapi import APIRouter, Depends, Query
from typing import Optional

from baseapp.config import setting
from baseapp.model.common import ApiResponse, CurrentUser, Status, UpdateStatus

from baseapp.config import setting
config = setting.get_settings()

from baseapp.utils.jwt import get_current_user
from baseapp.utils.logger import Logger
from baseapp.services._org import model

from baseapp.services._org.crud import CRUD
_crud = CRUD()

from baseapp.services.permission_check_service import PermissionChecker
permission_checker = PermissionChecker()

logger = Logger("baseapp.services._org.api")

router = APIRouter(prefix="/v1/_organization", tags=["Organization"])

@router.post("/init_owner", response_model=ApiResponse)
async def create(req: model.InitRequest) -> ApiResponse:
    response = _crud.init_owner_org(req.org, req.user)
    return ApiResponse(status=0, message="Data created", data=response)

@router.post("/init_partner", response_model=ApiResponse)
async def create(req: model.InitRequest, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    if not permission_checker.has_permission(cu.roles, "_organization", 2):  # 2 untuk izin simpan baru
        raise PermissionError("Access denied")
    
    # check authority is not owner
    if cu.authority != 1:
        raise PermissionError("Access denied")
    
    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )

    req.org.authority = 2

    response = _crud.init_partner_client_org(req.org, req.user)
    return ApiResponse(status=0, message="Data created", data=response)

@router.get("", response_model=ApiResponse)
async def get_all_data(
        page: int = Query(1, ge=1, description="Page number"),
        per_page: int = Query(10, ge=1, le=100, description="Items per page"),
        sort_field: str = Query("_id", description="Field to sort by"),
        sort_order: str = Query("asc", regex="^(asc|desc)$", description="Sort order: 'asc' or 'desc'"),
        org_name: Optional[str] = Query(None, description="Filter by organization name"),
        status: Optional[str] = Query(None, description="Filter by status"),
        cu: CurrentUser = Depends(get_current_user)
    ) -> ApiResponse:

    if not permission_checker.has_permission(cu.roles, "_organization", 1):  # 1 untuk izin baca
        raise PermissionError("Access denied")

    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )
    
    # Build filters dynamically
    filters = {
        "ref_id": cu.org_id
    }

    # addtional when filter running
    if org_name:
        filters["org_name"] = org_name
    if status:
        filters["status"] = status

    # Call CRUD function
    response = _crud.get_all(
        filters=filters,
        page=page,
        per_page=per_page,
        sort_field=sort_field,
        sort_order=sort_order,
    )
    return ApiResponse(status=0, message="Data loaded", data=response["data"], pagination=response["pagination"])

@router.get("/find/{org_id}", response_model=ApiResponse)
async def find_by_id(org_id: str, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    if not (permission_checker.has_permission(cu.roles, "_organization", 1) or permission_checker.has_permission(cu.roles, "_myorg", 1)):  # 1 untuk izin baca
        raise PermissionError("Access denied")
    
    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )
    
    response = _crud.get_by_id(org_id)
    return ApiResponse(status=0, message="Data found", data=response)

@router.put("/update/{org_id}", response_model=ApiResponse)
async def update_by_id(org_id: str, req: model.OrganizationUpdate, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    if not (permission_checker.has_permission(cu.roles, "_organization", 4) or permission_checker.has_permission(cu.roles, "_myorg", 4)):  # 4 untuk izin simpan perubahan
        raise PermissionError("Access denied")
    
    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )

    response = _crud.update_by_id(org_id,req)
    return ApiResponse(status=0, message="Data updated", data=response)

@router.put("/update_status/{org_id}", response_model=ApiResponse)
async def update_status_by_id(org_id: str, req: UpdateStatus, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    if not (permission_checker.has_permission(cu.roles, "_organization", 4)):  # 4 untuk izin simpan perubahan
        raise PermissionError("Access denied")
    
    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )

    response = _crud.update_status(org_id,req)
    return ApiResponse(status=0, message="Data updated", data=response)

@router.delete("/delete/{org_id}", response_model=ApiResponse)
async def update_status(org_id: str, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    if not permission_checker.has_permission(cu.roles, "_organization", 4):  # 4 untuk izin simpan perubahan
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
    response = _crud.update_status(org_id,manual_data)
    return ApiResponse(status=0, message="Data deleted", data=response)