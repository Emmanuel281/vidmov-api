from fastapi import APIRouter, Query, Depends

from baseapp.model.common import ApiResponse, CurrentUser, Status, UpdateStatus
from baseapp.utils.jwt import get_current_user

from baseapp.config import setting
config = setting.get_settings()

from baseapp.services._role.model import Role

from baseapp.services._role.crud import CRUD
_crud = CRUD()

from baseapp.services.permission_check_service import PermissionChecker
permission_checker = PermissionChecker()

router = APIRouter(prefix="/v1/_role", tags=["Role"])

@router.post("/create", response_model=ApiResponse)
async def create(
    req: Role,
    cu: CurrentUser = Depends(get_current_user)
) -> ApiResponse:
    if not permission_checker.has_permission(cu.roles, "_role", 2):  # 2 untuk izin simpan baru
        raise PermissionError("Access denied")

    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )

    response = _crud.create(req)

    return ApiResponse(status=0, message="Data created", data=response)
    
@router.put("/update/{role_id}", response_model=ApiResponse)
async def update_by_id(role_id: str, req: Role, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    if not permission_checker.has_permission(cu.roles, "_role", 4):  # 4 untuk izin simpan perubahan
        raise PermissionError("Access denied")
    
    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )

    response = _crud.update_by_id(role_id,req)
    
    return ApiResponse(status=0, message="Data updated", data=response)

@router.delete("/delete/{role_id}", response_model=ApiResponse)
async def update_status(role_id: str, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    if not permission_checker.has_permission(cu.roles, "_role", 4):  # 4 untuk izin simpan perubahan
        raise PermissionError("Access denied")
    
    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )
    
    # Buat instance model langsung
    manual_data = UpdateStatus(
        status=Status.DELETE  # nilai yang Anda tentukan
    )
    response = _crud.update_status(role_id,manual_data)
    return ApiResponse(status=0, message="Data deleted", data=response)

@router.get("", response_model=ApiResponse)
async def get_all_data(
        page: int = Query(1, ge=1, description="Page number"),
        per_page: int = Query(10, ge=1, le=100, description="Items per page"),
        sort_field: str = Query("_id", description="Field to sort by"),
        sort_order: str = Query("asc", regex="^(asc|desc)$", description="Sort order: 'asc' or 'desc'"),
        cu: CurrentUser = Depends(get_current_user),
        name: str = Query(None, description="Name of role (exact match)"),
        name_contains: str = Query(None, description="Name contains (case insensitive)"),
        name_starts_with: str = Query(None, description="Name starts with"),
        name_ends_with: str = Query(None, description="Name ends with"),
        status: str = Query(None, description="Status data")
    ) -> ApiResponse:

    if not permission_checker.has_permission(cu.roles, "_role", 1):  # 1 untuk izin baca
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

    if name:
        filters["name"] = name  # exact match
    elif name_contains:
        filters["name"] = {"$regex": f".*{name_contains}.*", "$options": "i"}
    elif name_starts_with:
        filters["name"] = {"$regex": f"^{name_starts_with}", "$options": "i"}
    elif name_ends_with:
        filters["name"] = {"$regex": f"{name_ends_with}$", "$options": "i"}

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
    
@router.get("/find/{role_id}", response_model=ApiResponse)
async def find_by_id(role_id: str, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    if not permission_checker.has_permission(cu.roles, "_role", 1):  # 1 untuk izin baca
        raise PermissionError("Access denied")
    
    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )
    response = _crud.get_by_id(role_id)
    return ApiResponse(status=0, message="Data found", data=response)

