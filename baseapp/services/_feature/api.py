from fastapi import APIRouter, Depends

from baseapp.model.common import ApiResponse, CurrentUser
from baseapp.utils.jwt import get_current_user

from baseapp.config import setting
config = setting.get_settings()

from baseapp.services._feature.model import Feature
from baseapp.services._feature.crud import CRUD
_crud = CRUD()

from baseapp.services.permission_check_service import PermissionChecker
permission_checker = PermissionChecker()

router = APIRouter(prefix="/v1/_feature", tags=["Feature"])

@router.put("/update", response_model=ApiResponse)
async def update_feature_permission(req: Feature, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    if not permission_checker.has_permission(cu.roles, "_feature", 4):  # 4 untuk izin simpan perubahan
        raise PermissionError("Access denied")
    
    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        authority=cu.authority,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )

    response = _crud.set_permission(req)
    return ApiResponse(status=0, message="Data updated", data=response)
    
@router.get("/list/{role_id}", response_model=ApiResponse)
async def find_by_role_id(role_id: str, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    if not permission_checker.has_permission(cu.roles, "_feature", 1):  # 1 untuk izin baca
        raise PermissionError("Access denied")
    
    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        authority=cu.authority,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )

    # Build filters dynamically
    filters = {}

    if role_id:
        filters["r_id"] = role_id

    response = _crud.get_all(filters=filters)
    return ApiResponse(status=0, message="Data found", data=response)

