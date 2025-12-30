from fastapi import APIRouter, Query, Depends

from baseapp.config import setting
from baseapp.model.common import ApiResponse, CurrentUser, Status, UpdateStatus, RoleAction
from baseapp.utils.jwt import get_current_user

from baseapp.services.permission_check_service import PermissionChecker
from baseapp.services.brand.model import Brand, BrandCreateByOwner
from baseapp.services.brand.crud import CRUD

config = setting.get_settings()
permission_checker = PermissionChecker()
router = APIRouter(prefix="/v1/owner/brand", tags=["Brand"])

@router.post("/create", response_model=ApiResponse)
async def create(
    req: BrandCreateByOwner,
    cu: CurrentUser = Depends(get_current_user)
) -> ApiResponse:
    
    with CRUD() as _crud:
        if not permission_checker.has_permission(cu.roles, "brand", RoleAction.ADD.value, mongo_conn=_crud.mongo):  # 2 untuk izin simpan baru
            raise PermissionError("Access denied")
        _crud.set_context(
            user_id=cu.id,
            org_id=cu.org_id,
            ip_address=cu.ip_address,  # Jika ada
            user_agent=cu.user_agent   # Jika ada
        )

        response = _crud.create(req)

    return ApiResponse(status=0, message="Data created", data=response)
    
@router.put("/update/{brand_id}", response_model=ApiResponse)
async def update_by_id(brand_id: str, req: Brand, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
        
    with CRUD() as _crud:
        if not permission_checker.has_permission(cu.roles, "brand", RoleAction.EDIT.value, mongo_conn=_crud.mongo):  # 4 untuk izin simpan perubahan
            raise PermissionError("Access denied")
        _crud.set_context(
            user_id=cu.id,
            org_id=cu.org_id,
            ip_address=cu.ip_address,  # Jika ada
            user_agent=cu.user_agent   # Jika ada
        )

        response = _crud.update_by_id(brand_id,req)
    
    return ApiResponse(status=0, message="Data updated", data=response)

@router.delete("/delete/{brand_id}", response_model=ApiResponse)
async def update_status(brand_id: str, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    
    with CRUD() as _crud:
        if not permission_checker.has_permission(cu.roles, "brand", RoleAction.DELETE.value, mongo_conn=_crud.mongo):  # 4 untuk izin simpan perubahan
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
        response = _crud.update_status(brand_id,manual_data)
    return ApiResponse(status=0, message="Data deleted", data=response)

@router.get("", response_model=ApiResponse)
async def get_all_data(
        page: int = Query(1, ge=1, description="Page number"),
        per_page: int = Query(10, ge=1, le=100, description="Items per page"),
        sort_field: str = Query("_id", description="Field to sort by"),
        sort_order: str = Query("asc", pattern="^(asc|desc)$", description="Sort order: 'asc' or 'desc'"),
        org_id: str = Query(None, description="ID organization of partner (exact match)"),
        name: str = Query(None, description="Name of brand (exact match)"),
        name_contains: str = Query(None, description="Name contains (case insensitive)"),
        status: str = Query(None, description="Status data"),
        cu: CurrentUser = Depends(get_current_user)
    ) -> ApiResponse:

    with CRUD() as _crud:
        if not permission_checker.has_permission(cu.roles, "brand", RoleAction.VIEW.value, mongo_conn=_crud.mongo):  # 1 untuk izin baca
            raise PermissionError("Access denied")
        _crud.set_context(
            user_id=cu.id,
            org_id=cu.org_id,
            ip_address=cu.ip_address,  # Jika ada
            user_agent=cu.user_agent   # Jika ada
        )
        
        # Build filters dynamically
        filters = {}
        
        if org_id:
            filters["org_id"] = org_id

        if name:
            filters["name"] = name  # exact match
        elif name_contains:
            filters["name"] = {"$regex": f".*{name_contains}.*", "$options": "i"}
        
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
    
@router.get("/find/{brand_id}", response_model=ApiResponse)
async def find_by_id(brand_id: str, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    
    with CRUD() as _crud:
        if not permission_checker.has_permission(cu.roles, "brand", RoleAction.VIEW.value, mongo_conn=_crud.mongo):  # 1 untuk izin baca
            raise PermissionError("Access denied")
        _crud.set_context(
            user_id=cu.id,
            org_id=cu.org_id,
            ip_address=cu.ip_address,  # Jika ada
            user_agent=cu.user_agent   # Jika ada
        )
        response = _crud.get_by_id(brand_id)
    return ApiResponse(status=0, message="Data found", data=response)