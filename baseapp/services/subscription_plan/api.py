from typing import Optional
from fastapi import APIRouter, Query, Depends, Request

from baseapp.model.common import ApiResponse, CurrentUser, UpdateStatus
from baseapp.utils.jwt import get_current_user, get_current_user_optional
from baseapp.utils.utility import cbor_or_json, parse_request_body

from baseapp.config import setting
config = setting.get_settings()

from baseapp.services.subscription_plan.model import SubscriptionPlan, SubscriptionPlanUpdate

from baseapp.services.subscription_plan.crud import CRUD
_crud = CRUD()

from baseapp.services.permission_check_service import PermissionChecker
permission_checker = PermissionChecker()

router = APIRouter(prefix="/v1/subscription_plan", tags=["Subscription Plan"])

@router.post("/create", response_model=ApiResponse)
@cbor_or_json
async def create(
    req: Request,
    cu: CurrentUser = Depends(get_current_user)
) -> ApiResponse:
    if not permission_checker.has_permission(cu.roles, "subscription_plan", 2):  # 2 untuk izin simpan baru
        raise PermissionError("Access denied")

    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )

    req = await parse_request_body(req, SubscriptionPlan)
    response = _crud.create(req)

    return ApiResponse(status=0, message="Data created", data=response)
    
@router.put("/update/{sub_plan_id}", response_model=ApiResponse)
@cbor_or_json
async def update_by_id(sub_plan_id: str, req: Request, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    if not permission_checker.has_permission(cu.roles, "subscription_plan", 4):  # 4 untuk izin simpan perubahan
        raise PermissionError("Access denied")
    
    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )

    req = await parse_request_body(req, SubscriptionPlanUpdate)
    response = _crud.update_by_id(sub_plan_id,req)
    
    return ApiResponse(status=0, message="Data updated", data=response)

@router.get("", response_model=ApiResponse)
@cbor_or_json
async def get_all_data(
        page: int = Query(1, ge=1, description="Page number"),
        per_page: int = Query(10, ge=1, le=100, description="Items per page"),
        sort_field: str = Query("sort", description="Field to sort by"),
        sort_order: str = Query("asc", regex="^(asc|desc)$", description="Sort order: 'asc' or 'desc'"),
        cu: CurrentUser = Depends(get_current_user),
        name: str = Query(None, description="Name of subscription (exact match)"),
        name_contains: str = Query(None, description="Name contains (case insensitive)"),
        status: str = Query(None, description="Status data")
    ) -> ApiResponse:

    if not permission_checker.has_permission(cu.roles, "subscription_plan", 1):  # 1 untuk izin baca
        raise PermissionError("Access denied")

    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )
    
    # Build filters dynamically
    filters = {}
    
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
    
@router.get("/find/{sub_plan_id}", response_model=ApiResponse)
@cbor_or_json
async def find_by_id(sub_plan_id: str, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    if not permission_checker.has_permission(cu.roles, "subscription_plan", 1):  # 1 untuk izin baca
        raise PermissionError("Access denied")
    
    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )
    response = _crud.get_by_id(sub_plan_id)
    return ApiResponse(status=0, message="Data found", data=response)

@router.put("/update_status/{sub_plan_id}", response_model=ApiResponse)
@cbor_or_json
async def update_status(sub_plan_id: str, req: Request, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    if not permission_checker.has_permission(cu.roles, "subscription_plan", 4):  # 4 untuk izin simpan perubahan
        raise PermissionError("Access denied")
    
    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )
    
    # Buat instance model langsung
    req = await parse_request_body(req, UpdateStatus)
    response = _crud.update_by_id(sub_plan_id,req)

    return ApiResponse(status=0, message="Data updated", data=response)

@router.get("/explore", response_model=ApiResponse)
@cbor_or_json
async def get_all_data(
        page: int = Query(1, ge=1, description="Page number"),
        per_page: int = Query(10, ge=1, le=100, description="Items per page"),
        sort_field: str = Query("sort", description="Field to sort by"),
        sort_order: str = Query("asc", regex="^(asc|desc)$", description="Sort order: 'asc' or 'desc'"),
        cu: Optional[CurrentUser] = Depends(get_current_user_optional),
        name: str = Query(None, description="Name of subscription (exact match)"),
        name_contains: str = Query(None, description="Name contains (case insensitive)"),
        status: str = Query(None, description="Status data")
    ) -> ApiResponse:

    if cu:
        _crud.set_context(
            user_id=cu.id,
            org_id=cu.org_id,
            ip_address=cu.ip_address,  # Jika ada
            user_agent=cu.user_agent   # Jika ada
        )
    
    # Build filters dynamically
    filters = {}
    
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