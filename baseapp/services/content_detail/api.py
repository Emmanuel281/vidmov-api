from typing import Optional
from fastapi import APIRouter, Query, Depends

from baseapp.model.common import ApiResponse, CurrentUser
from baseapp.utils.jwt import get_current_user, get_current_user_optional

from baseapp.config import setting
config = setting.get_settings()

from baseapp.services.content_detail.model import ContentDetail, ContentDetailUpdate, ContentDetailUpdateStatus, ContentDetailSetTier

from baseapp.services.content_detail.crud import CRUD
_crud = CRUD()

from baseapp.services.permission_check_service import PermissionChecker
permission_checker = PermissionChecker()

router = APIRouter(prefix="/v1/content-detail", tags=["Content Detail"])

@router.post("/create", response_model=ApiResponse)
async def create(
    req: ContentDetail,
    cu: CurrentUser = Depends(get_current_user)
) -> ApiResponse:
    if not permission_checker.has_permission(cu.roles, "content", 2):  # 2 untuk izin simpan baru
        raise PermissionError("Access denied")

    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )

    response = _crud.create(req)

    return ApiResponse(status=0, message="Data created", data=response)
    
@router.put("/update/{video_id}", response_model=ApiResponse)
async def update_by_id(video_id: str, req: ContentDetailUpdate, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    if not permission_checker.has_permission(cu.roles, "content", 4):  # 4 untuk izin simpan perubahan
        raise PermissionError("Access denied")
    
    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )

    response = _crud.update_by_id(video_id,req)
    
    return ApiResponse(status=0, message="Data updated", data=response)

@router.put("/update_status/{video_id}", response_model=ApiResponse)
async def update_status(video_id: str, req: ContentDetailUpdateStatus, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    if not permission_checker.has_permission(cu.roles, "content", 4):  # 4 untuk izin simpan perubahan
        raise PermissionError("Access denied")
    
    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )
    
    response = _crud.update_by_id(video_id,req)

    return ApiResponse(status=0, message="Data updated", data=response)

@router.put("/set_tier/{video_id}", response_model=ApiResponse)
async def set_tier(video_id: str, req: ContentDetailSetTier, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    if not permission_checker.has_permission(cu.roles, "content", 4):  # 4 untuk izin simpan perubahan
        raise PermissionError("Access denied")
    
    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )
    
    response = _crud.update_by_id(video_id,req)

    return ApiResponse(status=0, message="Data updated", data=response)

@router.get("", response_model=ApiResponse)
async def get_all_data(
        page: int = Query(1, ge=1, description="Page number"),
        per_page: int = Query(10, ge=1, le=100, description="Items per page"),
        sort_field: str = Query("_id", description="Field to sort by"),
        sort_order: str = Query("asc", regex="^(asc|desc)$", description="Sort order: 'asc' or 'desc'"),
        cu: CurrentUser = Depends(get_current_user),
        content_id: str = Query(None, description="Reference ID to the main content."),
        title: str = Query(None, description="Title of video (exact match)"),
        title_contains: str = Query(None, description="Title contains (case insensitive)"),
        status: str = Query(None, description="Status of video")
    ) -> ApiResponse:

    if not permission_checker.has_permission(cu.roles, "content", 1):  # 1 untuk izin baca
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

    if content_id:
        filters["content_id"] = content_id

    if title:
        filters["title"] = title  # exact match
    elif title_contains:
        filters["title"] = {"$regex": f".*{title_contains}.*", "$options": "i"}
    
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
    
@router.get("/find/{video_id}", response_model=ApiResponse)
async def find_by_id(video_id: str, cu: CurrentUser = Depends(get_current_user_optional)) -> ApiResponse:
    if not permission_checker.has_permission(cu.roles, "content", 1):  # 1 untuk izin baca
        raise PermissionError("Access denied")
    
    if cu:
        _crud.set_context(
            user_id=cu.id,
            org_id=cu.org_id,
            ip_address=cu.ip_address,  # Jika ada
            user_agent=cu.user_agent   # Jika ada
        )
    response = _crud.get_by_id(video_id)
    return ApiResponse(status=0, message="Data found", data=response)

@router.get("/explore", response_model=ApiResponse)
async def get_all_data(
        page: int = Query(1, ge=1, description="Page number"),
        per_page: int = Query(10, ge=1, le=100, description="Items per page"),
        sort_field: str = Query("_id", description="Field to sort by"),
        sort_order: str = Query("asc", regex="^(asc|desc)$", description="Sort order: 'asc' or 'desc'"),
        cu: Optional[CurrentUser] = Depends(get_current_user_optional),
        content_id: str = Query(None, description="Reference ID to the main content."),
        title: str = Query(None, description="Title of video (exact match)"),
        title_contains: str = Query(None, description="Title contains (case insensitive)"),
        status: str = Query(None, description="Status of video")
    ) -> ApiResponse:

    # Build filters dynamically
    filters = {}

    if cu:
        _crud.set_context(
            user_id=cu.id,
            org_id=cu.org_id,
            ip_address=cu.ip_address,  # Jika ada
            user_agent=cu.user_agent   # Jika ada
        )

    if content_id:
        filters["content_id"] = content_id

    if title:
        filters["title"] = title  # exact match
    elif title_contains:
        filters["title"] = {"$regex": f".*{title_contains}.*", "$options": "i"}
    
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