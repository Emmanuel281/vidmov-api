from typing import List, Optional
from fastapi import APIRouter, Query, Depends

from baseapp.model.common import ApiResponse, CurrentUser
from baseapp.utils.jwt import get_current_user, get_current_user_optional

from baseapp.config import setting
config = setting.get_settings()

from baseapp.services.content.model import Content, ContentUpdate, ContentUpdateStatus

from baseapp.services.content.crud import CRUD
_crud = CRUD()

from baseapp.services.permission_check_service import PermissionChecker
permission_checker = PermissionChecker()

router = APIRouter(prefix="/v1/content", tags=["Content"])

@router.post("/create", response_model=ApiResponse)
async def create(
    req: Content,
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
    
@router.put("/update/{content_id}", response_model=ApiResponse)
async def update_by_id(content_id: str, req: ContentUpdate, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    if not permission_checker.has_permission(cu.roles, "content", 4):  # 4 untuk izin simpan perubahan
        raise PermissionError("Access denied")
    
    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )

    response = _crud.update_by_id(content_id,req)
    
    return ApiResponse(status=0, message="Data updated", data=response)

@router.put("/update_status/{content_id}", response_model=ApiResponse)
async def update_status(content_id: str, req: ContentUpdateStatus, cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    if not permission_checker.has_permission(cu.roles, "content", 4):  # 4 untuk izin simpan perubahan
        raise PermissionError("Access denied")
    
    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )
    
    response = _crud.update_by_id(content_id,req)

    return ApiResponse(status=0, message="Data updated", data=response)

@router.get("", response_model=ApiResponse)
async def get_all_data(
        page: int = Query(1, ge=1, description="Page number"),
        per_page: int = Query(10, ge=1, le=100, description="Items per page"),
        sort_field: str = Query("_id", description="Field to sort by"),
        sort_order: str = Query("asc", regex="^(asc|desc)$", description="Sort order: 'asc' or 'desc'"),
        cu: CurrentUser = Depends(get_current_user),
        name: str = Query(None, description="Name of content (exact match)"),
        name_contains: str = Query(None, description="Name contains (case insensitive)"),
        name_starts_with: str = Query(None, description="Name starts with"),
        name_ends_with: str = Query(None, description="Name ends with"),
        description_contains: str = Query(None, description="Descriptions contains (case insensitive)"),
        genre: Optional[str] = Query(None, description="Filter by genre ID"),
        genres: Optional[List[str]] = Query(None, description="Filter by multiple genre IDs"),
        type_content: str = Query(None, description="Type of content"),
        status: str = Query(None, description="Status of content")
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

    if name:
        filters["name"] = name  # exact match
    elif name_contains:
        filters["name"] = {"$regex": f".*{name_contains}.*", "$options": "i"}
    elif name_starts_with:
        filters["name"] = {"$regex": f"^{name_starts_with}", "$options": "i"}
    elif name_ends_with:
        filters["name"] = {"$regex": f"{name_ends_with}$", "$options": "i"}

    if description_contains:
        filters["description"] = {"$regex": f".*{description_contains}.*", "$options": "i"}

    if status:
        filters["status"] = status

    if type_content:
        filters["type"] = type_content

    # Filter by single role
    if genre:
        filters["genre"] = genre
    
    # Filter by multiple roles
    if genres:
        filters["genre"] = genres  # Akan diubah ke $in dalam CRUD

    # Call CRUD function
    response = _crud.get_all(
        filters=filters,
        page=page,
        per_page=per_page,
        sort_field=sort_field,
        sort_order=sort_order,
    )
    return ApiResponse(status=0, message="Data loaded", data=response["data"], pagination=response["pagination"])
    
@router.get("/find/{content_id}", response_model=ApiResponse)
async def find_by_id(content_id: str, cu: CurrentUser = Depends(get_current_user_optional)) -> ApiResponse:
    if not permission_checker.has_permission(cu.roles, "content", 1):  # 1 untuk izin baca
        raise PermissionError("Access denied")
    
    if cu:
        _crud.set_context(
            user_id=cu.id,
            org_id=cu.org_id,
            ip_address=cu.ip_address,  # Jika ada
            user_agent=cu.user_agent   # Jika ada
        )
    response = _crud.get_by_id(content_id)
    return ApiResponse(status=0, message="Data found", data=response)

@router.get("/explore", response_model=ApiResponse)
async def get_all_data(
        page: int = Query(1, ge=1, description="Page number"),
        per_page: int = Query(10, ge=1, le=100, description="Items per page"),
        sort_field: str = Query("_id", description="Field to sort by"),
        sort_order: str = Query("asc", regex="^(asc|desc)$", description="Sort order: 'asc' or 'desc'"),
        cu: Optional[CurrentUser] = Depends(get_current_user_optional),
        name: str = Query(None, description="Name of role (exact match)"),
        name_contains: str = Query(None, description="Name contains (case insensitive)"),
        name_starts_with: str = Query(None, description="Name starts with"),
        name_ends_with: str = Query(None, description="Name ends with"),
        description_contains: str = Query(None, description="Descriptions contains (case insensitive)"),
        genre: Optional[str] = Query(None, description="Filter by genre ID"),
        genres: Optional[List[str]] = Query(None, description="Filter by multiple genre IDs"),
        type_content: str = Query(None, description="Type of content"),
        status: str = Query(None, description="Status of content")
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

    if name:
        filters["name"] = name  # exact match
    elif name_contains:
        filters["name"] = {"$regex": f".*{name_contains}.*", "$options": "i"}
    elif name_starts_with:
        filters["name"] = {"$regex": f"^{name_starts_with}", "$options": "i"}
    elif name_ends_with:
        filters["name"] = {"$regex": f"{name_ends_with}$", "$options": "i"}

    if description_contains:
        filters["description"] = {"$regex": f".*{description_contains}.*", "$options": "i"}

    if status:
        filters["status"] = status

    if type_content:
        filters["type"] = type_content

    # Filter by single role
    if genre:
        filters["genre"] = genre
    
    # Filter by multiple roles
    if genres:
        filters["genre"] = genres  # Akan diubah ke $in dalam CRUD

    # Call CRUD function
    response = _crud.get_all(
        filters=filters,
        page=page,
        per_page=per_page,
        sort_field=sort_field,
        sort_order=sort_order,
    )
    return ApiResponse(status=0, message="Data loaded", data=response["data"], pagination=response["pagination"])