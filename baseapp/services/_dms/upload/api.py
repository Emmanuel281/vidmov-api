import json
from fastapi import APIRouter, Depends, File, UploadFile, Form

from baseapp.config import setting
from baseapp.model.common import ApiResponse, CurrentUser, RoleAction
from baseapp.utils.jwt import get_current_user

from baseapp.services.permission_check_service import PermissionChecker
from baseapp.services._dms.upload.model import SetMetaData
from baseapp.services._dms.upload.crud import CRUD

config = setting.get_settings()
permission_checker = PermissionChecker()

async def parse_metadata(
    doctype: str = Form(...),
    metadata: str = Form(...),
    refkey_id: str = Form(None),
    refkey_table: str = Form(None),
    refkey_name: str = Form(None),
) -> SetMetaData:
    try:
        # Parse metadata JSON string to dictionary
        metadata_dict = json.loads(metadata)
        return SetMetaData(doctype=doctype, metadata=metadata_dict, refkey_id=refkey_id, refkey_table=refkey_table, refkey_name=refkey_name)
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON format in metadata")
    
router = APIRouter(prefix="/v1/_dms", tags=["DMS - Upload"])

@router.post("/upload", response_model=ApiResponse)
async def create(file: UploadFile = File(...), payload: SetMetaData = Depends(parse_metadata), cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    with CRUD() as _crud:
        if not permission_checker.has_permission(cu.roles, "_dmsbrowse", RoleAction.ADD.value, mongo_conn=_crud.mongo):  # 4 untuk izin upload file
            raise PermissionError("Access denied")
        _crud.set_context(
            user_id=cu.id,
            org_id=cu.org_id,
            ip_address=cu.ip_address,  # Jika ada
            user_agent=cu.user_agent   # Jika ada
        )

        response = await _crud.upload_file_to_minio(file,payload)
    
    return ApiResponse(status=0, message="Data created", data=response)