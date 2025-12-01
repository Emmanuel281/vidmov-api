from fastapi import APIRouter, Depends

from baseapp.model.common import ApiResponse, CurrentUser
from baseapp.utils.jwt import get_current_user

from baseapp.config import setting
config = setting.get_settings()

from baseapp.services._menu.crud import CRUD
_crud = CRUD()

router = APIRouter(prefix="/v1/_menu", tags=["Menu"])
    
@router.get("/sidemenu", response_model=ApiResponse)
async def sidemenu(cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    _crud.set_context(
        user_id=cu.id,
        org_id=cu.org_id,
        authority=cu.authority,
        roles=cu.roles,
        ip_address=cu.ip_address,  # Jika ada
        user_agent=cu.user_agent   # Jika ada
    )

    response = _crud.get_all()
    return ApiResponse(status=0, message="Data found", data=response)

