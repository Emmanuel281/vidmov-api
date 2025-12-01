from fastapi import APIRouter

from baseapp.model.common import ApiResponse

from baseapp.config import setting
config = setting.get_settings()

from baseapp.services._forgot_password.model import OTPRequest, VerifyOTPRequest, ResetPasswordRequest
from baseapp.services._forgot_password.crud import CRUD
_crud = CRUD()

router = APIRouter(prefix="/v1/forgot-password", tags=["Forgot Password"])

@router.post("/send-otp", response_model=ApiResponse)
async def send_otp(req: OTPRequest) -> ApiResponse:
    response = _crud.send_otp(req)
    return ApiResponse(status=0, data=response)

@router.post("/verify-otp")
async def verify_otp(req: VerifyOTPRequest):
    response = _crud.verify_otp(req)
    return ApiResponse(status=0, data=response)

@router.post("/reset-password")
async def verify_otp(req: ResetPasswordRequest):
    response = _crud.reset_password(req)
    return ApiResponse(status=0, data=response)