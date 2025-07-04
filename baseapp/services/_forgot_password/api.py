from fastapi import APIRouter, Request

from baseapp.model.common import ApiResponse
from baseapp.utils.utility import cbor_or_json, parse_request_body

from baseapp.config import setting
config = setting.get_settings()

from baseapp.services._forgot_password.model import OTPRequest, VerifyOTPRequest, ResetPasswordRequest
from baseapp.services._forgot_password.crud import CRUD
_crud = CRUD()

router = APIRouter(prefix="/v1/forgot-password", tags=["Forgot Password"])

@router.post("/send-otp", response_model=ApiResponse)
@cbor_or_json
async def send_otp(req: Request) -> ApiResponse:
    req = await parse_request_body(req, OTPRequest)
    response = _crud.send_otp(req)
    return ApiResponse(status=0, data=response)

@router.post("/verify-otp")
@cbor_or_json
async def verify_otp(req: Request):
    req = await parse_request_body(req, VerifyOTPRequest)
    response = _crud.verify_otp(req)
    return ApiResponse(status=0, data=response)

@router.post("/reset-password")
@cbor_or_json
async def verify_otp(req: Request):
    req = await parse_request_body(req, ResetPasswordRequest)
    response = _crud.reset_password(req)
    return ApiResponse(status=0, data=response)


