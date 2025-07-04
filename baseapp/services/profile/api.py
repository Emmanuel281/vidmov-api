import cbor2
from fastapi import APIRouter, Depends

from baseapp.model.common import ApiResponse, CurrentUser
from baseapp.utils.jwt import get_current_user
from baseapp.utils.utility import cbor_or_json

from baseapp.config import setting
config = setting.get_settings()

import logging, httpx
logger = logging.getLogger()

router = APIRouter(prefix="/v1/profile", tags=["Profile"])
    
@router.get("/organization", response_model=ApiResponse)
@cbor_or_json
async def get_org_profile(cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    token: str = cu.token
    org_id: str = cu.org_id
    api_url = config.host + f"/v1/_organization/find/{org_id}"
    headers = {'Content-Type': 'application/json', 'Authorization': "Bearer "+ token}
    if config.app_env == "production":
        headers["Content-Type"] = "application/cbor"
    async with httpx.AsyncClient() as client:
        try:
            response = await client.request(
                method="GET",
                url=api_url,
                headers=headers,
                timeout=30
            )
            logger.debug(f"respon org profile: {response.raise_for_status()}")
            response.raise_for_status()
            if config.app_env == "production":
                abcd = cbor2.loads(response.content)
                return ApiResponse.model_validate(abcd)
            else:
                return ApiResponse.model_validate_json(response.text)
        except httpx.RequestError as exc:
            logger.error(f"An error occurred while requesting {exc.request.url}: {exc}")
            raise ValueError(str(exc))
        except httpx.HTTPStatusError as exc:
            logger.error(f"HTTP error occurred: {exc.response.status_code}")
            if config.app_env == "production":
                abcd = cbor2.loads(exc.response.content)
                return ApiResponse.model_validate(abcd)
            else:
                return ApiResponse.model_validate_json(exc.response.text)

@router.get("/user", response_model=ApiResponse)
@cbor_or_json
async def get_user_profile(cu: CurrentUser = Depends(get_current_user)) -> ApiResponse:
    token: str = cu.token
    user_id: str = cu.id
    api_url = config.host + f"/v1/_user/find/{user_id}"
    headers = {'Content-Type': 'application/json', 'Authorization': "Bearer "+ token}
    if config.app_env == "production":
        headers["Content-Type"] = "application/cbor"
    async with httpx.AsyncClient() as client:
        try:
            response = await client.request(
                method="GET",
                url=api_url,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            if config.app_env == "production":
                abcd = cbor2.loads(response.content)
                return ApiResponse.model_validate(abcd)
            else:
                return ApiResponse.model_validate_json(response.text)
        except httpx.RequestError as exc:
            logger.error(f"An error occurred while requesting {exc.request.url}: {exc}")
            raise ValueError(str(exc))
        except httpx.HTTPStatusError as exc:
            logger.error(f"HTTP error occurred: {exc.response.status_code}")
            if config.app_env == "production":
                abcd = cbor2.loads(exc.response.content)
                return ApiResponse.model_validate(abcd)
            else:
                return ApiResponse.model_validate_json(exc.response.text)