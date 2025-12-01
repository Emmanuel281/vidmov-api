from pydantic import BaseModel, Field
from baseapp.model.common import Status

class ApiCredential(BaseModel):
    key_name: str = Field(description="Name of the API credential key.", error_msg_templates={"value_error.missing": "Key name is required!"})
    status: Status = Field(description="Status of the API credential.")

class ApiCredentialCreate(ApiCredential):
    org_id: str = Field(description="Organization ID associated with the API credential.", error_msg_templates={"value_error.missing": "Organization ID is required!"})