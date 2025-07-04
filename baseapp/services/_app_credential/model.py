from pydantic import BaseModel, Field
from typing import Optional, List
from baseapp.model.common import Status
from datetime import datetime

class AppCredential(BaseModel):
    """Representation of application credential."""
    id: str = Field(description="Primary key")
    org_id: str = Field(description="Organization id")
    code: str = Field(description="Organization code")
    api_key: str = Field(description="API key")
    api_secret: str = Field(description="API secret key")
    expired_date: datetime = Field(description="When the token is expired")
    roles: List[str] = Field(description="Roles of the application.")
    status: Status = Field(description="Status of the credential.")
    rec_by: str = Field(description="Created by.")
    rec_date: datetime = Field(description="Created at.")
    mod_by: Optional[str] = Field(default=None, description="Modify by")
    mod_date: Optional[datetime] = Field(default=None, description="Modify at")