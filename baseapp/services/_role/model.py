from pydantic import BaseModel, Field
from typing import Optional
from baseapp.model.common import Status

class Role(BaseModel):
    color: Optional[str] = Field(default="black", description="Label color of role.")
    name: str = Field(description="Name of role.")
    status: Status = Field(default=None, description="Status of role.")