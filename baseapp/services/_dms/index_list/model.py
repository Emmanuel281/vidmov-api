from pydantic import BaseModel, Field
from typing import Optional
from baseapp.model.common import Status

class IndexList(BaseModel):
    name: str = Field(description="Index name.")
    description: Optional[str] = Field(description="Description of index.")
    type: str = Field(description="Type of index")
    status: Optional[Status] = Field(default=None, description="Status of index.")

class IndexListUpdate(BaseModel):
    description: Optional[str] = Field(description="Description of index.")
    type: str = Field(description="Type of index")
    status: Optional[Status] = Field(default=None, description="Status of index.")