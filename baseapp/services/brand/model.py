from pydantic import BaseModel, Field
from typing import List, Optional, Any
from datetime import datetime
from baseapp.model.common import Status

class Brand(BaseModel):
    name: str = Field(description="Name of brand.")
    status: Status = Field(default=Status.ACTIVE.value, description="Status of brand.")

class BrandCreateByOwner(Brand):
    org_id: str = Field(..., description="Organization ID that owns the brand.")

class BrandResponseByOwner(BrandCreateByOwner):
    # Mewarisi semua field Brand (name, email, dll)
    id: str = Field(serialization_alias="id")
    name: str
    status: Status
    org_id: str
    rec_date: Optional[datetime] = None
    rec_by: Optional[str] = None
    mod_date: Optional[datetime] = None
    mod_by: Optional[str] = None
    
    model_config = {
        "populate_by_name": True,
        "from_attributes": True
    }