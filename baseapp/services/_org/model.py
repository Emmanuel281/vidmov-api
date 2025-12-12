from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List, Any
from baseapp.model.common import Status

class Organization(BaseModel):
    org_name: str = Field(description="Name of the organization.", error_msg_templates={"value_error.missing": "Name is required!"})
    org_initial: Optional[str] = Field(default=None, description="Short initial or abbreviation of the organization.")
    org_email: str = Field(description="Email address of the organization.", error_msg_templates={"value_error.missing": "Email cannot be empty"})
    org_phone: str = Field(description="Phone number of the organization.", error_msg_templates={"value_error.missing": "Phonenumber cannot be empty"})
    authority: int = Field(description="Authority level of the organization.", error_msg_templates={"value_error.missing": "Auth cannot be empty"})
    org_address: Optional[str] = Field(default=None, description="Address of the organization.")
    org_desc: Optional[str] = Field(default=None, description="Description about the organization (e.g., vision, mission).")
    status: Status = Field(default=None, description="Status of the organization.")

class User(BaseModel):
    username: str = Field(description="User name use for login.", error_msg_templates={"value_error.missing": "Username is required!"})
    email: str = Field(description="Email of the user.", error_msg_templates={"value_error.missing": "Email cannot be empty"})
    password: str = Field(description="Password of the user.")
    status: Status = Field(default=None, description="Status of the user.")

class Role(BaseModel):
    color: Optional[str] = Field(default="#4DABF5", description="label color of role")
    name: str = Field(description="Role name")
    org_id: str = Field(description="Organization associated with the role.")
    status: Status = Field(default=None, description="Status of the role.")

class OrganizationUpdate(BaseModel):
    org_name: str = Field(description="Name of the organization.", error_msg_templates={"value_error.missing": "Name is required!"})
    org_initial: Optional[str] = Field(default=None, description="Short initial or abbreviation of the organization.")
    org_email: str = Field(description="Email address of the organization.", error_msg_templates={"value_error.missing": "Email cannot be empty"})
    org_phone: str = Field(description="Phone number of the organization.", error_msg_templates={"value_error.missing": "Phonenumber cannot be empty"})
    org_address: Optional[str] = Field(default=None, description="Address of the organization.")
    org_desc: Optional[str] = Field(default=None, description="Description about the organization (e.g., vision, mission).")

class InitRequest(BaseModel):
    org: Organization
    user: User

class OrganizationResponse(Organization):
    # Mewarisi semua field Organization (name, email, dll)
    id: str = Field(serialization_alias="id")
    storage: Optional[int] = None
    usedstorage: Optional[int] = None
    rec_date: Optional[datetime] = None
    rec_by: Optional[str] = None
    mod_date: Optional[datetime] = None
    mod_by: Optional[str] = None
    
    model_config = {
        "populate_by_name": True,
        "from_attributes": True
    }

class OrganizationListItem(BaseModel):
    """Response model untuk item dalam list (lebih ringkas)"""
    id: str
    org_name: str
    org_initial: Optional[str] = None
    org_phone: str
    org_address: Optional[str] = None
    org_desc: Optional[str] = None
    org_email: str
    status: Status
    
    model_config = {
        "populate_by_name": True,
        "from_attributes": True
    }
    
class UserResponse(BaseModel):
    # Kita redefine field agar password tidak ikut terkirim
    id: str = Field(serialization_alias="id") # Mapping _id dari mongo ke id
    username: str
    email: str
    status: Optional[Status] = None
    roles: Optional[List[str]] = []
    balance_coin: Optional[int] = None
    
    # Konfigurasi agar bisa membaca data dari dict/object mongo
    model_config = {
        "populate_by_name": True,
        "from_attributes": True
    }

class InitResponse(BaseModel):
    # Model khusus untuk return function init (gabungan org & user)
    org: OrganizationResponse
    user: UserResponse

class OrganizationListResponse(BaseModel):
    """Response untuk get_all dengan data list"""
    data: List[OrganizationListItem]
    
    model_config = {
        "from_attributes": True
    }