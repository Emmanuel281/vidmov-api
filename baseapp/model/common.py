from pydantic import BaseModel, Field
from typing import Optional, Any, List
from enum import Enum, IntEnum

MINIO_STORAGE_SIZE_LIMIT : int = 10737418240  # 10 GB in bytes

class Status(str, Enum):
    """Status of a user and client"""
    ACTIVE = "ACTIVE"
    SUSPENDED = "SUSPENDED"
    DELETED = "DELETED"

class CurrentUser(BaseModel):
    """current user"""
    id: str
    name: str = Field(description="Content would be username or email or phonenumber")
    roles: List
    org_id: str
    token: str
    authority: int
    features: Optional[dict] = None
    bitws: Optional[dict] = None
    log_id: Optional[str] = None
    ip_address: Optional[str] = None
    user_agent : Optional[str] = None

class CurrentClient(BaseModel):
    """current client"""
    id: str
    client_id: str = Field(description="Client ID")
    org_id: str
    token: str
    log_id: Optional[str] = None
    ip_address: Optional[str] = None
    user_agent : Optional[str] = None
    
class Pagination(BaseModel):
    """Pagination details."""
    total_items: int = Field(description="Total number of items.")
    total_pages: int = Field(description="Total number of pages.")
    current_page: int = Field(description="Current page.")
    items_per_page: int = Field(description="Number of items per page.")
class ApiResponse(BaseModel):
    """Representation of API response."""
    status: int = Field(description="Status of response, 0 is successfully.")
    message: Optional[str] = Field(
        default=None, description="Explaination of the error.")
    data: Optional[Any] = Field(
        default=None, description="Content of result from API call.")
    pagination: Optional[Pagination] = Field(
        default=None, description="Pagination details if applicable.")

class UpdateStatus(BaseModel):
    """Representation of update status model."""
    status: Status = Field(description="Status of the data.")

class DMSOperationType(str, Enum):
    TO_TRASH = "to_trash"
    RESTORE = "restore"

class DMSOperationType(str, Enum):
    TO_TRASH = "to_trash"
    RESTORE = "restore"

class DMSDataType(str, Enum):
    Str = "Str"
    Int = "Int"
    Float = "Float"
    Datetime = "Datetime"

class Authority(IntEnum):
    OWNER = 1
    PARTNER = 2
    MEMBER = 4
    AFFILIATOR = 8
    @property
    def label(self) -> str:
        match self:
            case Authority.OWNER: return "Arena"
            case Authority.PARTNER: return "Partner"
            case Authority.MEMBER: return "Member"
            case Authority.AFFILIATOR: return "Affiliator"
            case _: return self.value.upper() # Fallback

class RoleAction(IntEnum):
    VIEW = 1
    ADD = 2
    EDIT = 4
    DELETE = 8
    EXPORT = 16
    IMPORT = 32
    APPROVAL = 64
    SETTING = 128
    @property
    def label(self) -> str:
        match self:
            case RoleAction.VIEW: return "Read"
            case RoleAction.ADD: return "Create"
            case RoleAction.EDIT: return "Update"
            case RoleAction.DELETE: return "Delete"
            case RoleAction.EXPORT: return "Export"
            case RoleAction.IMPORT: return "Import"
            case RoleAction.APPROVAL: return "Approval"
            case RoleAction.SETTING: return "Setting"
            case _: return self.value.upper() # Fallback

class ContentStatus(str, Enum):
    """Status of a content"""
    PUBLISHED = "PUBLISHED"
    UNPUBLISHED = "UNPUBLISHED"
    SCHEDULED = "SCHEDULED"
    DRAFT = "DRAFT"
    ARCHIVED = "ARCHIVED"

class WalletType(str, Enum):
    """Type of a wallet"""
    TOPUP = "TOPUP"
    TRF_IN = "TRF_IN"
    TRF_OUT = "TRF_OUT"
    REWARD = "REWARD"
    USE = "USE"

class LanguageCode(str, Enum):
    """Type of a language"""
    EN = "en"
    ID = "id"
    @property
    def label(self) -> str:
        match self:
            case LanguageCode.EN: return "English"
            case LanguageCode.ID: return "Indonesia"
            case _: return self.value.upper() # Fallback

class ContentResolution(str, Enum):
    """Resolution of a content"""
    SD = "SD"
    HD = "HD"
    UHD = "4K"

class LicenseFrom(str, Enum):
    """Type of a License From"""
    Turkey = "Turkey"
    China = "China"
    Australia = "Australia"

class Territory(str, Enum):
    """Type of a License From"""
    Indonesia = "Indonesia"
    Malaysia = "Malaysia"
    Singapore = "Singapore"
    Australia = "Australia"
    Turkey = "Turkey"