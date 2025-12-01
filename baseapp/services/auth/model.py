from pydantic import BaseModel, Field
from typing import Optional, List, Dict

class UserLoginModel(BaseModel):
    """Representation of a user login."""
    username: Optional[str] = Field(
        default=None, description="User name use for login.")
    password: Optional[str] = Field(
        default=None, description="Password of the user.")
    
class UserInfo(BaseModel):
    """Representation of a user information."""
    id: Optional[str] = Field(default=None, description="Id of the user.")
    org_id: Optional[str] = Field(default=None, description="Organization associated with the user.")
    roles: List[str] = Field(description="Roles of the user")
    authority: Optional[int] = Field(default=None, description="Authorization of the organization associated with the user, as owner or client.")
    bitws: Optional[Dict] = Field(default=None, description="Dictionary of bitwise.")
    feature: Optional[Dict] = Field(default=None, description="Dictionary of the feature has assigned in user.")

class VerifyOTPRequest(BaseModel):
    username: str = Field(description="Email")
    otp: str = Field(description="OTP Code.")

class ClientInfo(BaseModel):
    """Representation of a client information."""
    id: str = Field(default=None, description="Id of the client.")
    org_id: str = Field(default=None, description="Organization associated with the client.")
    client_id: str = Field(default=None, description="Client ID.")

class ClientAuthCredential(BaseModel):
    """Representation of a client authentication credential."""
    client_id: str = Field(description="Client ID for authentication.")
    client_secret: str = Field(default=None, description="Client secret for authentication.")