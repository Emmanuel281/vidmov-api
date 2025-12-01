from pydantic import BaseModel, Field
from typing import List
from baseapp.model.common import Status

class User(BaseModel):
    username: str = Field(description="User name use for login.", error_msg_templates={"value_error.missing": "Username is required!"})
    email: str = Field(description="Email of the user.", error_msg_templates={"value_error.missing": "Email cannot be empty"})
    password: str = Field(description="Password of the user.")
    roles: List[str] = Field(description="Roles of the user")
    status: Status = Field(default=None, description="Status of the user.")

class UpdateByAdmin(BaseModel):
    username: str = Field(description="User name use for login.", error_msg_templates={"value_error.missing": "Username is required!"})
    email: str = Field(description="Email of the user.", error_msg_templates={"value_error.missing": "Email cannot be empty"})
    roles: List[str] = Field(description="Roles of the user")
    status: Status = Field(default=None, description="Status of the user.")

class UpdateUsername(BaseModel):
    """Representation of update username model."""
    username: str = Field(description="User name use for login.")

class UpdateEmail(BaseModel):
    """Representation of update email model."""
    email: str = Field(description="Email of the user.")

class UpdateRoles(BaseModel):
    """Representation of update roles model."""
    roles: List[str] = Field(description="Roles of the user")

class ChangePassword(BaseModel):
    """Representation of change password model."""
    old_password: str = Field(description="Old password.", default=None)
    new_password: str = Field(description="New password.")
    verify_password: str = Field(description="Retype new password.")

class ResetPassword(BaseModel):
    """Representation of reset password model."""
    new_password: str = Field(description="New password.")
    verify_password: str = Field(description="Retype new password.")

class UpdateCoin(BaseModel):
    """Representation of update balance coin model."""
    balance_coin: int = Field(description="Balance coin of the user.")
