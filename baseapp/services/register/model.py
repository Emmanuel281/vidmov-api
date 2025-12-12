from pydantic import BaseModel, Field


class Register(BaseModel):
    email: str = Field(description="Email of the user.", error_msg_templates={"value_error.missing": "Email cannot be empty"})
    phone_number: str = Field(description="Phone number of the user.", error_msg_templates={"value_error.missing": "Phone number cannot be empty"})
    password: str = Field(description="Password of the user.")

class RegisterResponse(BaseModel):
    session: str = Field(description="Session", error_msg_templates={"value_error.missing": "Session cannot be empty"})

class ResendOtpRequest(BaseModel):
    session: str = Field(description="Session", error_msg_templates={"value_error.missing": "Session cannot be empty"})

class VerifyOtp(BaseModel):
    session: str = Field(description="Session", error_msg_templates={"value_error.missing": "Session cannot be empty"})
    otp: str = Field(description="OTP", error_msg_templates={"value_error.missing": "OTP cannot be empty"})