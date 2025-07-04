from pydantic import BaseModel, Field, EmailStr

class OTPRequest(BaseModel):
    email: EmailStr = Field(description="Email")

class VerifyOTPRequest(BaseModel):
    email: EmailStr = Field(description="Email")
    otp: str = Field(description="OTP Code.")

class ResetPasswordRequest(BaseModel):
    email: EmailStr = Field(description="Email")
    reset_token: str = Field(description="Random token for reset")
    new_password: str = Field(description="OTP Code.")