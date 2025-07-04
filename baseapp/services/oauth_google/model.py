from pydantic import BaseModel, Field

class Google(BaseModel):
    id: str = Field(description="Google ID")
    email: str = Field(description="Google Email")
    name: str = Field(description="Google Name")
    picture: str = Field(description="Google Picture")

class GoogleToken(BaseModel):
    access_token: str = Field(description="Access Token")