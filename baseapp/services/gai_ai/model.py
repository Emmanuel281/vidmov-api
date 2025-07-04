from pydantic import BaseModel, Field

class Prompt(BaseModel):
    prompt: str = Field(default="black", description="User prompt")