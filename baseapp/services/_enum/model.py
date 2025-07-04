from uuid import UUID
from pydantic import BaseModel, Field, field_validator
from typing import Optional, Union, Dict, Literal

class CustomDataModel(BaseModel):
    key: str
    value: Union[str, int, Dict[str, str]]

class Enum(BaseModel):
    id: Optional[Union[UUID, str]] = Field(None, description="Custom ID")
    app: str = Field(description="App of the enum data.")
    mod: str = Field(description="Module of the enum data.")
    code: str = Field(description="Code of the enum data.")
    type: Literal["hardcoded","user"] = Field(description="Type of the enum data is hardcoded or user")
    value: Union[str,int,CustomDataModel]
    sort: int = Field(description="Sort of the enum data.")
    
    @field_validator("id")
    def validate_id(cls, value):
        print(f"value: {value}")
        # Jika bukan UUID, string harus memenuhi kriteria tertentu
        if not value or (isinstance(value, str) and len(value.strip()) == 0):
            raise ValueError("Custom ID must be a non-empty string.")
        return value

class EnumUpdate(BaseModel):
    app: str = Field(description="App of the enum data.")
    mod: str = Field(description="Module of the enum data.")
    code: str = Field(description="App of the enum data.")
    type: Literal["hardcoded","user"] = Field(description="Type of the enum data is hardcoded or user")
    value: Union[str,int,CustomDataModel]
    sort: int = Field(description="Sort of the enum data.")