from pydantic import BaseModel, Field
from typing import Optional, List, Dict
from baseapp.model.common import Status

class DocType(BaseModel):
    name: str = Field(description="Doctype name.")
    metadata: List[str] = Field(description="List key of index.")
    folder: str = Field(description="Structure folder when created of doctype")
    mapping: Optional[Dict] = Field(deafult=None, description="Mapping index with field of table")
    status: Optional[Status] = Field(default=None, description="Status of doctype.")

class DocTypeUpdate(BaseModel):
    metadata: List[str] = Field(description="List key of index.")
    folder: str = Field(description="Structure folder when created of doctype")
    mapping: Optional[Dict] = Field(deafult=None, description="Mapping index with field of table")
    status: Optional[Status] = Field(default=None, description="Status of doctype.")