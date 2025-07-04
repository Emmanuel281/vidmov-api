from pydantic import BaseModel, Field
from typing import Optional, Dict

class UploadFile(BaseModel):
    filename: str = Field(description="UUID Generate filename.")
    filestat: Dict = Field(description="Object filename. ")
    folder_id: str = Field(description="Reference folder ID")
    folder_path: Optional[str] = Field(description="Structure folder")
    
class SetMetaData(BaseModel):
    doctype: str = Field(description="Reference doctype id")
    metadata: Optional[Dict] = Field(default=None, description="Type of index")
    refkey_id: Optional[str] = Field(default=None, description="Reference ID")
    refkey_table: Optional[str] = Field(default=None, description="Reference table")
    refkey_name: Optional[str] = Field(default=None, description="Reference name|label|anthing")

class MoveToTrash(BaseModel):
    is_deleted: int = Field(default=0, description="Deletion status: 1 = deleted, 0 = not deleted")