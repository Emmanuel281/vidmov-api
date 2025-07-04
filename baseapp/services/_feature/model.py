from pydantic import BaseModel, Field

class Feature(BaseModel):
    f_id: str = Field(description="Feature ID")
    r_id: str = Field(description="Role ID")
    key_action: str = Field(description="key of permission [view,add,edit,delete,export,import,approval,setting]")