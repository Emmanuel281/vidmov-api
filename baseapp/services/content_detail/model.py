from pydantic import BaseModel, Field
from typing import Optional
from baseapp.model.common import StatusContent
from datetime import datetime

class ContentDetail(BaseModel):
    content_id: str = Field(..., description="Reference ID to the main content.")
    title: str = Field(..., description="Title of this episode or video segment.")
    episode: Optional[int] = Field(default=None, description="Episode number if content is a series.")
    duration: int = Field(..., description="Duration of the video in minutes.")
    release_date: Optional[datetime] = Field(default=None, description="Release date of the video.")
    is_free: bool = Field(default=True, description="Whether the video is available for free.")
    rating: Optional[float] = Field(default=None, ge=0.0, le=10.0, description="Rating score of the content.")
    status: StatusContent = Field(default="draft", description="Status of the episode/video.")

class UpdateStatus(BaseModel):
    """Representation of update status model."""
    status: StatusContent = Field(description="Status of the content (e.g., Published, Draft, Archived).")