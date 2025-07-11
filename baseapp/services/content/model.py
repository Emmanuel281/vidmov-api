from pydantic import BaseModel, Field
from typing import List, Optional
from baseapp.model.common import StatusContent, TypeContent, SubscriptionTier
from datetime import datetime

class Content(BaseModel):
    name: str = Field(description="Name of movie or series.")
    description: str = Field(description="Synopsis of movie or series.")
    cast: str = Field(description="Cast of movie or series.")
    genre: List[str] = Field(description="Genre of the movie or series.")
    release_date: datetime = Field(description="Release date of the movie or series (YYYY-MM-DD). example: 2025-03-20T00:00:00Z")
    duration: Optional[int] = Field(default=None, description="Duration in minutes.")
    type: TypeContent = Field(description="Type of content: 'MOVIE' or 'SERIES'.")
    episodes: Optional[int] = Field(default=None, description="Number of episodes (if series).")
    language: Optional[str] = Field(default="Indonesia", description="Primary language of the content.")
    rating: Optional[float] = Field(default=None, ge=0.0, le=10.0, description="Rating score of the content.")
    status: StatusContent = Field(default=None, description="Status of the content (e.g., Published, Draft, Archived).")
    required_tier: Optional[SubscriptionTier] = Field(default=None, description="Minimum subscription tier to access this entire series.")

class ContentUpdate(BaseModel):
    name: str = Field(description="Name of movie or series.")
    description: str = Field(description="Synopsis of movie or series.")
    cast: str = Field(description="Cast of movie or series.")
    genre: List[str] = Field(description="Genre of the movie or series.")
    release_date: datetime = Field(description="Release date of the movie or series (YYYY-MM-DD). example: 2025-03-20T00:00:00Z")
    duration: Optional[int] = Field(default=None, description="Duration in minutes.")
    type: TypeContent = Field(description="Type of content: 'MOVIE' or 'SERIES'.")
    episodes: Optional[int] = Field(default=None, description="Number of episodes (if series).")
    language: Optional[str] = Field(default="Indonesia", description="Primary language of the content.")

class ContentUpdateStatus(BaseModel):
    """Representation of update status model."""
    status: StatusContent = Field(description="Status of the content (e.g., Published, Draft, Archived).")    

class ContentSetTier(BaseModel):
    """Representation of set rating model."""
    required_tier: Optional[SubscriptionTier] = Field(default=None, description="Minimum subscription tier to access this entire series.")