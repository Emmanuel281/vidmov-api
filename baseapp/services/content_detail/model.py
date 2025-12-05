from pydantic import BaseModel, Field
from typing import Optional
from baseapp.model.common import ContentStatus
from baseapp.services.content.model import BrandPlacement
from datetime import datetime

class ContentDetail(BaseModel):
    content_id: str = Field(..., description="Reference ID to the main content.")
    title: str = Field(..., description="Title of this episode or video segment.")
    description: Optional[str] = Field(default=None, description="Synopsis of this episode or video segment.")
    episode: Optional[int] = Field(default=None, description="Episode number if content is a series.")
    duration: int = Field(..., description="Duration of the video in minutes.")
    release_date: Optional[datetime] = Field(default=None, description="Release date of the video.")
    rating: Optional[float] = Field(default=None, ge=0.0, le=10.0, description="Rating score of the content.")
    # --- LOGIKA KOIN DISINI (PENGGANTI TIER) ---
    is_free: bool = Field(default=False, description="Apakah episode ini gratis? (Biasanya eps 1-5 True)")
    episode_price: int = Field(default=0, description="Harga koin untuk membuka episode ini. 0 jika is_free=True.")
    # --- LOGIKA BRAND DISINI ---
    # Iklan spesifik di episode tertentu (misal: Episode 10 ada iklan sampo)
    episode_sponsor: Optional[BrandPlacement] = Field(default=None)
    status: ContentStatus = Field(default="draft", description="Status of the episode/video.")

class ContentDetailUpdate(BaseModel):
    content_id: str = Field(..., description="Reference ID to the main content.")
    title: str = Field(..., description="Title of this episode or video segment.")
    description: Optional[str] = Field(default=None, description="Synopsis of this episode or video segment.")
    episode: Optional[int] = Field(default=None, description="Episode number if content is a series.")
    duration: int = Field(..., description="Duration of the video in minutes.")
    release_date: Optional[datetime] = Field(default=None, description="Release date of the video.")
    # --- LOGIKA KOIN DISINI (PENGGANTI TIER) ---
    is_free: bool = Field(default=False, description="Apakah episode ini gratis? (Biasanya eps 1-5 True)")
    episode_price: int = Field(default=0, description="Harga koin untuk membuka episode ini. 0 jika is_free=True.")
    # --- LOGIKA BRAND DISINI ---
    # Iklan spesifik di episode tertentu (misal: Episode 10 ada iklan sampo)
    episode_sponsor: Optional[BrandPlacement] = Field(default=None)
    
class ContentDetailUpdateStatus(BaseModel):
    """Representation of update status model."""
    status: ContentStatus = Field(description="Status of the content (e.g., Published, Draft, Archived).")