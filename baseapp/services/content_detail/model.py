from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Dict, Any
from baseapp.model.common import ContentStatus
from baseapp.services.content.model import BrandPlacement
from datetime import datetime

class ContentDetail(BaseModel):
    content_id: str = Field(..., description="Reference ID to the main content.")
    episode: Optional[int] = Field(default=None, description="Episode number if content is a series.")
    duration: int = Field(..., description="Duration of the video in minutes.")
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
    episode: Optional[int] = Field(default=None, description="Episode number if content is a series.")
    duration: int = Field(..., description="Duration of the video in minutes.")
    # --- LOGIKA KOIN DISINI (PENGGANTI TIER) ---
    is_free: bool = Field(default=False, description="Apakah episode ini gratis? (Biasanya eps 1-5 True)")
    episode_price: int = Field(default=0, description="Harga koin untuk membuka episode ini. 0 jika is_free=True.")
    # --- LOGIKA BRAND DISINI ---
    # Iklan spesifik di episode tertentu (misal: Episode 10 ada iklan sampo)
    episode_sponsor: Optional[BrandPlacement] = Field(default=None)
    
class ContentDetailUpdateStatus(BaseModel):
    """Representation of update status model."""
    status: ContentStatus = Field(description="Status of the content (e.g., Published, Draft, Archived).")

class ContentDetailResponse(ContentDetail):
    # Mewarisi semua field Organization (name, email, dll)
    id: str = Field(serialization_alias="id")
    
    episode: Optional[str] = Field(default=None, description="Episode number formatted (01, 02..)")

    video: Optional[Dict[str, Dict[str, Any]]] = None
    subtitle: Optional[Dict[str, Dict[str, Any]]] = None
    dubbing: Optional[Dict[str, Dict[str, Any]]] = None

    rec_date: Optional[datetime] = None
    rec_by: Optional[str] = None
    mod_date: Optional[datetime] = None
    mod_by: Optional[str] = None
    
    model_config = {
        "populate_by_name": True,
        "from_attributes": True
    }

    @field_validator('episode', mode='before')
    @classmethod
    def format_episode_number(cls, v):
        if v is None:
            return None
        # Jika nilai adalah integer (dari DB), format dengan padding 0
        if isinstance(v, int):
            return f"{v:02d}" # Format: 1 -> "01", 10 -> "10"
        return str(v)

class ContentDetailListItem(BaseModel):
    """Response model untuk item dalam list (lebih ringkas)"""
    id: str
    episode: Optional[str] = Field(default=None, description="Episode number formatted (01, 02..)")
    is_free: bool = False
    
    # Analisis statistik sederhana
    total_views: Optional[int] = 0
    total_saved: Optional[int] = 0
    total_episodes: Optional[int] = 0
    retention_rate: Optional[float] = 0.0

    rec_date: Optional[datetime] = None
    status: ContentStatus

    model_config = {
        "populate_by_name": True,
        "from_attributes": True
    }

    @field_validator('episode', mode='before')
    @classmethod
    def format_episode_number(cls, v):
        if v is None:
            return None
        if isinstance(v, int):
            return f"{v:02d}"
        return str(v)