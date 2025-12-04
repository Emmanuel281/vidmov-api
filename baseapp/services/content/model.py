from pydantic import BaseModel, Field
from typing import List, Optional
from baseapp.model.common import ContentStatus
from datetime import datetime

class BrandPlacement(BaseModel):
    org_id: str = Field(..., description="ID dari user/organisasi Brand (Authority 8)")
    brand_name: str = Field(..., description="Nama brand yang akan muncul di player")
    campaign_name: str
    # logo_url: str = Field(..., description="URL logo brand yang akan muncul di player")
    # cta_url: Optional[str] = Field(None, description="Link redirect jika user klik logo")

class Content(BaseModel):
    name: str = Field(description="Name of movie or series.")
    description: str = Field(description="Synopsis of movie or series.")
    cast: str = Field(description="Cast of movie or series.")
    genre: List[str] = Field(description="Genre of the movie or series.")
    release_date: datetime = Field(description="Release date of the movie or series (YYYY-MM-DD). example: 2025-03-20T00:00:00Z")
    duration: Optional[int] = Field(default=None, description="Duration in minutes.")
    episodes: Optional[int] = Field(default=None, description="Number of episodes (if series).")
    language: Optional[str] = Field(default="Indonesia", description="Primary language of the content.")
    rating: Optional[float] = Field(default=None, ge=0.0, le=10.0, description="Rating score of the content.")
    # Monetization & Branding (Level Series)
    is_full_paid: bool = Field(default=False, description="Jika True, user harus beli 1 paket full. Jika False, beli per episode.")
    full_price_coins: Optional[int] = Field(default=None, description="Harga koin jika user ingin beli langsung 1 season.")
    # Sponsorship (Contoh: 'Presented by Kopi Kenangan')
    main_sponsor: Optional[BrandPlacement] = Field(default=None, description="Sponsor utama series ini.")
    status: ContentStatus = Field(default=None, description="Status of the content (e.g., Published, Draft, Archived).")

class ContentUpdate(BaseModel):
    name: str = Field(description="Name of movie or series.")
    description: str = Field(description="Synopsis of movie or series.")
    cast: str = Field(description="Cast of movie or series.")
    genre: List[str] = Field(description="Genre of the movie or series.")
    release_date: datetime = Field(description="Release date of the movie or series (YYYY-MM-DD). example: 2025-03-20T00:00:00Z")
    duration: Optional[int] = Field(default=None, description="Duration in minutes.")
    episodes: Optional[int] = Field(default=None, description="Number of episodes (if series).")
    language: Optional[str] = Field(default="Indonesia", description="Primary language of the content.")
    # Monetization & Branding (Level Series)
    is_full_paid: bool = Field(default=False, description="Jika True, user harus beli 1 paket full. Jika False, beli per episode.")
    full_price_coins: Optional[int] = Field(default=None, description="Harga koin jika user ingin beli langsung 1 season.")
    # Sponsorship (Contoh: 'Presented by Kopi Kenangan')
    main_sponsor: Optional[BrandPlacement] = Field(default=None, description="Sponsor utama series ini.")

class ContentUpdateStatus(BaseModel):
    """Representation of update status model."""
    status: ContentStatus = Field(description="Status of the content (e.g., Published, Draft, Archived).")    