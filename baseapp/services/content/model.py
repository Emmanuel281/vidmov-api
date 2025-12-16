from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from baseapp.model.common import ContentStatus
from datetime import datetime
from pydantic import field_validator

class BrandPlacement(BaseModel):
    org_id: str = Field(..., description="ID dari user/organisasi Brand (Authority 8)")
    brand_name: str = Field(..., description="Nama brand yang akan muncul di player")
    campaign_name: str
    # logo_url: str = Field(..., description="URL logo brand yang akan muncul di player")
    # cta_url: Optional[str] = Field(None, description="Link redirect jika user klik logo")

class Content(BaseModel):
    title: Dict[str, str] = Field(description="Title in multiple languages. Example: {'id': 'Judul', 'en': 'Title'}")
    synopsis: Dict[str, str] = Field(description="Synopsis in multiple languages.")
    genre: List[str] = Field(description="Genre of the short drama.")
    release_date: datetime = Field(description="Release date of the short drama (YYYY-MM-DD). example: 2025-03-20T00:00:00Z")
    cast: Optional[List[str]] = Field(default=None, description="Cast of short drama.")
    tags: Optional[List[str]] = Field(default=None, description="Tags of the short drama.")
    license_from: Optional[str] = Field(default=None, description="License from whom the content is acquired. E.g., 'Turkey, Korea, Indonesia'")
    licence_date: Optional[datetime] = Field(default=None, description="License date of the short drama (YYYY-MM-DD). example: 2025-03-20T00:00:00Z")
    origin: Optional[str] = Field(default="Indonesia", description="Origin country of the content.")
    territory: Optional[List[str]] = Field(default=None, description="Territory for publish of the content.")
    rating: Optional[float] = Field(default=None, ge=0.0, le=10.0, description="Rating score of the content.")
    mature_content: bool = Field(default=False, description="Indicates if the content is for mature audiences.")
    # Monetization & Branding (Level Series)
    is_full_paid: bool = Field(default=False, description="Jika True, user harus beli 1 paket full. Jika False, beli per episode.")
    full_price_coins: Optional[int] = Field(default=None, description="Harga koin jika user ingin beli langsung 1 season.")
    # Sponsorship (Contoh: 'Presented by Kopi Kenangan')
    main_sponsor: Optional[BrandPlacement] = Field(default=None, description="Sponsor utama series ini.")
    status: ContentStatus = Field(default=None, description="Status of the content (e.g., Published, Draft, Archived).")

    @field_validator('title', 'synopsis')
    def check_default_language(cls, v):
        # Pastikan key 'id' ada
        if 'id' not in v:
            raise ValueError("Bahasa Indonesia ('id') wajib diisi sebagai default.")
        return v

class ContentUpdate(BaseModel):
    title: Dict[str, str] = Field(description="Title in multiple languages. Example: {'id': 'Judul', 'en': 'Title'}")
    synopsis: Dict[str, str] = Field(description="Synopsis in multiple languages.")
    genre: List[str] = Field(description="Genre of the short drama.")
    release_date: datetime = Field(description="Release date of the short drama (YYYY-MM-DD). example: 2025-03-20T00:00:00Z")
    cast: Optional[List[str]] = Field(default=None, description="Cast of short drama.")
    tags: Optional[List[str]] = Field(default=None, description="Tags of the short drama.")
    license_from: str = Field(description="License from whom the content is acquired. E.g., 'Turkey, Korea, Indonesia'")
    licence_date: Optional[datetime] = Field(default=None, description="License date of the short drama (YYYY-MM-DD). example: 2025-03-20T00:00:00Z")
    origin: Optional[str] = Field(default="Indonesia", description="Origin country of the content.")
    territory: Optional[List[str]] = Field(default=None, description="Territory for publish of the content.")
    rating: Optional[float] = Field(default=None, ge=0.0, le=10.0, description="Rating score of the content.")
    mature_content: bool = Field(default=False, description="Indicates if the content is for mature audiences.")
    # Monetization & Branding (Level Series)
    is_full_paid: bool = Field(default=False, description="Jika True, user harus beli 1 paket full. Jika False, beli per episode.")
    full_price_coins: Optional[int] = Field(default=None, description="Harga koin jika user ingin beli langsung 1 season.")
    # Sponsorship (Contoh: 'Presented by Kopi Kenangan')
    main_sponsor: Optional[BrandPlacement] = Field(default=None, description="Sponsor utama series ini.")

    @field_validator('title', 'synopsis')
    def check_default_language(cls, v):
        # Pastikan key 'id' ada
        if 'id' not in v:
            raise ValueError("Bahasa Indonesia ('id') wajib diisi sebagai default.")
        return v

class ContentUpdateStatus(BaseModel):
    """Representation of update status model."""
    status: ContentStatus = Field(description="Status of the content (e.g., Published, Draft, Archived).")

class ContentResponse(Content):
    # Mewarisi semua field Organization (name, email, dll)
    id: str = Field(serialization_alias="id")
    genre_details: Optional[List[Any]] = None
    poster: Optional[Dict[str,str]] = None
    fyp: Optional[List[Dict]] = None
    highlight: Optional[List[Dict]] = None
    rec_date: Optional[datetime] = None
    rec_by: Optional[str] = None
    mod_date: Optional[datetime] = None
    mod_by: Optional[str] = None
    
    model_config = {
        "populate_by_name": True,
        "from_attributes": True
    }

class ContentListItem(BaseModel):
    """Response model untuk item dalam list (lebih ringkas)"""
    id: str
    title: str
    synopsis: str
    genre: List[str] = None
    total_views: Optional[int] = 0
    total_saved: Optional[int] = 0
    total_episodes: Optional[int] = 0
    poster: Optional[Dict[str,str]] = None
    status: ContentStatus
    
    model_config = {
        "populate_by_name": True,
        "from_attributes": True
    }