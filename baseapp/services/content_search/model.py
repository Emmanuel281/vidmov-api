from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime

# ===== Search Request Models =====

class ContentSearchRequest(BaseModel):
    """Request model untuk search content"""
    q: Optional[str] = Field(None, description="Search query untuk title, synopsis, cast, tags")
    genres: Optional[List[str]] = Field(None, description="Filter by genre IDs")
    tags: Optional[List[str]] = Field(None, description="Filter by tags")
    cast: Optional[str] = Field(None, description="Filter by cast name")
    origin: Optional[str] = Field(None, description="Filter by origin country")
    territory: Optional[str] = Field(None, description="Filter by territory")
    min_rating: Optional[float] = Field(None, ge=0.0, le=10.0, description="Minimum rating")
    max_rating: Optional[float] = Field(None, ge=0.0, le=10.0, description="Maximum rating")
    mature_content: Optional[bool] = Field(None, description="Filter mature content")
    language: Optional[str] = Field("id", description="Language preference for search (id, en)")
    sort_by: str = Field("relevance", description="Sort by: relevance, rating, release_date, views")
    page: int = Field(1, ge=1, description="Page number")
    page_size: int = Field(20, ge=1, le=100, description="Items per page")

# ===== Search Response Models =====

class MediaFile(BaseModel):
    """Model untuk file media (poster, video)"""
    id: str
    filename: str
    url: Optional[str] = None
    path: Optional[str] = None
    info_file: Optional[Dict[str, Any]] = None

class BrandPlacementResponse(BaseModel):
    """Response model untuk brand placement"""
    org_id: str
    brand_name: str
    campaign_name: str

class GenreDetail(BaseModel):
    """Detail genre dari lookup"""
    id: str
    value: str
    sort: Optional[int] = None

class ContentSearchItem(BaseModel):
    """Single item dalam search results"""
    content_id: str = Field(alias="id")
    title: Dict[str, str]
    synopsis: Dict[str, str]
    genre: List[str]
    genre_details: Optional[List[GenreDetail]] = None
    release_date: datetime
    cast: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    origin: Optional[str] = None
    territory: Optional[List[str]] = None
    rating: Optional[float] = None
    mature_content: bool = False
    status: str
    
    # Media files (grouped by language)
    poster: Optional[Dict[str, MediaFile]] = None
    
    # Stats
    total_views: int = 0
    total_saved: int = 0
    total_episodes: int = 0
    
    # Monetization
    is_full_paid: bool = False
    full_price_coins: Optional[int] = None
    main_sponsor: Optional[BrandPlacementResponse] = None
    
    # Metadata
    rec_date: Optional[datetime] = None
    
    model_config = {
        "populate_by_name": True,
        "from_attributes": True
    }

class ContentSearchResponse(BaseModel):
    """Response model untuk search results"""
    total: int
    page: int
    page_size: int
    total_pages: int
    items: List[ContentSearchItem]

class ContentDetailSearchResponse(ContentSearchItem):
    """Detail content dari search (dengan video files)"""
    fyp_1: Optional[Dict[str, Dict[str, MediaFile]]] = None  # {language: {resolution: file}}
    fyp_2: Optional[Dict[str, Dict[str, MediaFile]]] = None
    license_from: Optional[str] = None
    licence_date_start: Optional[datetime] = None
    licence_date_end: Optional[datetime] = None
    
    model_config = {
        "populate_by_name": True,
        "from_attributes": True
    }

# ===== Autocomplete & Suggestions =====

class AutocompleteResponse(BaseModel):
    """Response untuk autocomplete"""
    suggestions: List[str]

class TrendingContentResponse(BaseModel):
    """Response untuk trending content"""
    items: List[ContentSearchItem]

class PopularTagResponse(BaseModel):
    """Response untuk popular tags"""
    tag: str
    count: int

class PopularTagsResponse(BaseModel):
    """Response list popular tags"""
    tags: List[PopularTagResponse]

class GenreListResponse(BaseModel):
    """Response list available genres"""
    genres: List[GenreDetail]

# ===== OpenSearch Document Model =====

class ContentOpenSearchDocument(BaseModel):
    """
    Model untuk dokumen yang disimpan di OpenSearch.
    Ini adalah flat structure untuk indexing.
    """
    content_id: str
    
    # Multi-language fields (stored as nested for searching)
    title_id: str  # Indonesian title
    title_en: Optional[str] = None  # English title
    title_all: str  # Combined for search
    
    synopsis_id: str
    synopsis_en: Optional[str] = None
    synopsis_all: str
    
    # Arrays
    genre: List[str]
    cast: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    territory: Optional[List[str]] = None
    
    # Single values
    release_date: datetime
    origin: Optional[str] = None
    rating: Optional[float] = None
    mature_content: bool = False
    status: str
    
    # Stats for sorting/filtering
    total_views: int = 0
    total_saved: int = 0
    total_episodes: int = 0
    
    # Monetization
    is_full_paid: bool = False
    full_price_coins: Optional[int] = None
    
    # Sponsor info (simplified for search)
    sponsor_name: Optional[str] = None
    sponsor_campaign: Optional[str] = None
    
    # License info
    license_from: Optional[str] = None
    licence_date_start: Optional[datetime] = None
    licence_date_end: Optional[datetime] = None
    
    # Metadata
    org_id: str
    rec_date: datetime
    mod_date: Optional[datetime] = None
    
    # Search helper field (combined searchable text)
    search_text: str
    
    model_config = {
        "populate_by_name": True
    }

# ===== Admin Sync Models =====

class SyncStatsResponse(BaseModel):
    """Response untuk sync statistics"""
    mongodb_total: int
    mongodb_active: int
    opensearch_total: int
    opensearch_size_mb: float
    is_synced: bool
    difference: int

class BulkSyncResponse(BaseModel):
    """Response untuk bulk sync operation"""
    total_processed: int
    success_count: int
    failed_count: int
    duration_seconds: float
    items_per_second: float