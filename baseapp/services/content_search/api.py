from typing import Optional
from fastapi import APIRouter, Query, Depends, HTTPException

from baseapp.model.common import ApiResponse, CurrentUser, Authority
from baseapp.utils.jwt import get_current_user_optional, get_current_user
from baseapp.services.content_search.crud import ContentSearchCRUD

router = APIRouter(prefix="/v1/content/search", tags=["Content Search"])

# ===== Public Search Endpoints (untuk Mobile Apps) =====

@router.get("/", response_model=ApiResponse)
async def search_contents(
    q: Optional[str] = Query(None, description="Search query"),
    genres: Optional[str] = Query(None, description="Comma-separated genre IDs"),
    tags: Optional[str] = Query(None, description="Comma-separated tags"),
    cast: Optional[str] = Query(None, description="Filter by cast name"),
    origin: Optional[str] = Query(None, description="Filter by origin country"),
    territory: Optional[str] = Query(None, description="Filter by territory"),
    min_rating: Optional[float] = Query(None, ge=0.0, le=10.0, description="Minimum rating"),
    max_rating: Optional[float] = Query(None, ge=0.0, le=10.0, description="Maximum rating"),
    mature_content: Optional[bool] = Query(None, description="Filter mature content"),
    language: str = Query("id", description="Language preference (id, en)"),
    sort_by: str = Query("relevance", description="Sort by: relevance, rating, views, release_date"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    cu: Optional[CurrentUser] = Depends(get_current_user_optional)
) -> ApiResponse:
    """
    Search content catalog dengan full-text search dan multiple filters.
    
    **Features:**
    - Multi-language search (Indonesian & English)
    - Filter by: genres, tags, cast, origin, territory, rating, mature content
    - Multiple sorting options
    - Pagination support
    
    **Example queries:**
    - Search by title: `?q=cinta&language=id`
    - Filter by genre: `?genres=genre1,genre2&sort_by=rating`
    - Combine search & filter: `?q=action&min_rating=4.0&mature_content=false`
    """
    try:
        with ContentSearchCRUD() as crud:
            if cu:
                crud.set_context(user_id=cu.id, org_id=cu.org_id)
            
            # Parse comma-separated values
            genres_list = genres.split(',') if genres else None
            tags_list = tags.split(',') if tags else None
            
            result = crud.search_contents(
                query=q,
                genres=genres_list,
                tags=tags_list,
                cast=cast,
                origin=origin,
                territory=territory,
                min_rating=min_rating,
                max_rating=max_rating,
                mature_content=mature_content,
                language=language,
                sort_by=sort_by,
                page=page,
                page_size=page_size
            )
            
            return ApiResponse(
                status=0,
                message="Search completed",
                data=result.get("data", []),
                pagination=result.get("pagination", None)
            )
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")

@router.get("/{content_id}", response_model=ApiResponse)
async def get_content_detail(
    content_id: str,
    cu: Optional[CurrentUser] = Depends(get_current_user_optional)
) -> ApiResponse:
    """
    Get detail content by ID dari OpenSearch.
    
    Includes:
    - Full content metadata
    - Multi-language title & synopsis
    - Genre details
    - Poster images (grouped by language)
    - Video files (FYP #1 & #2, grouped by language and resolution)
    - Presigned URLs untuk semua media
    """
    try:
        with ContentSearchCRUD() as crud:
            if cu:
                crud.set_context(user_id=cu.id, org_id=cu.org_id)
            
            content = crud.get_content_detail(content_id)
            
            if not content:
                raise HTTPException(status_code=404, detail="Content not found")
            
            return ApiResponse(
                status=0,
                message="Content found",
                data=content
            )
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get content: {str(e)}")

@router.get("/suggest/autocomplete", response_model=ApiResponse)
async def autocomplete(
    q: str = Query(..., min_length=1, description="Search query"),
    language: str = Query("id", description="Language (id, en)"),
    limit: int = Query(10, ge=1, le=20, description="Max suggestions"),
    cu: Optional[CurrentUser] = Depends(get_current_user_optional)
) -> ApiResponse:
    """
    Autocomplete untuk search box.
    
    Returns title suggestions based on user input.
    Supports multi-language (Indonesian & English).
    """
    try:
        with ContentSearchCRUD() as crud:
            if cu:
                crud.set_context(user_id=cu.id, org_id=cu.org_id)

            suggestions = crud.autocomplete_search(
                query=q,
                language=language,
                limit=limit
            )
            
            return ApiResponse(
                status=0,
                message="Suggestions generated",
                data={"suggestions": suggestions}
            )
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Autocomplete failed: {str(e)}")

@router.get("/discover/trending", response_model=ApiResponse)
async def get_trending(
    limit: int = Query(20, ge=1, le=50, description="Number of items"),
    cu: Optional[CurrentUser] = Depends(get_current_user_optional)
) -> ApiResponse:
    """
    Get trending contents berdasarkan views dan saved count.
    
    Perfect for homepage "Trending Now" section in mobile apps.
    """
    try:
        with ContentSearchCRUD() as crud:
            if cu:
                crud.set_context(user_id=cu.id, org_id=cu.org_id)
            
            items = crud.get_trending_contents(limit=limit)
            
            return ApiResponse(
                status=0,
                message="Trending contents loaded",
                data={"items": items}
            )
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get trending: {str(e)}")

@router.get("/filters/genres", response_model=ApiResponse)
async def get_available_genres(
    cu: Optional[CurrentUser] = Depends(get_current_user_optional)
) -> ApiResponse:
    """
    Get list semua genres yang tersedia.
    
    Useful for genre filter dropdown/chips in mobile apps.
    Returns genres sorted by sort field.
    """
    try:
        with ContentSearchCRUD() as crud:
            if cu:
                crud.set_context(user_id=cu.id, org_id=cu.org_id)

            genres = crud.get_available_genres()
            
            return ApiResponse(
                status=0,
                message="Genres loaded",
                data={"genres": genres}
            )
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get genres: {str(e)}")

@router.get("/filters/tags", response_model=ApiResponse)
async def get_popular_tags(
    limit: int = Query(50, ge=1, le=100, description="Max tags"),
    cu: Optional[CurrentUser] = Depends(get_current_user_optional)
) -> ApiResponse:
    """
    Get popular tags dengan count.
    
    Useful for tag cloud atau trending tags section.
    Sorted by popularity (doc count).
    """
    try:
        with ContentSearchCRUD() as crud:
            if cu:
                crud.set_context(user_id=cu.id, org_id=cu.org_id)

            tags = crud.get_popular_tags(limit=limit)
            
            return ApiResponse(
                status=0,
                message="Popular tags loaded",
                data={"tags": tags}
            )
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get tags: {str(e)}")

# ===== Admin Endpoints (Internal Use) =====

@router.post("/admin/setup", response_model=ApiResponse)
async def setup_search_index() -> ApiResponse:
    """
    Setup OpenSearch index dengan mapping.
    
    **Admin Only** - Call this once during initial deployment.
    """
    try:
        with ContentSearchCRUD() as crud:
            crud.setup_index()
            
            return ApiResponse(
                status=0,
                message="Search index created successfully"
            )
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Setup failed: {str(e)}")

@router.post("/admin/sync/{content_id}", response_model=ApiResponse)
async def sync_single_content(
    content_id: str,
    cu: Optional[CurrentUser] = Depends(get_current_user)
) -> ApiResponse:
    """
    Sync single content dari MongoDB ke OpenSearch.
    
    **Admin Only** - Use for manual sync or testing.
    """
    try:
        # check authority is not owner
        if cu.authority != Authority.OWNER.value:
            raise PermissionError("Access denied")
        
        with ContentSearchCRUD() as crud:
            success = crud.sync_single_content(content_id)
            
            if success:
                return ApiResponse(
                    status=0,
                    message=f"Content {content_id} synced successfully"
                )
            else:
                raise HTTPException(status_code=404, detail="Content not found in MongoDB")
                
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Sync failed: {str(e)}")

@router.post("/admin/sync/bulk", response_model=ApiResponse)
async def bulk_sync_contents(
    batch_size: int = Query(1000, ge=100, le=5000, description="Batch size"),
    cu: Optional[CurrentUser] = Depends(get_current_user)
) -> ApiResponse:
    """
    Bulk sync semua contents dari MongoDB ke OpenSearch.
    
    **Admin Only** - Use for initial data migration or full re-sync.
    
    Warning: This operation can take several minutes for large datasets.
    """
    try:
        # check authority is not owner
        if cu.authority != Authority.OWNER.value:
            raise PermissionError("Access denied")
        
        with ContentSearchCRUD() as crud:
            stats = crud.bulk_sync_contents(batch_size=batch_size)
            
            response_data = {
                "total_processed": stats['success'] + stats['failed'],
                "success_count": stats['success'],
                "failed_count": stats['failed'],
                "duration_seconds": round(stats['duration'], 2),
                "items_per_second": round(stats['success'] / stats['duration'], 2) if stats['duration'] > 0 else 0
            }
            
            return ApiResponse(
                status=0,
                message="Bulk sync completed",
                data=response_data
            )
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Bulk sync failed: {str(e)}")

@router.delete("/admin/sync/{content_id}", response_model=ApiResponse)
async def delete_content_from_search(
    content_id: str,
    cu: Optional[CurrentUser] = Depends(get_current_user)
) -> ApiResponse:
    """
    Delete content dari OpenSearch index.
    
    **Admin Only** - Use when content is deleted from MongoDB.
    """
    try:
        # check authority is not owner
        if cu.authority != Authority.OWNER.value:
            raise PermissionError("Access denied")
        
        with ContentSearchCRUD() as crud:
            success = crud.delete_content_from_opensearch(content_id)
            
            if success:
                return ApiResponse(
                    status=0,
                    message=f"Content {content_id} deleted from search index"
                )
            else:
                raise HTTPException(status_code=404, detail="Content not found in search index")
                
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")

@router.get("/admin/stats", response_model=ApiResponse)
async def get_sync_statistics(
    cu: Optional[CurrentUser] = Depends(get_current_user)
) -> ApiResponse:
    """
    Get statistics tentang sync status.
    
    **Admin Only** - Monitor sync health and identify discrepancies.
    
    Returns:
    - MongoDB document counts
    - OpenSearch document counts
    - Index size
    - Sync status
    """
    try:
        # check authority is not owner
        if cu.authority != Authority.OWNER.value:
            raise PermissionError("Access denied")
        
        with ContentSearchCRUD() as crud:
            # MongoDB stats
            mongo_total = crud.mongo.get_database()[crud.mongodb_collection].count_documents({})
            mongo_active = crud.mongo.get_database()[crud.mongodb_collection].count_documents(
                {"status": "published"}
            )
            
            # OpenSearch stats
            client = crud.opensearch.get_client()
            
            if not client.indices.exists(index=crud.opensearch_index):
                return ApiResponse(
                    status=1,
                    message="OpenSearch index does not exist. Run setup first.",
                    data=None
                )
            
            # Get document count
            count_response = crud.opensearch.search(
                body={"query": {"match_all": {}}, "size": 0}
            )
            os_total = count_response['hits']['total']['value']
            
            # Get index size
            index_stats = client.indices.stats(index=crud.opensearch_index)
            index_size_bytes = index_stats['indices'][crud.opensearch_index]['total']['store']['size_in_bytes']
            index_size_mb = round(index_size_bytes / (1024 * 1024), 2)
            
            # Calculate difference
            difference = mongo_active - os_total
            is_synced = difference == 0
            
            stats_data = {
                "mongodb_total": mongo_total,
                "mongodb_active": mongo_active,
                "opensearch_total": os_total,
                "opensearch_size_mb": index_size_mb,
                "is_synced": is_synced,
                "difference": difference
            }
            
            if is_synced:
                message = "✓ Fully synced"
            else:
                message = f"⚠ Out of sync ({abs(difference)} documents difference)"
            
            return ApiResponse(
                status=0,
                message=message,
                data=stats_data
            )
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get statistics: {str(e)}")