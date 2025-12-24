from typing import Dict, List, Optional, Any
from datetime import datetime
import time

from baseapp.config import mongodb, opensearch, minio, setting
from baseapp.model.common import ContentStatus
from baseapp.utils.logger import Logger
from baseapp.services.content_search.model import (
    ContentOpenSearchDocument,
    ContentSearchItem,
    ContentDetailSearchResponse,
    MediaFile,
    GenreDetail,
    BrandPlacementResponse
)

config = setting.get_settings()
logger = Logger("services.content_search.crud")

# OpenSearch Index Mapping
CONTENT_INDEX_MAPPING = {
    "settings": {
        "number_of_shards": 2,
        "number_of_replicas": 1,
        "analysis": {
            "analyzer": {
                "multilang_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "asciifolding"]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "content_id": {"type": "keyword"},
            
            # Multi-language title
            "title_id": {
                "type": "text",
                "analyzer": "multilang_analyzer",
                "fields": {
                    "keyword": {"type": "keyword"},
                    "suggest": {"type": "completion"}
                }
            },
            "title_en": {
                "type": "text",
                "analyzer": "multilang_analyzer"
            },
            "title_all": {
                "type": "text",
                "analyzer": "multilang_analyzer"
            },
            
            # Multi-language synopsis
            "synopsis_id": {"type": "text", "analyzer": "multilang_analyzer"},
            "synopsis_en": {"type": "text", "analyzer": "multilang_analyzer"},
            "synopsis_all": {"type": "text", "analyzer": "multilang_analyzer"},
            
            # Arrays
            "genre": {"type": "keyword"},
            "cast": {"type": "keyword"},
            "tags": {"type": "keyword"},
            "territory": {"type": "keyword"},
            
            # Single values
            "release_date": {"type": "date"},
            "origin": {"type": "keyword"},
            "rating": {"type": "float"},
            "mature_content": {"type": "boolean"},
            "status": {"type": "keyword"},
            
            # Stats
            "total_views": {"type": "long"},
            "total_saved": {"type": "long"},
            "total_episodes": {"type": "integer"},
            
            # Monetization
            "is_full_paid": {"type": "boolean"},
            "full_price_coins": {"type": "integer"},
            
            # Sponsor
            "sponsor_name": {"type": "text"},
            "sponsor_campaign": {"type": "text"},
            
            # License
            "license_from": {"type": "text"},
            "licence_date_start": {"type": "date"},
            "licence_date_end": {"type": "date"},
            
            # Metadata
            "org_id": {"type": "keyword"},
            "rec_date": {"type": "date"},
            "mod_date": {"type": "date"},
            
            # Search helper
            "search_text": {"type": "text", "analyzer": "multilang_analyzer"}
        }
    }
}

class ContentSearchCRUD:
    
    def __init__(self, opensearch_index="content_search"):
        self.opensearch_index = opensearch_index
        self.mongodb_collection = "content"
        self.user_id = None
        self.org_id = None
    
    def __enter__(self):
        self._mongo_context = mongodb.MongoConn()
        self.mongo = self._mongo_context.__enter__()
        
        self._opensearch_context = opensearch.OpenSearchConn(self.opensearch_index)
        self.opensearch = self._opensearch_context.__enter__()
        
        self._minio_context = minio.MinioConn()
        self.minio = self._minio_context.__enter__()
        
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        if hasattr(self, '_mongo_context'):
            self._mongo_context.__exit__(exc_type, exc_value, traceback)
        if hasattr(self, '_opensearch_context'):
            self._opensearch_context.__exit__(exc_type, exc_value, traceback)
        if hasattr(self, '_minio_context'):
            self._minio_context.__exit__(exc_type, exc_value, traceback)
        return False
    
    def set_context(self, user_id: Optional[str] = None, org_id: Optional[str] = None):
        """Set user context for operations"""
        self.user_id = user_id
        self.org_id = org_id
    
    # ===== Index Management =====
    
    def setup_index(self):
        """Setup OpenSearch index dengan mapping"""
        try:
            self.opensearch.create_index(body=CONTENT_INDEX_MAPPING)
            logger.info(f"Index '{self.opensearch_index}' created successfully")
        except Exception as e:
            logger.error(f"Failed to create index: {str(e)}")
            raise
    
    # ===== Document Transformation =====
    
    def transform_to_opensearch_document(self, mongo_doc: Dict) -> ContentOpenSearchDocument:
        """
        Transform MongoDB document ke OpenSearch document
        """
        content_id = str(mongo_doc.get('_id'))
        
        # Extract multi-language fields
        title = mongo_doc.get('title', {})
        synopsis = mongo_doc.get('synopsis', {})
        
        title_id = title.get('id', '')
        title_en = title.get('en', '')
        title_all = ' '.join([v for v in title.values() if v])
        
        synopsis_id = synopsis.get('id', '')
        synopsis_en = synopsis.get('en', '')
        synopsis_all = ' '.join([v for v in synopsis.values() if v])
        
        # Extract sponsor info
        main_sponsor = mongo_doc.get('main_sponsor')
        sponsor_name = main_sponsor.get('brand_name') if main_sponsor else None
        sponsor_campaign = main_sponsor.get('campaign_name') if main_sponsor else None
        
        # Build search text (kombinasi semua text untuk searching)
        search_parts = [
            title_all,
            synopsis_all,
            ' '.join(mongo_doc.get('cast', [])),
            ' '.join(mongo_doc.get('tags', [])),
            mongo_doc.get('origin', ''),
            sponsor_name or ''
        ]
        search_text = ' '.join(filter(None, search_parts))
        
        doc = ContentOpenSearchDocument(
            content_id=content_id,
            title_id=title_id,
            title_en=title_en,
            title_all=title_all,
            synopsis_id=synopsis_id,
            synopsis_en=synopsis_en,
            synopsis_all=synopsis_all,
            genre=mongo_doc.get('genre', []),
            cast=mongo_doc.get('cast', []),
            tags=mongo_doc.get('tags', []),
            territory=mongo_doc.get('territory', []),
            release_date=mongo_doc.get('release_date'),
            origin=mongo_doc.get('origin'),
            rating=mongo_doc.get('rating', 0.0),
            mature_content=mongo_doc.get('mature_content', False),
            status=mongo_doc.get('status', 'draft'),
            total_views=mongo_doc.get('total_views', 0),
            total_saved=mongo_doc.get('total_saved', 0),
            total_episodes=mongo_doc.get('total_episodes', 0),
            is_full_paid=mongo_doc.get('is_full_paid', False),
            full_price_coins=mongo_doc.get('full_price_coins'),
            sponsor_name=sponsor_name,
            sponsor_campaign=sponsor_campaign,
            license_from=mongo_doc.get('license_from'),
            licence_date_start=mongo_doc.get('licence_date_start'),
            licence_date_end=mongo_doc.get('licence_date_end'),
            org_id=mongo_doc.get('org_id', ''),
            rec_date=mongo_doc.get('rec_date'),
            mod_date=mongo_doc.get('mod_date'),
            search_text=search_text
        )
        
        return doc
    
    def enrich_content_with_media(self, content: Dict, include_videos: bool = False) -> Dict:
        """
        Enrich content dengan data media dari MongoDB dan generate presigned URLs
        """
        content_id = content.get('content_id') or content.get('id')
        
        # Get genre details
        if 'genre' in content and content['genre']:
            genre_details = list(
                self.mongo.get_database()['_enum'].find(
                    {"_id": {"$in": content['genre']}},
                    {"_id": 1, "value": 1, "sort": 1}
                )
            )
            content['genre_details'] = [
                GenreDetail(id=str(g['_id']), value=g['value'], sort=g.get('sort'))
                for g in genre_details
            ]
        
        # Get poster files
        poster_files = list(
            self.mongo.get_database()['_dmsfile'].find(
                {
                    "refkey_id": content_id,
                    "doctype": "64c1c7ba4a5246648bf224bfd19fe118"  # Poster doctype
                },
                {
                    "_id": 1,
                    "filename": 1,
                    "metadata": 1,
                    "folder_path": 1,
                    "filestat": 1
                }
            )
        )
        
        # Group poster by language
        grouped_poster = {}
        for poster in poster_files:
            lang_key = "other"
            if poster.get("metadata") and "Language" in poster["metadata"]:
                lang_key = poster["metadata"]["Language"].lower()
            
            url = self.minio.presigned_get_object(config.minio_bucket, poster['filename'])
            
            grouped_poster[lang_key] = MediaFile(
                id=str(poster['_id']),
                filename=poster['filename'],
                url=url,
                path=poster.get('folder_path'),
                info_file=poster.get('filestat')
            )
        
        content['poster'] = grouped_poster
        
        # Get video files if requested
        if include_videos:
            # FYP #1 videos
            fyp1_files = list(
                self.mongo.get_database()['_dmsfile'].find(
                    {
                        "refkey_id": content_id,
                        "doctype": "31c557f0f4574f7aae55c1b6860a2e19"
                    },
                    {
                        "_id": 1,
                        "filename": 1,
                        "metadata": 1,
                        "folder_path": 1,
                        "filestat": 1
                    }
                )
            )
            
            grouped_fyp1 = {}
            for video in fyp1_files:
                lang_key = "other"
                res_key = "original"
                
                if video.get("metadata"):
                    if "Language" in video["metadata"]:
                        lang_key = video["metadata"]["Language"].lower()
                    if "Resolution" in video["metadata"]:
                        res_key = video["metadata"]["Resolution"].lower()
                
                url = self.minio.presigned_get_object(config.minio_bucket, video['filename'])
                
                if lang_key not in grouped_fyp1:
                    grouped_fyp1[lang_key] = {}
                
                grouped_fyp1[lang_key][res_key] = MediaFile(
                    id=str(video['_id']),
                    filename=video['filename'],
                    url=url,
                    path=video.get('folder_path'),
                    info_file=video.get('filestat')
                )
            
            content['fyp_1'] = grouped_fyp1
            
            # FYP #2 videos (similar logic)
            fyp2_files = list(
                self.mongo.get_database()['_dmsfile'].find(
                    {
                        "refkey_id": content_id,
                        "doctype": "8014149170ad41148f5ae01d9b0aac7b"
                    },
                    {
                        "_id": 1,
                        "filename": 1,
                        "metadata": 1,
                        "folder_path": 1,
                        "filestat": 1
                    }
                )
            )
            
            grouped_fyp2 = {}
            for video in fyp2_files:
                lang_key = "other"
                res_key = "original"
                
                if video.get("metadata"):
                    if "Language" in video["metadata"]:
                        lang_key = video["metadata"]["Language"].lower()
                    if "Resolution" in video["metadata"]:
                        res_key = video["metadata"]["Resolution"].lower()
                
                url = self.minio.presigned_get_object(config.minio_bucket, video['filename'])
                
                if lang_key not in grouped_fyp2:
                    grouped_fyp2[lang_key] = {}
                
                grouped_fyp2[lang_key][res_key] = MediaFile(
                    id=str(video['_id']),
                    filename=video['filename'],
                    url=url,
                    path=video.get('folder_path'),
                    info_file=video.get('filestat')
                )
            
            content['fyp_2'] = grouped_fyp2
        
        return content
    
    # ===== Sync Operations =====
    
    def sync_single_content(self, content_id: str) -> bool:
        """
        Sync single content dari MongoDB ke OpenSearch
        """
        try:
            # Get content from MongoDB
            content = self.mongo.get_database()[self.mongodb_collection].find_one(
                {"_id": content_id}
            )
            
            if not content:
                logger.warning(f"Content {content_id} not found in MongoDB")
                return False
            
            # Transform to OpenSearch document
            os_doc = self.transform_to_opensearch_document(content)
            
            # Index to OpenSearch
            self.opensearch.index_document(
                doc_id=content_id,
                body=os_doc.model_dump(),
                refresh=True
            )
            
            logger.info(f"Content {content_id} synced to OpenSearch")
            return True
            
        except Exception as e:
            logger.log_error_with_context(e, {
                "operation": "sync_single_content",
                "content_id": content_id
            })
            return False
    
    def bulk_sync_contents(self, batch_size: int = 1000) -> Dict[str, Any]:
        """
        Bulk sync semua contents dari MongoDB ke OpenSearch
        """
        stats = {"success": 0, "failed": 0, "duration": 0}
        start_time = time.time()
        
        try:
            collection = self.mongo.get_database()[self.mongodb_collection]
            total = collection.count_documents({})
            
            logger.info(f"Starting bulk sync for {total} contents")
            
            cursor = collection.find({}).batch_size(batch_size)
            actions = []
            
            for content in cursor:
                try:
                    content_id = str(content.get('_id'))
                    os_doc = self.transform_to_opensearch_document(content)
                    
                    action = {
                        "_index": self.opensearch_index,
                        "_id": content_id,
                        "_source": os_doc.model_dump()
                    }
                    actions.append(action)
                    
                    # Bulk index setiap batch_size dokumen
                    if len(actions) >= batch_size:
                        success, failed = self.opensearch.bulk_index(actions)
                        stats['success'] += success
                        stats['failed'] += len(failed) if failed else 0
                        actions = []
                        
                except Exception as e:
                    logger.error(f"Error transforming content {content.get('_id')}: {e}")
                    stats['failed'] += 1
            
            # Index sisa dokumen
            if actions:
                success, failed = self.opensearch.bulk_index(actions)
                stats['success'] += success
                stats['failed'] += len(failed) if failed else 0
            
            stats['duration'] = time.time() - start_time
            
            logger.log_operation(
                "bulk_sync_contents",
                "completed",
                total=total,
                success=stats['success'],
                failed=stats['failed'],
                duration=stats['duration']
            )
            
        except Exception as e:
            logger.log_error_with_context(e, {
                "operation": "bulk_sync_contents"
            })
            raise
        
        return stats
    
    def delete_content_from_opensearch(self, content_id: str) -> bool:
        """
        Delete content dari OpenSearch
        """
        try:
            result = self.opensearch.delete_document(doc_id=content_id)
            logger.info(f"Content {content_id} deleted from OpenSearch")
            return result is not None
        except Exception as e:
            logger.log_error_with_context(e, {
                "operation": "delete_content_from_opensearch",
                "content_id": content_id
            })
            return False
    
    # ===== Search Operations =====
    
    def search_contents(
        self,
        query: Optional[str] = None,
        genres: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
        cast: Optional[str] = None,
        origin: Optional[str] = None,
        territory: Optional[str] = None,
        min_rating: Optional[float] = None,
        max_rating: Optional[float] = None,
        mature_content: Optional[bool] = None,
        language: str = "id",
        sort_by: str = "relevance",
        page: int = 1,
        page_size: int = 20
    ) -> Dict[str, Any]:
        """
        Search contents dengan berbagai filter
        """
        try:
            # Build query
            must_queries = []
            filter_queries = []
            
            # Text search (multi-language aware)
            if query:
                should_queries = [
                    {
                        "multi_match": {
                            "query": query,
                            "fields": [
                                f"title_{language}^3",
                                "title_all^2",
                                f"synopsis_{language}^2",
                                "synopsis_all",
                                "cast",
                                "tags",
                                "search_text"
                            ],
                            "type": "best_fields",
                            "operator": "or"
                        }
                    }
                ]
                must_queries.append({"bool": {"should": should_queries}})
            
            # Filter by status (only published)
            filter_queries.append({"term": {"status": ContentStatus.PUBLISHED.value}})
            
            # Genre filter
            if genres:
                filter_queries.append({"terms": {"genre": genres}})
            
            # Tags filter
            if tags:
                filter_queries.append({"terms": {"tags": tags}})
            
            # Cast filter
            if cast:
                filter_queries.append({"term": {"cast": cast}})
            
            # Origin filter
            if origin:
                filter_queries.append({"term": {"origin": origin}})
            
            # Territory filter
            if territory:
                filter_queries.append({"term": {"territory": territory}})
            
            # Rating filter
            if min_rating is not None or max_rating is not None:
                rating_filter = {"range": {"rating": {}}}
                if min_rating is not None:
                    rating_filter["range"]["rating"]["gte"] = min_rating
                if max_rating is not None:
                    rating_filter["range"]["rating"]["lte"] = max_rating
                filter_queries.append(rating_filter)
            
            # Mature content filter
            if mature_content is not None:
                filter_queries.append({"term": {"mature_content": mature_content}})
            
            # Build complete query
            search_body = {
                "query": {
                    "bool": {
                        "must": must_queries if must_queries else [{"match_all": {}}],
                        "filter": filter_queries
                    }
                },
                "from": (page - 1) * page_size,
                "size": page_size
            }
            
            # Sorting
            if sort_by == "rating":
                search_body["sort"] = [{"rating": "desc"}, "_score"]
            elif sort_by == "views":
                search_body["sort"] = [{"total_views": "desc"}, "_score"]
            elif sort_by == "release_date":
                search_body["sort"] = [{"release_date": "desc"}, "_score"]
            # default: relevance (menggunakan _score)
            
            # Execute search
            response = self.opensearch.search(body=search_body)
            
            # Parse results
            hits = response.get('hits', {})
            total = hits.get('total', {}).get('value', 0)
            
            items = []
            for hit in hits.get('hits', []):
                source = hit['_source']
                
                # Reconstruct title and synopsis as dict
                source['title'] = {
                    'id': source.pop('title_id', ''),
                    'en': source.pop('title_en', '')
                }
                source['synopsis'] = {
                    'id': source.pop('synopsis_id', ''),
                    'en': source.pop('synopsis_en', '')
                }
                
                # Remove search helper fields
                source.pop('title_all', None)
                source.pop('synopsis_all', None)
                source.pop('search_text', None)
                
                # Rename content_id to id
                source['id'] = source.pop('content_id')
                
                # Enrich with media
                enriched = self.enrich_content_with_media(source, include_videos=True)
                
                items.append(enriched)
            
            return {
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": (total + page_size - 1) // page_size,
                "items": items
            }
            
        except Exception as e:
            logger.log_error_with_context(e, {
                "operation": "search_contents",
                "query": query
            })
            raise
    
    def get_content_detail(self, content_id: str) -> Optional[Dict]:
        """
        Get content detail dari OpenSearch dan enrich dengan media
        """
        try:
            # Get from OpenSearch
            response = self.opensearch.search(
                body={
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"content_id": content_id}},
                                {"term": {"status": ContentStatus.PUBLISHED.value}}
                            ]
                        }
                    },
                    "size": 1
                }
            )
            
            hits = response.get('hits', {}).get('hits', [])
            if not hits:
                return None
            
            source = hits[0]['_source']
            
            # Reconstruct title and synopsis
            source['title'] = {
                'id': source.pop('title_id', ''),
                'en': source.pop('title_en', '')
            }
            source['synopsis'] = {
                'id': source.pop('synopsis_id', ''),
                'en': source.pop('synopsis_en', '')
            }
            
            source.pop('title_all', None)
            source.pop('synopsis_all', None)
            source.pop('search_text', None)
            source['id'] = source.pop('content_id')
            
            # Enrich with all media including videos
            enriched = self.enrich_content_with_media(source, include_videos=True)
            
            return enriched
            
        except Exception as e:
            logger.log_error_with_context(e, {
                "operation": "get_content_detail",
                "content_id": content_id
            })
            return None
    
    def autocomplete_search(self, query: str, language: str = "id", limit: int = 10) -> List[str]:
        """
        Autocomplete untuk search box
        """
        try:
            field = f"title_{language}.suggest"
            
            search_body = {
                "suggest": {
                    "title-suggest": {
                        "prefix": query,
                        "completion": {
                            "field": field,
                            "size": limit,
                            "skip_duplicates": True
                        }
                    }
                }
            }
            
            response = self.opensearch.search(body=search_body)
            
            suggestions = []
            for option in response.get('suggest', {}).get('title-suggest', [{}])[0].get('options', []):
                suggestions.append(option['text'])
            
            return suggestions
            
        except Exception as e:
            logger.log_error_with_context(e, {
                "operation": "autocomplete_search",
                "query": query
            })
            return []
    
    def get_trending_contents(self, limit: int = 20) -> List[Dict]:
        """
        Get trending contents berdasarkan views
        """
        try:
            search_body = {
                "query": {
                    "bool": {
                        "filter": [
                            {"term": {"status": ContentStatus.PUBLISHED.value}},
                            {"range": {"total_views": {"gt": 0}}}
                        ]
                    }
                },
                "sort": [
                    {"total_views": "desc"},
                    {"total_saved": "desc"}
                ],
                "size": limit
            }
            
            response = self.opensearch.search(body=search_body)
            
            items = []
            for hit in response.get('hits', {}).get('hits', []):
                source = hit['_source']
                
                source['title'] = {
                    'id': source.pop('title_id', ''),
                    'en': source.pop('title_en', '')
                }
                source['synopsis'] = {
                    'id': source.pop('synopsis_id', ''),
                    'en': source.pop('synopsis_en', '')
                }
                
                source.pop('title_all', None)
                source.pop('synopsis_all', None)
                source.pop('search_text', None)
                source['id'] = source.pop('content_id')
                
                enriched = self.enrich_content_with_media(source, include_videos=False)
                items.append(enriched)
            
            return items
            
        except Exception as e:
            logger.log_error_with_context(e, {
                "operation": "get_trending_contents"
            })
            return []
    
    def get_available_genres(self) -> List[Dict]:
        """Get list semua genres dari OpenSearch aggregation"""
        try:
            search_body = {
                "size": 0,
                "aggs": {
                    "genres": {
                        "terms": {
                            "field": "genre",
                            "size": 100
                        }
                    }
                }
            }
            
            response = self.opensearch.search(body=search_body)
            genre_ids = [
                bucket['key']
                for bucket in response['aggregations']['genres']['buckets']
            ]
            
            # Get genre details from MongoDB
            genre_details = list(
                self.mongo.get_database()['_enum'].find(
                    {"_id": {"$in": genre_ids}},
                    {"_id": 1, "value": 1, "sort": 1}
                ).sort("sort", 1)
            )
            
            return [
                {
                    "id": str(g['_id']),
                    "value": g['value'],
                    "sort": g.get('sort')
                }
                for g in genre_details
            ]
            
        except Exception as e:
            logger.log_error_with_context(e, {
                "operation": "get_available_genres"
            })
            return []
    
    def get_popular_tags(self, limit: int = 50) -> List[Dict]:
        """Get popular tags dari aggregation"""
        try:
            search_body = {
                "size": 0,
                "aggs": {
                    "tags": {
                        "terms": {
                            "field": "tags",
                            "size": limit
                        }
                    }
                }
            }
            
            response = self.opensearch.search(body=search_body)
            
            return [
                {
                    "tag": bucket['key'],
                    "count": bucket['doc_count']
                }
                for bucket in response['aggregations']['tags']['buckets']
            ]
            
        except Exception as e:
            logger.log_error_with_context(e, {
                "operation": "get_popular_tags"
            })
            return []