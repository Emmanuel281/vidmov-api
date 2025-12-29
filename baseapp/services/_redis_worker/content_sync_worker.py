"""
Worker untuk sync content ke OpenSearch menggunakan Redis Queue.
"""

from typing import Dict, Any
from pymongo.errors import PyMongoError

from baseapp.services._redis_worker.base_worker import BaseWorker
from baseapp.config import setting, mongodb, opensearch
from baseapp.utils.logger import Logger

config = setting.get_settings()
logger = Logger("baseapp.services._redis_worker.content_sync_worker")

class ContentSyncWorker(BaseWorker):
    """
    Worker untuk sync content ke OpenSearch.
    
    Task types:
    - sync: Sync single content (create/update)
    - delete: Delete content from OpenSearch
    - bulk_sync: Trigger bulk sync (admin operation)
    """
    
    def __init__(self, queue_manager, max_retries: int = 3):
        super().__init__(queue_manager, max_retries)
        self.mongodb_collection = "content"
        self.opensearch_index = "content_search"
    
    def process_task(self, data: dict):
        """
        Process sync task.
        
        Expected data structure:
        {
            "action": "sync" | "delete" | "bulk_sync",
            "content_id": "xxx",  # Required for sync/delete
            "batch_size": 1000    # Optional for bulk_sync
        }
        """
        logger.info(f"Processing content sync task: {data}")
        
        try:
            # Validate task data
            if not data.get("action"):
                logger.error(f"Invalid task data: missing 'action' field. Data: {data}")
                raise ValueError("Missing required field: 'action'")
            
            action = data.get("action")
            
            # Route to appropriate handler
            if action == "sync":
                return self._handle_sync(data)
            elif action == "delete":
                return self._handle_delete(data)
            elif action == "bulk_sync":
                return self._handle_bulk_sync(data)
            else:
                logger.error(f"Unknown action: {action}")
                raise ValueError(f"Unknown action: {action}")
                
        except ValueError as ve:
            # Error validasi data - log dan skip task ini
            logger.error(f"Validation error: {ve}")
            return False
        except Exception as e:
            logger.exception(f"Unexpected error processing task: {str(e)}")
            raise
    
    def _handle_sync(self, data: dict) -> bool:
        """
        Handle sync action (create/update content).
        """
        content_id = data.get("content_id")
        
        if not content_id:
            logger.error("Missing 'content_id' for sync action")
            raise ValueError("Missing required field: 'content_id'")
        
        try:
            # Initialize connections
            with mongodb.MongoConn() as mongo, opensearch.OpenSearchConn(self.opensearch_index) as os_conn:
                # Get content from MongoDB
                collection = mongo.get_database()[self.mongodb_collection]
                content = collection.find_one({"_id": content_id})
                
                if not content:
                    logger.warning(f"Content {content_id} not found in MongoDB")
                    return False
                
                # Transform to OpenSearch document
                os_doc = self._transform_to_opensearch_document(content)
                
                # Index to OpenSearch
                os_conn.index_document(
                    doc_id=content_id,
                    body=os_doc,
                    refresh=True
                )
                
                logger.info(f"Successfully synced content {content_id} to OpenSearch")
                return True
                
        except PyMongoError as pme:
            logger.error(f"MongoDB error syncing content {content_id}: {str(pme)}")
            raise ValueError("Database error while syncing content") from pme
        except Exception as e:
            logger.error(f"Error syncing content {content_id}: {str(e)}")
            raise
    
    def _handle_delete(self, data: dict) -> bool:
        """
        Handle delete action.
        """
        content_id = data.get("content_id")
        
        if not content_id:
            logger.error("Missing 'content_id' for delete action")
            raise ValueError("Missing required field: 'content_id'")
        
        try:
            with opensearch.OpenSearchConn(self.opensearch_index) as os_conn:
                result = os_conn.delete_document(doc_id=content_id)
                
                if result:
                    logger.info(f"Successfully deleted content {content_id} from OpenSearch")
                    return True
                else:
                    logger.warning(f"Content {content_id} not found in OpenSearch")
                    return False
                    
        except Exception as e:
            logger.error(f"Error deleting content {content_id}: {str(e)}")
            raise
    
    def _handle_bulk_sync(self, data: dict) -> bool:
        """
        Handle bulk sync action (sync all published contents).
        """
        batch_size = data.get("batch_size", 1000)
        
        try:
            with mongodb.MongoConn() as mongo, opensearch.OpenSearchConn(self.opensearch_index) as os_conn:
                collection = mongo.get_database()[self.mongodb_collection]
                
                # Count total documents
                total = collection.count_documents({})
                logger.info(f"Starting bulk sync for {total} contents (batch size: {batch_size})")
                
                # Process in batches
                cursor = collection.find({}).batch_size(batch_size)
                actions = []
                success_count = 0
                failed_count = 0
                
                for content in cursor:
                    try:
                        content_id = str(content.get('_id'))
                        os_doc = self._transform_to_opensearch_document(content)
                        
                        action = {
                            "_index": self.opensearch_index,
                            "_id": content_id,
                            "_source": os_doc
                        }
                        actions.append(action)
                        
                        # Bulk index every batch_size documents
                        if len(actions) >= batch_size:
                            success, failed = os_conn.bulk_index(actions)
                            success_count += success
                            failed_count += len(failed) if failed else 0
                            actions = []
                            
                    except Exception as e:
                        logger.error(f"Error transforming content {content.get('_id')}: {e}")
                        failed_count += 1
                
                # Index remaining documents
                if actions:
                    success, failed = os_conn.bulk_index(actions)
                    success_count += success
                    failed_count += len(failed) if failed else 0
                
                logger.info(
                    f"Bulk sync completed. Success: {success_count}, Failed: {failed_count}, "
                    f"Total: {total}"
                )
                
                return failed_count == 0
                
        except PyMongoError as pme:
            logger.error(f"MongoDB error during bulk sync: {str(pme)}")
            raise ValueError("Database error during bulk sync") from pme
        except Exception as e:
            logger.error(f"Error during bulk sync: {str(e)}")
            raise
    
    def _transform_to_opensearch_document(self, mongo_doc: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform MongoDB document ke OpenSearch document.
        
        Flattens multi-language fields dan creates search text.
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
        
        # Safely extract array fields
        def safe_list(value):
            """Convert value to list safely"""
            if value is None:
                return []
            if isinstance(value, list):
                return value
            if isinstance(value, str):
                return [value] if value else []
            return []
        
        cast_list = safe_list(mongo_doc.get('cast'))
        tags_list = safe_list(mongo_doc.get('tags'))
        genre_list = safe_list(mongo_doc.get('genre'))
        territory_list = safe_list(mongo_doc.get('territory'))

        # Build search text (kombinasi semua text untuk searching)
        search_parts = [
            title_all,
            synopsis_all,
            ' '.join(cast_list),
            ' '.join(tags_list),
            mongo_doc.get('origin', ''),
            sponsor_name or ''
        ]
        search_text = ' '.join(filter(None, search_parts))
        
        # Create flattened document
        doc = {
            "content_id": content_id,
            
            # Multi-language fields (flattened)
            "title_id": title_id,
            "title_en": title_en,
            "title_all": title_all,
            "synopsis_id": synopsis_id,
            "synopsis_en": synopsis_en,
            "synopsis_all": synopsis_all,
            
            # Arrays
            "genre": genre_list,
            "cast": cast_list,
            "tags": tags_list,
            "territory": territory_list,
            
            # Single values
            "release_date": mongo_doc.get('release_date'),
            "origin": mongo_doc.get('origin'),
            "rating": mongo_doc.get('rating', 0.0),
            "mature_content": mongo_doc.get('mature_content', False),
            "status": mongo_doc.get('status', 'draft'),
            
            # Stats
            "total_views": mongo_doc.get('total_views', 0),
            "total_saved": mongo_doc.get('total_saved', 0),
            "total_episodes": mongo_doc.get('total_episodes', 0),
            
            # Monetization
            "is_full_paid": mongo_doc.get('is_full_paid', False),
            "full_price_coins": mongo_doc.get('full_price_coins'),
            
            # Sponsor
            "sponsor_name": sponsor_name,
            "sponsor_campaign": sponsor_campaign,
            
            # License
            "license_from": mongo_doc.get('license_from'),
            "licence_date_start": mongo_doc.get('licence_date_start'),
            "licence_date_end": mongo_doc.get('licence_date_end'),
            
            # Metadata
            "org_id": mongo_doc.get('org_id', ''),
            "rec_date": mongo_doc.get('rec_date'),
            "mod_date": mongo_doc.get('mod_date'),
            
            # Search helper
            "search_text": search_text
        }
        
        return doc