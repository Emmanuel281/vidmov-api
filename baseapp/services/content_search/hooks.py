"""
Hooks untuk auto-sync content ke OpenSearch menggunakan Redis Queue.
Tasks akan diproses asynchronously oleh worker.
"""

from typing import Dict, Any, Optional
from baseapp.config.redis import RedisConn
from baseapp.services.redis_queue import RedisQueueManager
from baseapp.utils.logger import Logger

logger = Logger("services.content.hooks")

class ContentSearchHooks:
    """
    Hooks untuk auto-sync content catalog ke OpenSearch via Redis Queue.
    Hook ini dipanggil di service layer setelah operasi CRUD MongoDB.
    Tasks akan diproses asynchronously oleh ContentSyncWorker.
    
    Menggunakan singleton pattern untuk efficient connection management.
    """
    
    _instance = None
    _redis_conn = None
    _queue_manager = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ContentSearchHooks, cls).__new__(cls)
        return cls._instance
    
    def __init__(self, queue_name: str = "content_sync_tasks"):
        # Hanya initialize sekali (singleton pattern)
        if not self._initialized:
            self.queue_name = queue_name
            self._initialize_connection()
            ContentSearchHooks._initialized = True
    
    def _initialize_connection(self):
        """
        Initialize Redis connection dan queue manager.
        Dipanggil sekali saat pertama kali hook digunakan.
        """
        try:
            if self._redis_conn is None:
                logger.info(f"Initializing Redis connection for content sync hooks")
                self._redis_conn = RedisConn()
                self._queue_manager = RedisQueueManager(
                    redis_conn=self._redis_conn,
                    queue_name=self.queue_name
                )
                logger.info(f"Redis connection initialized for queue: {self.queue_name}")
        except Exception as e:
            logger.error(f"Failed to initialize Redis connection: {e}", exc_info=True)
            # Don't raise - allow app to start even if Redis is temporarily unavailable
    
    def _ensure_connection(self) -> bool:
        """
        Ensure Redis connection is available.
        Retry connection jika belum ada atau terputus.
        
        Returns:
            bool: True jika connection tersedia, False jika gagal
        """
        if self._queue_manager is None:
            logger.warning("Queue manager not initialized, attempting to reconnect...")
            try:
                self._initialize_connection()
            except Exception as e:
                logger.error(f"Failed to reconnect to Redis: {e}")
                return False
        
        return self._queue_manager is not None
    
    def _enqueue_task(self, task_data: Dict[str, Any]) -> bool:
        """
        Helper method untuk enqueue task ke Redis.
        
        Returns:
            bool: True jika berhasil enqueue, False jika gagal
        """
        try:
            # Ensure connection available
            if not self._ensure_connection():
                logger.error("Redis connection not available, cannot enqueue task")
                return False
            
            success = self._queue_manager.enqueue_task(task_data)
            if success:
                logger.debug(f"Task enqueued: {task_data}")
                return True
            else:
                logger.warning(f"Failed to enqueue task: {task_data}")
                return False
                
        except Exception as e:
            logger.error(f"Error enqueuing task: {e}", exc_info=True)
            # Try to reinitialize connection for next attempt
            self._queue_manager = None
            return False
    
    def after_create(self, content_id: str, content_data: Dict[str, Any]):
        """
        Hook setelah create content baru di MongoDB.
        Enqueue sync task ke Redis queue.
        
        Args:
            content_id: ID content yang baru dibuat
            content_data: Data content (untuk logging)
        """
        try:
            title = content_data.get('title', {}).get('id', 'Unknown')
            logger.info(f"Enqueueing sync for new content {content_id}: {title}")
            
            task = {
                "action": "sync",
                "content_id": content_id
            }
            
            success = self._enqueue_task(task)
            
            if success:
                logger.info(f"Content {content_id} sync task enqueued successfully")
            else:
                logger.warning(f"Failed to enqueue sync task for content {content_id}")
                    
        except Exception as e:
            logger.error(f"Error in after_create hook for content {content_id}: {e}", exc_info=True)
            # Tidak raise exception agar create ke MongoDB tetap sukses
    
    def after_update(self, content_id: str, updated_fields: Dict[str, Any]):
        """
        Hook setelah update content di MongoDB.
        Enqueue sync task ke Redis queue.
        
        Args:
            content_id: ID content yang diupdate
            updated_fields: Fields yang diupdate (untuk logging)
        """
        try:
            logger.info(f"Enqueueing sync for updated content {content_id}")
            logger.debug(f"Updated fields: {list(updated_fields.keys())}")
            
            task = {
                "action": "sync",
                "content_id": content_id
            }
            
            success = self._enqueue_task(task)
            
            if success:
                logger.info(f"Content {content_id} update sync task enqueued successfully")
            else:
                logger.warning(f"Failed to enqueue update sync task for content {content_id}")
                    
        except Exception as e:
            logger.error(f"Error in after_update hook for content {content_id}: {e}", exc_info=True)
    
    def after_delete(self, content_id: str):
        """
        Hook setelah delete content dari MongoDB.
        Enqueue delete task ke Redis queue.
        
        Args:
            content_id: ID content yang dihapus
        """
        try:
            logger.info(f"Enqueueing delete for content {content_id}")
            
            task = {
                "action": "delete",
                "content_id": content_id
            }
            
            success = self._enqueue_task(task)
            
            if success:
                logger.info(f"Content {content_id} delete task enqueued successfully")
            else:
                logger.warning(f"Failed to enqueue delete task for content {content_id}")
                    
        except Exception as e:
            logger.error(f"Error in after_delete hook for content {content_id}: {e}", exc_info=True)
    
    def after_status_change(self, content_id: str, new_status: str):
        """
        Hook khusus untuk perubahan status content.
        
        Status penting karena mempengaruhi visibility di search:
        - published: Muncul di search results
        - draft/archived: Tidak muncul di search results
        
        Args:
            content_id: ID content
            new_status: Status baru (published, draft, archived)
        """
        try:
            logger.info(f"Enqueueing sync for content {content_id} status change to {new_status}")
            
            # For status changes, we always sync (even if status is deleted)
            # to ensure OpenSearch reflects the current state
            task = {
                "action": "sync",
                "content_id": content_id
            }
            
            success = self._enqueue_task(task)
            
            if success:
                logger.info(f"Content {content_id} status change sync task enqueued")
            else:
                logger.warning(f"Failed to enqueue status change sync for content {content_id}")
                    
        except Exception as e:
            logger.error(
                f"Error in after_status_change hook for content {content_id}: {e}", 
                exc_info=True
            )
    
    def after_media_update(self, content_id: str, media_type: str):
        """
        Hook setelah update media files (poster, fyp_1, fyp_2).
        
        Note: Media files disimpan di collection terpisah (_dmsfile),
        tapi metadata tetap perlu di-sync.
        
        Args:
            content_id: ID content
            media_type: Type media yang diupdate (poster, fyp_1, fyp_2)
        """
        try:
            logger.info(f"Enqueueing sync for content {content_id} media update: {media_type}")
            
            task = {
                "action": "sync",
                "content_id": content_id
            }
            
            success = self._enqueue_task(task)
            
            if success:
                logger.info(f"Content {content_id} media update sync task enqueued")
            else:
                logger.warning(f"Failed to enqueue media update sync for content {content_id}")
                    
        except Exception as e:
            logger.error(
                f"Error in after_media_update hook for content {content_id}: {e}", 
                exc_info=True
            )
    
    def trigger_bulk_sync(self, batch_size: int = 1000) -> bool:
        """
        Trigger bulk sync untuk semua content.
        Digunakan untuk initial sync atau full re-sync.
        
        Args:
            batch_size: Batch size untuk bulk operation
            
        Returns:
            bool: True jika task berhasil dienqueue
        """
        try:
            logger.info(f"Enqueueing bulk sync task (batch_size: {batch_size})")
            
            task = {
                "action": "bulk_sync",
                "batch_size": batch_size
            }
            
            success = self._enqueue_task(task)
            
            if success:
                logger.info("Bulk sync task enqueued successfully")
                return True
            else:
                logger.warning("Failed to enqueue bulk sync task")
                return False
                    
        except Exception as e:
            logger.error(f"Error triggering bulk sync: {e}", exc_info=True)
            return False
    
    @classmethod
    def close_connection(cls):
        """
        Close Redis connection.
        Dipanggil saat aplikasi shutdown (optional).
        """
        try:
            if cls._redis_conn is not None:
                logger.info("Closing Redis connection for content sync hooks")
                # RedisConn biasanya handle connection pooling
                # Tidak perlu explicit close kecuali ada method khusus
                cls._redis_conn = None
                cls._queue_manager = None
                cls._initialized = False
                logger.info("Redis connection closed")
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}")

# Singleton instance - akan di-initialize saat pertama kali digunakan
content_search_hooks = ContentSearchHooks()


# ===== Integration Example =====
"""
Integrate hooks dengan existing content CRUD di baseapp/services/content/crud.py:

from baseapp.services.content_search.hooks import content_search_hooks

class CRUD:
    # ... existing code ...
    
    def create(self, data: Content):
        # ... existing create logic ...
        result = collection.insert_one(obj)
        content_id = str(result.inserted_id)
        
        # ⭐ Trigger sync hook (non-blocking, async via Redis)
        content_search_hooks.after_create(content_id, obj)
        
        return ContentResponse(**obj)
    
    def update_by_id(self, content_id: str, data):
        # ... existing update logic ...
        obj = data.model_dump()
        update_content = collection.find_one_and_update(
            {"_id": content_id},
            {"$set": obj},
            return_document=True
        )
        
        if update_content:
            # ⭐ Trigger sync hook
            content_search_hooks.after_update(content_id, obj)
        
        return ContentResponse(**update_content)
    
    def delete_by_id(self, content_id: str):
        # ... existing delete logic ...
        result = collection.delete_one({"_id": content_id})
        
        if result.deleted_count > 0:
            # ⭐ Trigger delete hook
            content_search_hooks.after_delete(content_id)
        
        return result.deleted_count > 0

# Untuk status change di baseapp/services/content/api.py:

from baseapp.services.content_search.hooks import content_search_hooks

@router.put("/update_status/{content_id}")
async def update_status(content_id: str, req: ContentUpdateStatus, cu: CurrentUser):
    with CRUD() as _crud:
        # ... permission check & update logic ...
        response = _crud.update_by_id(content_id, req)
        
        # ⭐ Trigger status change hook (important!)
        content_search_hooks.after_status_change(content_id, req.status)
    
    return ApiResponse(status=0, message="Data updated", data=response)

# Untuk DMS file updates (poster, video)
# Di baseapp/services/dmsfile/crud.py atau sejenisnya:

from baseapp.services.content_search.hooks import content_search_hooks

def after_upload_media(refkey_id: str, refkey_name: str):
    '''
    Dipanggil setelah upload media file untuk content
    
    Args:
        refkey_id: content_id
        refkey_name: poster, fyp_1, atau fyp_2
    '''
    if refkey_name in ['poster', 'fyp_1', 'fyp_2']:
        content_search_hooks.after_media_update(refkey_id, refkey_name)

# Optional: Cleanup di main.py shutdown
from baseapp.services.content_search.hooks import ContentSearchHooks

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting application...")
    
    yield
    
    # Shutdown
    logger.info("Shutting down application...")
    ContentSearchHooks.close_connection()  # Optional cleanup
"""