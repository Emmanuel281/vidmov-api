import logging
logger = logging.getLogger("rabbit")

from baseapp.services._redis_worker.base_worker import BaseWorker
from pymongo.errors import PyMongoError
from baseapp.config import setting, minio, mongodb
config = setting.get_settings()

class DeleteFileWorker(BaseWorker):
    def __init__(self, queue_manager):
        super().__init__(queue_manager)
        self.minio_conn = minio.MinioConn()        
        self.collection_file = "_dmsfile"
        self.collection_organization = "_organization"

    def process_task(self, data: dict):
        """
        Process a task (e.g., send OTP).
        """
        logger.info(f"data task: {data} type data: {type(data)}")

        with mongodb.MongoConn() as mongo:
            collection = mongo.get_database()[self.collection_file]
            collection_org = mongo.get_database()[self.collection_organization]
            with self.minio_conn as conn:
                try:
                    minio_client = conn.get_minio_client()

                    # Apply filters
                    query_filter = {
                        "refkey_table": data.get("table"),
                        "refkey_id": data.get("id")
                    }
                    selected_fields = {
                        "id": "$_id",
                        "filename": 1,
                        "filestat": 1,
                        "folder_id": 1,
                        "folder_path": 1,
                        "metadata": 1,
                        "doctype": 1,
                        "refkey_id": 1,
                        "refkey_table": 1,
                        "refkey_name": 1,
                        "_id": 0
                    }

                    # Aggregation pipeline
                    pipeline = [
                        {"$match": query_filter},  # Filter stage
                        {"$project": selected_fields}  # Project only selected fields
                    ]

                    # Execute aggregation pipeline
                    cursor = collection.aggregate(pipeline)
                    results = list(cursor)

                    # looping data
                    for x in results:
                        # remove file in minio
                        minio_client.remove_object(config.minio_bucket, x['filename'])

                        # update space storage after deleted file
                        deleted_size = x['filestat']['size']
                        collection_org.update_one({"_id": data.get("org_id")}, {"$inc": {"usedstorage": -deleted_size}}, upsert=True)
                        
                        # delete file in mongodb
                        collection.delete_one({"_id": x['id']})
                    return len(results)
                except PyMongoError as pme:
                    logger.error(f"Error retrieving index with filters and pagination: {str(e)}")
                    raise ValueError("Database error while retrieve document") from pme
                except Exception as e:
                    logger.exception(f"Unexpected error during deletion: {str(e)}")
                    raise
