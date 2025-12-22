from baseapp.services._redis_worker.base_worker import BaseWorker
from pymongo.errors import PyMongoError
from minio.error import S3Error
from baseapp.config import setting, minio, mongodb
from baseapp.utils.logger import Logger
config = setting.get_settings()
logger = Logger("baseapp.services._redis_worker.delete_file_worker")

class DeleteFileWorker(BaseWorker):
    def __init__(self, queue_manager, max_retries: int = 3):
        super().__init__(queue_manager, max_retries)
        self.collection_file = "_dmsfile"
        self.collection_organization = "_organization"
    
    def process_task(self, data: dict):
        """
        Process a task (e.g., send OTP).
        """
        logger.info(f"data task: {data} type data: {type(data)}")
        try:
            if not data.get("table"):
                logger.error(f"Invalid task data: missing 'table' field. Data: {data}")
                raise ValueError("Missing required field: 'table'")
            
            if not data.get("id"):
                logger.error(f"Invalid task data: missing 'id' field. Data: {data}")
                raise ValueError("Missing required field: 'id'")
            
            if not data.get("org_id"):
                logger.error(f"Invalid task data: missing 'org_id' field. Data: {data}")
                raise ValueError("Missing required field: 'org_id'")
            
            with mongodb.MongoConn() as mongo:
                collection = mongo.get_database()[self.collection_file]
                collection_org = mongo.get_database()[self.collection_organization]
            with minio.MinioConn() as minio_client:
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

                if not results:
                    logger.info(f"No files found for table={data.get('table')}, id={data.get('id')}")
                    return 0

                # looping data
                deleted_count = 0
                for x in results:
                    try:
                        # remove file in minio
                        minio_client.remove_object(config.minio_bucket, x['filename'])
                        logger.debug(f"Deleted file from MinIO: {x['filename']}")

                        # update space storage after deleted file
                        deleted_size = x.get('filestat', {}).get('size', 0)
                        if deleted_size > 0:
                            collection_org.update_one(
                                {"_id": data.get("org_id")}, 
                                {"$inc": {"usedstorage": -deleted_size}}, 
                                upsert=True
                            )
                        
                        # delete file in mongodb
                        collection.delete_one({"_id": x['id']})
                        deleted_count += 1
                        logger.debug(f"Deleted file from MongoDB: {x['id']}")
                    except S3Error as s3e:
                        # Log error tapi lanjutkan proses file lain
                        logger.error(f"Error deleting file {x['filename']} from MinIO: {str(s3e)}")
                        # Jika file tidak ada di MinIO, tetap hapus dari MongoDB
                        if "NoSuchKey" in str(s3e) or "Not Found" in str(s3e):
                            logger.warning(f"File {x['filename']} not found in MinIO, deleting from MongoDB anyway")
                            # update space storage after deleted file
                            deleted_size = x.get('filestat', {}).get('size', 0)
                            if deleted_size > 0:
                                collection_org.update_one(
                                    {"_id": data.get("org_id")}, 
                                    {"$inc": {"usedstorage": -deleted_size}}, 
                                    upsert=True
                                )
                            collection.delete_one({"_id": x['id']})
                            deleted_count += 1
                        else:
                            # Error serius dari MinIO
                            raise
                logger.info(f"Successfully deleted {deleted_count}/{len(results)} files for table={data.get('table')}, id={data.get('id')}",)
                return deleted_count
        except ValueError as ve:
            # Error validasi data - ini bukan error fatal, log dan skip task ini
            logger.error(f"Validation error: {ve}")
            # Tidak raise agar tidak dihitung sebagai consecutive error
            return 0
        except PyMongoError as pme:
            logger.error(f"Error retrieving index with filters and pagination: {str(pme)}")
            raise ValueError("Database error while retrieve document") from pme
        except S3Error  as s3e:
            logger.error(f"Error uploading file: {str(s3e)}")
            raise ValueError("Error uploading file.") from s3e
        except Exception as e:
            logger.exception(f"Unexpected error during deletion: {str(e)}")
            raise
