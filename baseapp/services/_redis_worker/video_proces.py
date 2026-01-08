from time import sleep
from baseapp.services._redis_worker.base_worker import BaseWorker
from pymongo.errors import PyMongoError
from minio.error import S3Error
from baseapp.config import setting, minio, mongodb
from baseapp.utils.logger import Logger
config = setting.get_settings()
logger = Logger("baseapp.services._redis_worker.video_proces")

class VideoWorker(BaseWorker):
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
            sleep(10)
            logger.info(f"video_process queue recieve successfully to {data.get('email')}")
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
