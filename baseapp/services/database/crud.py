import json,time
from pymongo.errors import PyMongoError
from minio.error import S3Error

from baseapp.config import setting, mongodb, minio
from baseapp.utils.logger import Logger

config = setting.get_settings()
logger = Logger("baseapp.services.database.crud")

class CRUD:
    def __init__(self):
        pass

    def create_db(self):
        """
        Create database and tables with schema.
        """
        try:
            with open(f"{config.file_location}mongodb.json") as json_file:
                initData = json.load(json_file)            
                with mongodb.MongoConn() as mongo_conn:
                    is_exists = mongo_conn.check_database_exists()
                    logger.debug(f"Database exist is {is_exists}")
                    if not is_exists:
                        mongo_conn.create_database(initData)
                    return is_exists
        except PyMongoError as pme:
            logger.error(f"Database error occurred: {str(pme)}")
            raise ValueError("Database error occurred while create database and tables.") from pme
        except Exception as e:
            logger.exception(f"Unexpected error occurred while creating document: {str(e)}")
            raise

    def create_bucket(self):
        """
        Create bucket.
        """
        start_time = time.perf_counter()
        bucket_name = config.minio_bucket
        try:
            with minio.MinioConn() as conn:
                if not conn.bucket_exists(bucket_name):
                    conn.create_bucket(bucket_name)
                    duration_ms = (time.perf_counter() - start_time) * 1000
                    logger.log_operation(
                        "bucket_creation",
                        "success",
                        bucket=bucket_name,
                        duration_ms=round(duration_ms, 2)
                    )
        except S3Error as s3e:
            logger.error(
                "Error while creating bucket",
                bucket=bucket_name,
                error_type="S3Error",
                error=str(s3e.message)
            )
            raise ValueError("Error while creating bucket") from s3e
        except Exception as e:
            logger.log_error_with_context(e, {
                "operation": "create_bucket_context_error",
                "host": config.minio_host,
                "port": config.minio_port,
                "bucket": bucket_name
            })
            raise