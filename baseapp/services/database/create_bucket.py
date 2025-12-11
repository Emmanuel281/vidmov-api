import time
import logging.config
from minio.error import S3Error

from baseapp.config import setting, minio
from baseapp.utils.logger import Logger
from baseapp.config.logging import get_logging_config

config = setting.get_settings()
logging.config.dictConfig(get_logging_config())
logger = Logger("baseapp.services.database.create_bucket")

def create_bucket():
    """
    Create bucket.
    """
    start_time = time.perf_counter()
    bucket_name = config.minio_bucket
    try:
        with minio.MinioConn() as conn:
            if not conn.bucket_exists(bucket_name):
                conn.make_bucket(bucket_name)
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

if __name__ == "__main__":
    # 1. Opsional: Print info bahwa script berjalan
    logger.info("Starting manual bucket creation script...")
    
    # 2. Jalankan fungsi
    try:
        create_bucket()
        logger.info("Bucket creation script finished successfully.")
    except Exception as e:
        logger.critical(f"Script failed: {e}")