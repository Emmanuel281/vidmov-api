import logging
from minio import Minio
from minio.error import S3Error, InvalidResponseError
from baseapp.config import setting

logger = logging.getLogger()

class MinioConn:
    def __init__(self, host=None, port=None, access_key=None, secret_key=None, secure=False, bucket="baseapp", verify=False):
        config = setting.get_settings()
        self.host = host or config.minio_host
        self.port = port or config.minio_port
        self.access_key = access_key or config.minio_access_key
        self.secret_key = secret_key or config.minio_secret_key
        self.secure = secure or config.minio_secure
        self.bucket = bucket or config.minio_bucket
        self.verify = verify or config.minio_verify
        self._conn = None

    def __enter__(self):
        try:
            # Inisialisasi koneksi Minio
            self._conn = Minio(
                endpoint=f"{self.host}:{self.port}",
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure,
                http_client=None if self.verify else False,
            )
            logger.info(f"Connected to MinIO at {self.host}:{self.port}")
            return self
        except S3Error as e:
            logger.error(f"MinIO S3Error: {e.message}")
            raise ConnectionError(f"Failed to connect to MinIO: {e.message}")
        except InvalidResponseError as e:
            logger.error(f"Invalid response from MinIO: {e}")
            raise ConnectionError("MinIO returned an invalid response.")
        except Exception as e:
            logger.exception(f"Unexpected error while connecting to MinIO: {e}")
            raise
    
    def get_minio_client(self):
        """
        Returns the MinIO client instance.
        """
        if not self._conn:
            logger.error("MinIO is not connected.")
            raise ConnectionError("MinIO is not connected.")
        return self._conn

    def bucket_exists(self):
        """
        Check if the default bucket exists.
        """
        if self._conn.bucket_exists(self.bucket):
            logger.info(f"Bucket '{self.bucket}' exists.")
            return True
        else:
            logger.warning(f"Bucket '{self.bucket}' does not exist.")
            return False

    def create_bucket(self):
        """
        Create the default bucket if it doesn't exist.
        """
        try:
            if not self._conn.bucket_exists(self.bucket):
                self._conn.make_bucket(self.bucket)
                logger.info(f"Bucket '{self.bucket}' created.")
                return True
            else:
                logger.info(f"Bucket '{self.bucket}' already exists.")
                return False
        except S3Error as e:
            logger.error(f"Error while creating bucket '{self.bucket}': {e.message}")
            raise ValueError("Error while creating bucket")
        except Exception as e:
            logger.exception(f"Unexpected error while creating bucket '{self.bucket}': {e}")
            raise

    def close(self):
        """
        Close the MinIO connection (if needed).
        """
        # MinIO client in the current library doesn't require explicit connection close,
        # but this method can be used for cleanup in future versions if required.
        logger.info("MinIO connection closed.")

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()
        if exc_type:
            logger.exception(f"mod: Minio.__exit__, exc_type: {exc_type}, exc_value: {exc_value}, exc_traceback: {exc_traceback}")
            return False

    def get_minio_endpoint(self):
        """
        Get MinIO endpoint from settings
        """
        return f"{self.host}:{self.port}"