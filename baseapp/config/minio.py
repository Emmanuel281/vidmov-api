import time
from minio import Minio
from minio.error import S3Error, InvalidResponseError
from baseapp.config import setting
from baseapp.utils.logger import Logger

config = setting.get_settings()
logger = Logger("baseapp.config.minio")

class MinioConn:
    def __init__(self, host=None, port=None, access_key=None, secret_key=None, secure=False, verify=False):
        self.host = host or config.minio_host
        self.port = port or config.minio_port
        self.access_key = access_key or config.minio_access_key
        self.secret_key = secret_key or config.minio_secret_key
        self.secure = secure or config.minio_secure
        self.verify = verify or config.minio_verify
        self._conn = None
        self._context_start_time = None

    def __enter__(self):
        try:
            self._context_start_time = time.perf_counter()
            # Inisialisasi koneksi Minio
            self._conn = Minio(
                endpoint=f"{self.host}:{self.port}",
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure,
                http_client=None if self.verify else False,
            )

            duration_ms = (time.perf_counter() - self._context_start_time) * 1000
            logger.log_operation(
                "Minio Connection Established",
                "success",
                duration_ms=round(duration_ms, 2),
                host=self.host,
                port=self.port
            )
            return self._conn
        except S3Error as e:
            logger.error(
                "MinIO S3Error",
                host=self.host,
                port=self.port,
                error=str(e.message),
                error_type="S3Error"
            )
            raise ConnectionError(f"Failed to connect to MinIO: {e.message}")
        except InvalidResponseError as e:
            logger.error(
                "MinIO Invalid Response",
                host=self.host,
                port=self.port,
                error=str(e),
                error_type="InvalidResponseError"
            )
            raise ConnectionError("MinIO returned an invalid response.")
        except Exception as e:
            logger.log_error_with_context(e, {
                "operation": "minio_initialize",
                "host": self.host,
                "port": self.port
            })
            raise

    def close(self):
        """
        Close the MinIO connection (if needed).
        """
        # MinIO client in the current library doesn't require explicit connection close,
        # but this method can be used for cleanup in future versions if required.
        logger.info(
            "MinIO connection closed.",
            host=self.host,
            port=self.port
        )
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()
        if exc_type:
            logger.exception(
                f"exc_type: {exc_type}, exc_value: {exc_value}, exc_traceback: {exc_traceback}",
                host=self.host,
                port=self.port
            )
            return False