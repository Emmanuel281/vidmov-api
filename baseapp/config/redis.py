import redis,time
from baseapp.config import setting
from baseapp.utils.logger import Logger

config = setting.get_settings()
logger = Logger("baseapp.config.redis")

class RedisConn:
    def __init__(self, host=None, port=None, max_connections=10, retry_on_timeout=True, socket_timeout=5):
        self.host = host or config.redis_host
        self.port = port or config.redis_port
        self.max_connections  = max_connections or config.redis_max_connections
        self.retry_on_timeout = retry_on_timeout
        self.socket_timeout = socket_timeout
        self.pool = None
        self._conn = None
        self._context_start_time = None

    def __enter__(self):
        self._context_start_time = time.perf_counter()
        try:
            self.pool = redis.ConnectionPool(
                host=self.host,
                port=self.port,
                max_connections=self.max_connections,
                decode_responses=True,
                retry_on_timeout=self.retry_on_timeout,
                socket_timeout=self.socket_timeout,
            )
            self._conn = redis.Redis(connection_pool=self.pool)
            # Validate connection
            self._conn.ping()
            
            duration_ms = (time.perf_counter() - self._context_start_time) * 1000
            logger.log_operation(
                "Redis Connection",
                "success",
                duration_ms=round(duration_ms, 2),
                host=self.host,
                port=self.port,
                max_connections=self.max_connections
            )
            return self._conn
        except redis.ConnectionError as e:
            logger.error(
                "Redis: Failed to connect",
                host=self.host,
                port=self.port,
                max_connections=self.max_connections,
                error=str(e),
                error_type="ConnectionError"
            )
            raise ConnectionError("Failed to initialize Redis Connection Pool") # Mengangkat kesalahan koneksi Redis
        except Exception as e:
            logger.log_error_with_context(e, {
                "operation": "redis_context_enter",
                "host": self.host,
                "port": self.port,
                "max_connections": self.max_connections
            })
            raise  # Mengangkat kesalahan lainnya

    def close(self):
        if self.pool:
            try:
                self.pool.disconnect()
                # logger.info("Redis Connection Pool closed.")
            except Exception as e:
                logger.error(f"Error while closing Redis Connection Pool: {e}")

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()
        duration_ms = None
        if self._context_start_time:
            duration_ms = (time.perf_counter() - self._context_start_time) * 1000
        if exc_type:
            logger.error(
                "Redis context error",
                host=self.host,
                port=self.port,
                max_connections=self.max_connections,
                duration_ms=round(duration_ms, 2) if duration_ms else None,
                error_type=exc_type.__name__,
                error=str(exc_value)
            )
            return False  # Membiarkan pengecualian diteruskan keluar dari blok 'with'
        else:
            # Success - hanya log jika duration signifikan (> 100ms)
            if duration_ms and duration_ms > 100:
                logger.debug(
                    "Redis context closed",
                    host=self.host,
                    port=self.port,
                    max_connections=self.max_connections,
                    duration_ms=round(duration_ms, 2)
                )