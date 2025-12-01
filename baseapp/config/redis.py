import redis,logging
from baseapp.config import setting

config = setting.get_settings()
logger = logging.getLogger()

class RedisConn:
    def __init__(self, host=None, port=None, max_connections=10, retry_on_timeout=True, socket_timeout=5):
        self.host = host or config.redis_host
        self.port = port or config.redis_port
        self.max_connections  = max_connections or config.redis_max_connections
        self.retry_on_timeout = retry_on_timeout
        self.socket_timeout = socket_timeout
        self.pool = None
        self._conn = None

    def __enter__(self):
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
            # logger.info("Redis Connection Pool established.")
            return self._conn
        except redis.ConnectionError as e:
            logger.error("Failed to initialize Redis Connection Pool: %s", e)
            raise ConnectionError("Failed to initialize Redis Connection Pool") # Mengangkat kesalahan koneksi Redis
        except Exception as e:
            logger.error(f"Unexpected error while initializing Redis: {e}")
            raise  # Mengangkat kesalahan lainnya
    
    def get_connection(self):
        if not self._conn:
            self.__enter__()
        return self._conn

    def close(self):
        if self.pool:
            try:
                self.pool.disconnect()
                # logger.info("Redis Connection Pool closed.")
            except Exception as e:
                logger.error(f"Error while closing Redis Connection Pool: {e}")

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()
        if exc_type:
            logger.exception(
                f"Error occurred in RedisConn: exc_type={exc_type}, exc_value={exc_value}, traceback={exc_traceback}"
            )
            return False  # Membiarkan pengecualian diteruskan keluar dari blok 'with'