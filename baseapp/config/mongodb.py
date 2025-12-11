from pymongo import MongoClient,errors
import time
from baseapp.config import setting
from baseapp.utils.logger import Logger

config = setting.get_settings()
logger = Logger("baseapp.config.mongodb")

class MongoConn:
    _client = None

    def __init__(self, database=None):
        self.database = database or config.mongodb_db
        self._db = None
        self._context_start_time = None

    @classmethod
    def initialize(cls):
        """
        Inisialisasi Global Connection Pool.
        Wajib dipanggil SEKALI saat aplikasi start (misal di main.py).
        """
        if cls._client is None:
            try:
                start_time = time.perf_counter()

                # Konstruksi URI dengan/tanpa autentikasi
                if config.mongodb_user and config.mongodb_pass:
                    uri = f"mongodb://{config.mongodb_user}:{config.mongodb_pass}@{config.mongodb_host}:{config.mongodb_port}"
                else:
                    uri = f"mongodb://{config.mongodb_host}:{config.mongodb_port}"

                # Membuat MongoClient (Otomatis mengatur pooling)
                # maxPoolSize=100 (default)
                cls._client = MongoClient(
                    uri,
                    minPoolSize=config.mongodb_min_pool_size, 
                    maxPoolSize=config.mongodb_max_pool_size,
                )
                
                # Test koneksi ringan (opsional)
                # cls._client.admin.command('ping')
                
                duration_ms = (time.perf_counter() - start_time) * 1000
                logger.log_operation(
                    "mongodb_pool_initialize",
                    "success",
                    duration_ms=round(duration_ms, 2),
                    uri=uri,
                    min_pool_size=config.mongodb_min_pool_size,
                    max_pool_size=config.mongodb_max_pool_size
                )
                
            except errors.ConnectionFailure as e:
                logger.error(
                    "MongoDB connection failed",
                    host=config.mongodb_host,
                    port=config.mongodb_port,
                    error=str(e),
                    error_type="ConnectionFailure"
                )
                raise ConnectionError("Failed to connect to MongoDB")
            except errors.ServerSelectionTimeoutError as e:
                logger.error(
                    "MongoDB server selection timeout",
                    host=config.mongodb_host,
                    port=config.mongodb_port,
                    error=str(e),
                    error_type="ServerSelectionTimeoutError"
                )
                raise ConnectionError("MongoDB server unreachable") from e
            except Exception as e:
                logger.log_error_with_context(e, {
                    "operation": "mongodb_initialize",
                    "host": config.mongodb_host,
                    "port": config.mongodb_port
                })
                raise

    @classmethod
    def close_connection(cls):
        """
        Menutup seluruh koneksi di pool. Dipanggil saat aplikasi shutdown.
        """
        if cls._client:
            logger.info("Closing MongoDB connection pool")
            
            try:
                cls._client.close()
                cls._client = None
                
                logger.log_operation(
                    "mongodb_pool_close",
                    "success"
                )
                
            except Exception as e:
                logger.error(
                    "Error closing MongoDB connection pool",
                    error=str(e),
                    error_type=type(e).__name__
                )

    def __enter__(self):
        self._context_start_time = time.perf_counter()
        try:
            # Lazy Init: Jaga-jaga jika lupa panggil initialize() di main.py
            if self.__class__._client is None:
                logger.warning(
                    "MongoDB client not initialized, initializing now",
                    database=self.database
                )
                self.__class__.initialize()

            logger.debug(
                "MongoDB context opened",
                database=self.database
            )
            # Pilih Database dari client yang sudah ada (sangat cepat)
            self._db = self.__class__._client[self.database]
            return self
        except errors.ServerSelectionTimeoutError as e:
            logger.error(
                "MongoDB server selection timeout on context enter",
                database=self.database,
                error=str(e)
            )
            raise ConnectionError("Failed to connect to MongoDB")
        except errors.OperationFailure as e:
            logger.error(
                "MongoDB authentication failed",
                database=self.database,
                error=str(e),
                error_code=e.code if hasattr(e, 'code') else None
            )
            raise ConnectionError("Authentication failed to connect to MongoDB")
        except errors.PyMongoError as e:
            logger.error(
                "MongoDB error on context enter",
                database=self.database,
                error=str(e),
                error_type=type(e).__name__
            )
            raise 
        except Exception as e:
            logger.log_error_with_context(e, {
                "operation": "mongodb_context_enter",
                "database": self.database
            })
            raise

    def __exit__(self, exc_type, exc_value, exc_traceback):
        duration_ms = None
        if self._context_start_time:
            duration_ms = (time.perf_counter() - self._context_start_time) * 1000
        
        if exc_type:
            # Ada error dalam context
            logger.error(
                "MongoDB context error",
                database=self.database,
                duration_ms=round(duration_ms, 2) if duration_ms else None,
                error_type=exc_type.__name__,
                error=str(exc_value)
            )
        else:
            # Success - hanya log jika duration signifikan (> 100ms)
            if duration_ms and duration_ms > 100:
                logger.debug(
                    "MongoDB context closed",
                    database=self.database,
                    duration_ms=round(duration_ms, 2)
                )
        
        self._db = None
        self._context_start_time = None
        
        # Return False agar exception tetap naik (raise) ke pemanggil
        return False

    def __getattr__(self, name):
        """
        Memungkinkan akses collection langsung via attribute.
        Contoh: mongo.users.find() daripada mongo.get_database()['users'].find()
        """
        if self._db is not None:
            return self._db[name]
        logger.error(
            "Database context not active",
            attribute=name,
            database=self.database
        )
        raise AttributeError(f"Database context not active or attribute '{name}' not found.")
    
    def get_database(self):
        if self._db is None:
            logger.warning(
                "Attempted to get database outside context",
                database=self.database
            )
            raise ValueError("Database is not selected")
        return self._db

    def get_connection(self):
        if not self.__class__._client:
            logger.info("Getting connection - client not initialized, initializing now")
            self.__class__.initialize()
        return self.__class__._client