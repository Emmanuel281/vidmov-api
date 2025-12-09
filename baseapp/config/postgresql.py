import psycopg2
import time
from psycopg2 import errors
from psycopg2.extras import RealDictCursor
from typing import List
from baseapp.config import setting
from baseapp.utils.logger import Logger

config = setting.get_settings()
logger = Logger("baseapp.config.postgresql")

class PostgreSQLConn:
    _pool = None  # Variable statis untuk menyimpan Pool (Shared)

    def __init__(self):
        # Kita tidak lagi butuh host/user per instance, karena ikut konfigurasi Pool
        self.database = config.postgresql_db
        self._conn = None
        self._cursor = None
        self._context_start_time = None

    @classmethod
    def initialize_pool(cls):
        """
        Inisialisasi Pool. Panggil ini SEKALI saja saat aplikasi start (misal di main.py).
        """
        if cls._pool is None:
            try:
                # Setup ThreadedConnectionPool
                # minconn=1: Minimal ada 1 koneksi standby
                # maxconn=20: Maksimal 20 koneksi bersamaan (sesuaikan dengan beban app)
                start_time = time.perf_counter()
                cls._pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=config.postgresql_min_pool_size,
                    maxconn=config.postgresql_max_pool_size,
                    host=config.postgresql_host,
                    port=config.postgresql_port,
                    database=config.postgresql_db,
                    user=config.postgresql_user,
                    password=config.postgresql_pass,
                    cursor_factory=RealDictCursor # Agar default cursornya Dictionary
                )
                duration_ms = (time.perf_counter() - start_time) * 1000
                logger.log_operation(
                    "postgresql Connection Pool",
                    "success",
                    duration_ms=round(duration_ms, 2),
                    host=config.postgresql_host,
                    port=config.postgresql_port,
                    database=config.postgresql_db,
                    minconn=config.postgresql_min_pool_size,
                    maxconn=config.postgresql_max_pool_size
                )
            except Exception as e:
                logger.log_error_with_context(e, {
                    "operation": "postgresql_initialize",
                    "host": config.postgresql_host,
                    "port": config.postgresql_port,
                    "database": config.postgresql_db,
                    "minconn": config.postgresql_min_pool_size,
                    "maxconn": config.postgresql_max_pool_size                    
                })
                raise

    @classmethod
    def close_pool(cls):
        """
        Tutup semua koneksi di pool saat aplikasi mati (shutdown).
        """
        if cls._pool:
            logger.info("Closing PostgreSQL connection pool")
            try:
                cls._pool.closeall()
                cls._pool = None
                logger.log_operation(
                    "postgresql_pool_close",
                    "success"
                )
            except Exception as e:
                logger.error(
                    "Error closing PostgreSQL connection pool",
                    error=str(e),
                    error_type=type(e).__name__
                )

    def __enter__(self):
        self._context_start_time = time.perf_counter()
        try:
            # Lazy initialization: Jika lupa panggil initialize_pool, kita panggil otomatis
            if self.__class__._pool is None:
                logger.warning(
                    "PostgreSQL client not initialized, initializing now",
                    database=self.database
                )
                self.__class__.initialize_pool()

            logger.debug(
                "PostgreSQL context opened",
                database=self.database
            )

            # PINJAM koneksi dari pool
            self._conn = self.__class__._pool.getconn()
            self._conn.autocommit = False
            
            # Buat cursor (otomatis pakai RealDictCursor dari setting pool)
            self._cursor = self._conn.cursor()
            
            return self
            
        except errors.OperationalError as e:
            logger.error(
                "PostgreSQL Failed to get connection from pool",
                database=self.database,
                error=str(e)
            )
            raise ConnectionError("Failed to connect to PostgreSQL")
        except Exception as e:
            logger.log_error_with_context(e, {
                "operation": "postgresql_context_enter",
                "database": self.database
            })
            raise

    def __exit__(self, exc_type, exc_value, exc_traceback):
        duration_ms = None
        if self._context_start_time:
            duration_ms = (time.perf_counter() - self._context_start_time) * 1000

        try:

            # 1. Handle Transaksi (Commit/Rollback)
            if exc_type:
                if self._conn:
                    self._conn.rollback()
                    logger.error(
                        "Transaction rolled back",
                        database=self.database,
                        duration_ms=round(duration_ms, 2) if duration_ms else None,
                        error_type=exc_type.__name__,
                        error=str(exc_value)
                    )
            else:
                if self._conn:
                    self._conn.commit()
                    # Success - hanya log jika duration signifikan (> 100ms)
                    if duration_ms and duration_ms > 100:
                        logger.debug(
                            "PostgreSql context closed",
                            database=self.database,
                            duration_ms=round(duration_ms, 2)
                        )
                    # logger.info("Transaction committed successfully") # Uncomment jika ingin log verbose
        except Exception as e:
            logger.error(
                "Error during commit/rollback",
                database=self.database,
                duration_ms=round(duration_ms, 2) if duration_ms else None,
                error_type=exc_type.__name__,
                error=str(exc_value)
            )
        finally:
            # CLEANUP: Wajib dijalankan apa pun yang terjadi di atas
            if self._cursor:
                self._cursor.close()
            
            if self._conn:
                # Kembalikan koneksi ke pool
                try:
                    self.__class__._pool.putconn(self._conn)
                except Exception as put_error:
                    logger.error(
                        "Failed to return connection to pool",
                        database=self.database,
                        duration_ms=round(duration_ms, 2) if duration_ms else None,
                        error_type=exc_type.__name__,
                        error=str(put_error)
                    )
                
                self._conn = None # Reset instance variable

    def execute_query(self, query: str, params: tuple = None) -> None:
        """
        Execute a SELECT query and return results as list of dictionaries.
        """
        try:
            if params:
                self._cursor.execute(query, params)
            else:
                self._cursor.execute(query)
            
            logger.log_db_operation(
                "select_query",
                "success",
                query=query,
                params=params
            )
            result = self._cursor.fetchall() # FETCH DATA (Perbaikan dari versi sebelumnya)
            return result
            
        except errors.Error as e:
            logger.error(
                "PostgreSQL error during query execution",
                query=query,
                params=params,
                error=str(e),
                error_type=type(e).__name__
            )
            raise ValueError(f"Database error: {e}")
        except Exception as e:
            logger.log_error_with_context(e, {
                "operation": "Unexpected error during query execution",
                "query": query,
                "params": params
            })
            raise

    def execute_non_query(self, query: str, params: tuple = None) -> int:
        """
        Execute an INSERT, UPDATE, or DELETE query and return affected row count.
        """
        try:
            if params:
                self._cursor.execute(query, params)
            else:
                self._cursor.execute(query)
            
            affected_rows = self._cursor.rowcount
            logger.log_db_operation(
                "execute_query",
                "success",
                query=query,
                params=params,
                affected_rows=affected_rows
            )
            return affected_rows
            
        except errors.Error as e:
            logger.error(
                "PostgreSQL error during non-query execution",
                query=query,
                params=params,
                error=str(e),
                error_type=type(e).__name__
            )
            raise ValueError(f"Database error: {e}")
        except Exception as e:
            logger.log_error_with_context(e, {
                "operation": "Unexpected error during non-query execution",
                "query": query,
                "params": params
            })
            raise

    def execute_many(self, query: str, params_list: List[tuple]) -> int:
        """
        Execute the same query multiple times with different parameters.
        """
        try:
            self._cursor.executemany(query, params_list)
            affected_rows = self._cursor.rowcount
            logger.log_db_operation(
                "batch_execute_query",
                "success",
                query=query,
                params_list=params_list,
                affected_rows=affected_rows
            )
            return affected_rows
            
        except errors.Error as e:
            logger.error(
                "PostgreSQL error during batch execution",
                query=query,
                params_list=params_list,
                error=str(e),
                error_type=type(e).__name__
            )
            raise ValueError(f"Database error: {e}")
        except Exception as e:
            logger.log_error_with_context(e, {
                "operation": "Unexpected error during batch execution",
                "query": query,
                "params_list": params_list
            })
            raise