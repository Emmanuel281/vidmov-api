import psycopg2
from psycopg2 import errors
from psycopg2.extras import RealDictCursor
import logging
from typing import List
from baseapp.config import setting

config = setting.get_settings()
logger = logging.getLogger(__name__)

class PostgreSQLConn:
    _pool = None  # Variable statis untuk menyimpan Pool (Shared)

    def __init__(self):
        # Kita tidak lagi butuh host/user per instance, karena ikut konfigurasi Pool
        self._conn = None
        self._cursor = None

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
                logger.info("PostgreSQL Connection Pool created successfully.")
            except Exception as e:
                logger.exception(f"Failed to create connection pool: {e}")
                raise

    @classmethod
    def close_pool(cls):
        """
        Tutup semua koneksi di pool saat aplikasi mati (shutdown).
        """
        if cls._pool:
            cls._pool.closeall()
            cls._pool = None
            logger.info("PostgreSQL Connection Pool closed.")

    def __enter__(self):
        try:
            # Lazy initialization: Jika lupa panggil initialize_pool, kita panggil otomatis
            if self.__class__._pool is None:
                self.__class__.initialize_pool()

            # PINJAM koneksi dari pool
            self._conn = self.__class__._pool.getconn()
            self._conn.autocommit = False
            
            # Buat cursor (otomatis pakai RealDictCursor dari setting pool)
            self._cursor = self._conn.cursor()
            
            return self
            
        except errors.OperationalError as e:
            logger.error(f"Failed to get connection from pool: {e}")
            raise ConnectionError("Failed to connect to PostgreSQL")
        except Exception as e:
            logger.exception(f"Unexpected error in __enter__: {e}")
            raise

    def __exit__(self, exc_type, exc_value, exc_traceback):
        # 1. Handle Transaksi (Commit/Rollback)
        if exc_type:
            if self._conn:
                self._conn.rollback()
                logger.error(f"Transaction rolled back due to error: {exc_value}")
        else:
            if self._conn:
                self._conn.commit()
                # logger.info("Transaction committed successfully") # Uncomment jika ingin log verbose

        # 2. Bersihkan Cursor
        if self._cursor:
            self._cursor.close()

        # 3. KEMBALIKAN koneksi ke Pool (JANGAN di-close)
        if self._conn:
            # putconn mengembalikan koneksi agar bisa dipakai request lain
            self.__class__._pool.putconn(self._conn)
            self._conn = None # Lepas referensi

        if exc_type:
            # Biarkan exception naik ke atas
            return False

    def execute_query(self, query: str, params: tuple = None) -> None:
        """
        Execute a SELECT query and return results as list of dictionaries.
        """
        try:
            if params:
                self._cursor.execute(query, params)
            else:
                self._cursor.execute(query)
            
            logger.info(f"Query executed successfully")
            result = self._cursor.fetchall() # FETCH DATA (Perbaikan dari versi sebelumnya)
            return result
            
        except errors.Error as e:
            logger.error(f"PostgreSQL error during query execution: {e}")
            raise ValueError(f"Database error: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error during query execution: {e}")
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
            logger.info(f"Non-query executed successfully, affected {affected_rows} rows")
            return affected_rows
            
        except errors.Error as e:
            logger.error(f"PostgreSQL error during non-query execution: {e}")
            raise ValueError(f"Database error: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error during non-query execution: {e}")
            raise

    def execute_many(self, query: str, params_list: List[tuple]) -> int:
        """
        Execute the same query multiple times with different parameters.
        """
        try:
            self._cursor.executemany(query, params_list)
            affected_rows = self._cursor.rowcount
            logger.info(f"Batch query executed successfully, affected {affected_rows} rows")
            return affected_rows
            
        except errors.Error as e:
            logger.error(f"PostgreSQL error during batch execution: {e}")
            raise ValueError(f"Database error: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error during batch execution: {e}")
            raise