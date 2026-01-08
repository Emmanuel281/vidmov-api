from opensearchpy import OpenSearch, exceptions
import time
from baseapp.config import setting
from baseapp.utils.logger import Logger

config = setting.get_settings()
logger = Logger("baseapp.config.opensearch")

class OpenSearchConn:
    _client = None

    def __init__(self, index=None):
        self.index = index
        self._context_start_time = None

    @classmethod
    def initialize(cls):
        """
        Inisialisasi Global OpenSearch Connection.
        Wajib dipanggil SEKALI saat aplikasi start (misal di main.py).
        """
        if cls._client is None:
            try:
                start_time = time.perf_counter()

                # Konfigurasi OpenSearch client
                opensearch_config = {
                    'hosts': [{'host': config.opensearch_host, 'port': config.opensearch_port}],
                    'http_compress': True,
                    'use_ssl': config.opensearch_use_ssl,
                    'verify_certs': config.opensearch_verify_certs,
                    'ssl_assert_hostname': False,
                    'ssl_show_warn': False,
                    'max_retries': 3,
                    'retry_on_timeout': True,
                    'timeout': 30
                }

                # Tambahkan autentikasi jika ada
                if config.opensearch_user and config.opensearch_pass:
                    opensearch_config['http_auth'] = (config.opensearch_user, config.opensearch_pass)

                cls._client = OpenSearch(**opensearch_config)
                
                # Test koneksi
                info = cls._client.info()
                
                duration_ms = (time.perf_counter() - start_time) * 1000
                logger.log_operation(
                    "opensearch_initialize",
                    "success",
                    duration_ms=round(duration_ms, 2),
                    host=config.opensearch_host,
                    port=config.opensearch_port,
                    version=info.get('version', {}).get('number', 'unknown')
                )
                
            except exceptions.ConnectionError as e:
                logger.error(
                    "OpenSearch connection failed",
                    host=config.opensearch_host,
                    port=config.opensearch_port,
                    error=str(e),
                    error_type="ConnectionError"
                )
                raise ConnectionError("Failed to connect to OpenSearch")
            except exceptions.AuthenticationException as e:
                logger.error(
                    "OpenSearch authentication failed",
                    host=config.opensearch_host,
                    port=config.opensearch_port,
                    error=str(e),
                    error_type="AuthenticationException"
                )
                raise ConnectionError("Authentication failed to OpenSearch")
            except Exception as e:
                logger.log_error_with_context(e, {
                    "operation": "opensearch_initialize",
                    "host": config.opensearch_host,
                    "port": config.opensearch_port
                })
                raise

    @classmethod
    def close_connection(cls):
        """
        Menutup koneksi OpenSearch. Dipanggil saat aplikasi shutdown.
        """
        if cls._client:
            logger.info("Closing OpenSearch connection")
            
            try:
                cls._client.close()
                cls._client = None
                
                logger.log_operation(
                    "opensearch_close",
                    "success"
                )
                
            except Exception as e:
                logger.error(
                    "Error closing OpenSearch connection",
                    error=str(e),
                    error_type=type(e).__name__
                )

    def __enter__(self):
        self._context_start_time = time.perf_counter()
        try:
            # Lazy Init
            if self.__class__._client is None:
                logger.warning(
                    "OpenSearch client not initialized, initializing now",
                    index=self.index
                )
                self.__class__.initialize()

            logger.debug(
                "OpenSearch context opened",
                index=self.index
            )
            return self
            
        except exceptions.ConnectionError as e:
            logger.error(
                "OpenSearch connection error on context enter",
                index=self.index,
                error=str(e)
            )
            raise ConnectionError("Failed to connect to OpenSearch")
        except Exception as e:
            logger.log_error_with_context(e, {
                "operation": "opensearch_context_enter",
                "index": self.index
            })
            raise

    def __exit__(self, exc_type, exc_value, exc_traceback):
        duration_ms = None
        if self._context_start_time:
            duration_ms = (time.perf_counter() - self._context_start_time) * 1000
        
        if exc_type:
            logger.error(
                "OpenSearch context error",
                index=self.index,
                duration_ms=round(duration_ms, 2) if duration_ms else None,
                error_type=exc_type.__name__,
                error=str(exc_value)
            )
        else:
            if duration_ms and duration_ms > 100:
                logger.debug(
                    "OpenSearch context closed",
                    index=self.index,
                    duration_ms=round(duration_ms, 2)
                )
        
        self._context_start_time = None
        return False

    def get_client(self):
        """Mendapatkan OpenSearch client instance"""
        if not self.__class__._client:
            logger.info("Getting client - not initialized, initializing now")
            self.__class__.initialize()
        return self.__class__._client

    def search(self, body, index=None, **kwargs):
        """
        Melakukan pencarian di OpenSearch
        
        Args:
            body: Query body dalam format OpenSearch DSL
            index: Nama index (opsional, menggunakan self.index jika tidak disediakan)
            **kwargs: Parameter tambahan untuk search API
        """
        target_index = index or self.index
        if not target_index:
            raise ValueError("Index name must be provided")
        
        try:
            start_time = time.perf_counter()
            response = self.get_client().search(
                index=target_index,
                body=body,
                **kwargs
            )
            duration_ms = (time.perf_counter() - start_time) * 1000
            
            logger.debug(
                "OpenSearch search executed",
                index=target_index,
                duration_ms=round(duration_ms, 2),
                hits=response.get('hits', {}).get('total', {}).get('value', 0)
            )
            
            return response
        except exceptions.NotFoundError as e:
            logger.error(
                "OpenSearch index not found",
                index=target_index,
                error=str(e)
            )
            raise
        except Exception as e:
            logger.log_error_with_context(e, {
                "operation": "opensearch_search",
                "index": target_index
            })
            raise

    def index_document(self, doc_id, body, index=None, **kwargs):
        """
        Index atau update dokumen
        
        Args:
            doc_id: ID dokumen
            body: Body dokumen
            index: Nama index
            **kwargs: Parameter tambahan
        """
        target_index = index or self.index
        if not target_index:
            raise ValueError("Index name must be provided")
        
        try:
            response = self.get_client().index(
                index=target_index,
                id=doc_id,
                body=body,
                **kwargs
            )
            logger.debug(
                "Document indexed",
                index=target_index,
                doc_id=doc_id,
                result=response.get('result')
            )
            return response
        except Exception as e:
            logger.log_error_with_context(e, {
                "operation": "opensearch_index_document",
                "index": target_index,
                "doc_id": doc_id
            })
            raise

    def bulk_index(self, actions, index=None, **kwargs):
        """
        Bulk indexing dokumen
        
        Args:
            actions: List of actions untuk bulk operation
            index: Nama index
            **kwargs: Parameter tambahan
        """
        from opensearchpy import helpers
        
        target_index = index or self.index
        
        try:
            start_time = time.perf_counter()
            success, failed = helpers.bulk(
                self.get_client(),
                actions,
                index=target_index,
                **kwargs
            )
            duration_ms = (time.perf_counter() - start_time) * 1000
            
            logger.log_operation(
                "opensearch_bulk_index",
                "success",
                duration_ms=round(duration_ms, 2),
                success_count=success,
                failed_count=len(failed) if failed else 0,
                index=target_index
            )
            
            return success, failed
        except Exception as e:
            logger.log_error_with_context(e, {
                "operation": "opensearch_bulk_index",
                "index": target_index
            })
            raise

    def delete_document(self, doc_id, index=None, **kwargs):
        """Delete dokumen dari index"""
        target_index = index or self.index
        if not target_index:
            raise ValueError("Index name must be provided")
        
        try:
            response = self.get_client().delete(
                index=target_index,
                id=doc_id,
                **kwargs
            )
            logger.debug(
                "Document deleted",
                index=target_index,
                doc_id=doc_id
            )
            return response
        except exceptions.NotFoundError:
            logger.warning(
                "Document not found for deletion",
                index=target_index,
                doc_id=doc_id
            )
            return None
        except Exception as e:
            logger.log_error_with_context(e, {
                "operation": "opensearch_delete_document",
                "index": target_index,
                "doc_id": doc_id
            })
            raise

    def create_index(self, index=None, body=None, **kwargs):
        """Create index baru dengan mapping"""
        target_index = index or self.index
        if not target_index:
            raise ValueError("Index name must be provided")
        
        try:
            response = self.get_client().indices.create(
                index=target_index,
                body=body or {},
                **kwargs
            )
            logger.log_operation(
                "opensearch_create_index",
                "success",
                index=target_index
            )
            return response
        except exceptions.RequestError as e:
            if 'resource_already_exists_exception' in str(e):
                logger.warning(
                    "Index already exists",
                    index=target_index
                )
                return None
            raise
        except Exception as e:
            logger.log_error_with_context(e, {
                "operation": "opensearch_create_index",
                "index": target_index
            })
            raise

    def delete_index(self, index=None, **kwargs):
        """Delete index"""
        target_index = index or self.index
        if not target_index:
            raise ValueError("Index name must be provided")
        
        try:
            response = self.get_client().indices.delete(
                index=target_index,
                **kwargs
            )
            logger.log_operation(
                "opensearch_delete_index",
                "success",
                index=target_index
            )
            return response
        except exceptions.NotFoundError:
            logger.warning(
                "Index not found for deletion",
                index=target_index
            )
            return None
        except Exception as e:
            logger.log_error_with_context(e, {
                "operation": "opensearch_delete_index",
                "index": target_index
            })
            raise