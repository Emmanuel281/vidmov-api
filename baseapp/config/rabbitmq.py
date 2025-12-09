import pika,time

import pika.exceptions
from baseapp.config import setting
from baseapp.utils.logger import Logger

config = setting.get_settings()
logger = Logger("baseapp.config.rabbitmq")

class RabbitMqConn:
    def __init__(self, host=None, port=None, user=None, password=None):
        self.host = host or config.rabbitmq_host
        self.port = port or config.rabbitmq_port
        self.user = user or config.rabbitmq_user
        self.password = password or config.rabbitmq_pass
        self.connection = None
        self.channel = None
        self._context_start_time = None
    
    def __enter__(self):
        self._context_start_time = time.perf_counter()
        try:
            credentials = pika.PlainCredentials(self.user, self.password)
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.host, 
                    port=self.port,
                    credentials=credentials,
                )
            )
            self.channel = self.connection.channel()
            duration_ms = (time.perf_counter() - self._context_start_time) * 1000
            logger.log_operation(
                "RabbitMQ Connection",
                "success",
                duration_ms=round(duration_ms, 2),
                host=self.host,
                port=self.port,
                channel=self.channel
            )
            return self.channel  # Return channel for usage in 'with' block
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(
                "RabbitMQ: Failed to connect",
                host=self.host,
                port=self.port,
                error=str(e),
                error_type="AMQPConnectionError"
            )
            raise ConnectionError("Failed to connect to RabbitMQ") # Mengangkat kesalahan khusus koneksi RabbitMQ
        except pika.exceptions.ChannelError as e:
            logger.error(
                "RabbitMQ: Channel error",
                channel=self.channel,
                error=str(e),
                error_type="ChannelError"
            )
            raise ConnectionError("RabbitMQ: Channel error") # Mengangkat kesalahan pada channel
        except Exception as e:
            logger.log_error_with_context(e, {
                "operation": "rabbitmq_context_enter",
                "host": self.host,
                "port": self.port,
                "channel": self.channel
            })
            raise # Mengangkat kesalahan lainnya

    def close(self):
        if self.channel and self.channel.is_open:
            self.channel.close()
            logger.info("RabbitMQ channel closed.")
        if self.connection and self.connection.is_open:
            self.connection.close()
            logger.info("RabbitMQ connection closed.")

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        duration_ms = None
        if self._context_start_time:
            duration_ms = (time.perf_counter() - self._context_start_time) * 1000
        if exc_type:
            logger.error(
                "RabbitMQ context error",
                host=self.host,
                port=self.port,
                channel=self.channel,
                duration_ms=round(duration_ms, 2) if duration_ms else None,
                error_type=exc_type.__name__,
                error=str(exc_value)
            )
            return False  # Memungkinkan pengecualian untuk terus diproses di luar blok 'with'
        else:
            # Success - hanya log jika duration signifikan (> 100ms)
            if duration_ms and duration_ms > 100:
                logger.debug(
                    "RabbitMQ context closed",
                    host=self.host,
                    port=self.port,
                    channel=self.channel,
                    duration_ms=round(duration_ms, 2)
                )