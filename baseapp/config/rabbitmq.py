import pika,logging

import pika.exceptions
from baseapp.config import setting

config = setting.get_settings()
logger = logging.getLogger()

class RabbitMqConn:
    def __init__(self, host=None, port=None):
        self.host = host or config.rabbitmq_host
        self.port = port or config.rabbitmq_port
        self.connection = None
        self.channel = None
    
    def __enter__(self):
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.host, port=self.port)
            )
            self.channel = self.connection.channel()
            logger.info("RabbitMQ: Connection and channel established.")
            return self.channel  # Return channel for usage in 'with' block
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"RabbitMQ: Failed to connect : {e}")
            raise ConnectionError("Failed to connect to RabbitMQ") # Mengangkat kesalahan khusus koneksi RabbitMQ
        except pika.exceptions.ChannelError as e:
            logger.error(f"RabbitMQ: Channel error: {e}")
            raise ConnectionError("RabbitMQ: Channel error") # Mengangkat kesalahan pada channel
        except Exception as e:
            logger.error(f"RabbitMQ: Unexpected error: {e}")
            raise # Mengangkat kesalahan lainnya
    
    def get_connection(self):
        if not self.connection or self.connection.is_closed:
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=self.host, port=self.port)
                )
                logger.info("RabbitMQ connection established.")
            except pika.exceptions.AMQPConnectionError as e:
                logger.error(f"Failed to connect to RabbitMQ: {e}")
                raise ConnectionError("Failed to connect to RabbitMQ") # Mengangkat kesalahan koneksi RabbitMQ
            except Exception as e:
                logger.error(f"RabbitMQ: Unexpected error while establishing connection: {e}")
                raise  # Mengangkat kesalahan lainnya
        return self.connection

    def get_channel(self):
        if not self.channel or self.channel.is_closed:
            try:
                conn = self.get_connection()
                self.channel = conn.channel()
                logger.info("RabbitMQ channel created.")
            except pika.exceptions.ChannelError as e:
                logger.error(f"RabbitMQ: Channel error: {e}")
                raise ConnectionError("RabbitMQ: Channel error")# Mengangkat kesalahan pada channel
            except Exception as e:
                logger.error(f"RabbitMQ: Unexpected error while creating channel: {e}")
                raise  # Mengangkat kesalahan lainnya
        return self.channel

    def close(self):
        if self.channel and self.channel.is_open:
            self.channel.close()
            logger.info("RabbitMQ channel closed.")
        if self.connection and self.connection.is_open:
            self.connection.close()
            logger.info("RabbitMQ connection closed.")

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        if exc_type:
            logger.exception(f"Error occurred during RabbitMQ operation: {exc_type}, {exc_value}")
            return False  # Memungkinkan pengecualian untuk terus diproses di luar blok 'with'