from abc import ABC, abstractmethod
import sys
from baseapp.utils.logger import Logger

logger = Logger("baseapp.services._rabbitmq_worker.base_worker")

class BaseRabbitMQWorker(ABC):
    """
    Base class untuk RabbitMQ workers.
    Menyediakan error handling yang konsisten.
    """
    
    def __init__(self, max_consecutive_errors: int = 3):
        self.max_consecutive_errors = max_consecutive_errors
        self.consecutive_errors = 0
    
    @abstractmethod
    def process_task(self, data: dict):
        """
        Method yang WAJIB di-override oleh setiap worker spesifik.
        Raise exception jika terjadi infrastructure error.
        """
        pass
    
    def process(self, task_data: dict) -> bool:
        """
        Wrapper untuk process_task dengan error handling.
        Returns:
            True jika task berhasil (ack)
            False jika task gagal validasi (nack, tidak requeue)
        Raises:
            Exception untuk infrastructure errors (akan di-handle oleh consumer)
        """
        try:
            self.process_task(task_data)
            # Reset error counter on success
            self.consecutive_errors = 0
            return True
            
        except ValueError as ve:
            # Error validasi data - skip task ini
            logger.warning(f"Task validation failed: {ve}. Task will be rejected (nack).")
            # Reset counter karena ini bukan infrastructure error
            self.consecutive_errors = 0
            return False
            
        except Exception as e:
            # Infrastructure error - increment counter
            self.consecutive_errors += 1
            logger.error(
                f"Infrastructure error processing task: {e}. "
                f"Consecutive errors: {self.consecutive_errors}/{self.max_consecutive_errors}"
            )
            
            # Jika terlalu banyak error berturut-turut, crash container
            if self.consecutive_errors >= self.max_consecutive_errors:
                logger.critical(
                    f"Max consecutive errors ({self.max_consecutive_errors}) reached. "
                    "Exiting container for restart."
                )
                sys.exit(1)
            
            # Re-raise agar consumer bisa handle (nack dengan requeue)
            raise
    
    def reset_error_counter(self):
        """Reset consecutive error counter"""
        self.consecutive_errors = 0