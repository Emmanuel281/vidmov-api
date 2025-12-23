from abc import abstractmethod
import time
import sys
from threading import Thread, Event

from baseapp.utils.logger import Logger
from baseapp.services.redis_queue import RedisQueueManager

logger = Logger("baseapp.services._redis_worker.base_worker")

class BaseWorker:
    def __init__(self,redis_queue_manager: RedisQueueManager, max_retries: int = 3):
        self.queue_manager = redis_queue_manager
        self.is_running = False
        self.thread = None
        self.stop_event = Event()
        self.max_retries = max_retries
        self.consecutive_errors = 0

    @abstractmethod
    def process_task(self, data: dict):
        """Metode ini WAJIB di-override oleh setiap worker spesifik."""
        pass

    def worker_loop(self):
        """
        Worker loop to continuously process tasks from the queue.
        """
        self.is_running = True
        logger.info("Worker loop started.")
        try:
            while self.is_running and not self.stop_event.is_set():
                try:
                    task = self.queue_manager.dequeue_task()
                    if task:
                        self.process_task(task)
                        # Reset error counter on successful task processing
                        self.consecutive_errors = 0
                    else:
                        time.sleep(1)  # Sleep if no tasks are available
                except Exception as e:
                    self.consecutive_errors += 1
                    logger.error(
                        f"Error processing task. Error: {e}. "
                        f"Consecutive errors: {self.consecutive_errors}/{self.max_retries}"
                    )

                    # Jika terlalu banyak error berturut-turut, hentikan worker
                    if self.consecutive_errors >= self.max_retries:
                        logger.critical(
                            f"Max consecutive errors ({self.max_retries}) reached. "
                            "Stopping worker and exiting container."
                        )
                        self.is_running = False
                        # Exit dengan status code 1 agar container crash
                        sys.exit(1)
                    
                    # Tunggu sebentar sebelum mencoba lagi
                    time.sleep(2)
        except Exception as e:
            logger.critical(f"Fatal error in worker loop: {e}")
            sys.exit(1)
        finally:
            logger.info("Worker loop ended.")  

    def start(self):
        """
        Start the worker in a new thread.
        """
        # Ubah daemon=False agar main thread menunggu worker thread
        self.thread = Thread(target=self.worker_loop, daemon=False)
        self.thread.start()
        logger.info("Worker started.")
        return self.thread

    def stop(self):
        """
        Stop the worker gracefully.
        """
        logger.info("Stopping worker...")
        self.is_running = False
        self.stop_event.set()
        
        # Tunggu thread selesai (dengan timeout)
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=10)
            if self.thread.is_alive():
                logger.warning("Worker thread did not stop gracefully")
        
        logger.info("Worker stopped.")

    def is_alive(self):
        """
        Check if worker thread is still alive.
        """
        return self.thread and self.thread.is_alive()