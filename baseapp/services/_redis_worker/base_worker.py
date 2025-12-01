from abc import abstractmethod
import time
from threading import Thread

import logging
logger = logging.getLogger("rabbit")

from baseapp.services.redis_queue import RedisQueueManager

class BaseWorker:
    def __init__(self,redis_queue_manager: RedisQueueManager):
        self.queue_manager = redis_queue_manager
        self.is_running = False

    @abstractmethod
    def process_task(self, data: dict):
        """Metode ini WAJIB di-override oleh setiap worker spesifik."""
        pass

    def worker_loop(self):
        """
        Worker loop to continuously process tasks from the queue.
        """
        self.is_running = True
        while self.is_running:
            task = self.queue_manager.dequeue_task()
            if task:
                try:
                    self.process_task(task)
                except Exception as e:
                    logger.error(f"Error processing task {task}. Error: {e}")
            else:
                time.sleep(1)  # Sleep if no tasks are available

    def start(self):
        """
        Start the worker in a new thread.
        """
        thread = Thread(target=self.worker_loop, daemon=True)
        thread.start()
        logger.info("Worker started.")
        return thread

    def stop(self):
        """
        Stop the worker.
        """
        self.is_running = False
        logger.info("Worker stopped.")
