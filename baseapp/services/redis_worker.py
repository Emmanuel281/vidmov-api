import time
from threading import Thread

import logging
logger = logging.getLogger()

from baseapp.services.redis_queue import RedisQueueManager
from baseapp.services.mail_manager import MailManager
from baseapp.services.minio_manager import MinioManager

class RedisWorker:
    def __init__(self,redis_queue_manager: RedisQueueManager):
        self.queue_manager = redis_queue_manager
        self.mail_manager = MailManager()
        self.minio_manager = MinioManager()
        self.is_running = False

    def process_task(self, task: dict):
        """
        Process a task (e.g., send OTP).
        """
        func = task.get("func")
        if func == "otp":
            email = task.get("email")
            otp = task.get("otp")
            logger.info(f"Processing task: Sending OTP {otp} to {email}")
            time.sleep(3)  # Simulate sending OTP
            self.mail_manager.send_mail(task)
            logger.info(f"Task completed: OTP {otp} sent to {email}")
        elif func == "minio_del_file":
            _table = task.get("table")
            _data_id = task.get("id")
            logger.info(f"Processing task: Deleting file on table:{_table} with ID:{_data_id}")
            time.sleep(3)  # Simulate deleting file on data
            self.minio_manager.delete_file(task)
            logger.info(f"Task completed: File deleted from table: {_table} with ID:{_data_id}")
        else:
            logger.info("Other task")

    def worker_loop(self):
        """
        Worker loop to continuously process tasks from the queue.
        """
        self.is_running = True
        while self.is_running:
            task = self.queue_manager.dequeue_task()
            if task:
                self.process_task(task)
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
