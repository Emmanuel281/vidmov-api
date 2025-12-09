import logging
logger = logging.getLogger("")

class WebhookWorker:
    def process(self, task_data: dict):
        """Hanya berisi logika untuk memproses tugas webhook."""
        logger.info(f"Processing webhook task: {task_data}")
        logger.info("Webhook task finished.")