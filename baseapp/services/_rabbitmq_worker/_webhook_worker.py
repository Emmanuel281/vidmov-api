from baseapp.utils.logger import Logger
logger = Logger("baseapp.services._rabbitmq_worker._webhook_worker")

class WebhookWorker:
    def process(self, task_data: dict):
        """Hanya berisi logika untuk memproses tugas webhook."""
        logger.info(f"Processing webhook task: {task_data}")
        logger.info("Webhook task finished.")