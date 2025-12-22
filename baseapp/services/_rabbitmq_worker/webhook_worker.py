from baseapp.utils.logger import Logger
from baseapp.services._rabbitmq_worker.base_worker import BaseRabbitMQWorker
logger = Logger("baseapp.services._rabbitmq_worker._webhook_worker")

class WebhookWorker(BaseRabbitMQWorker):
    def __init__(self, max_consecutive_errors: int = 3):
        super().__init__(max_consecutive_errors)
    
    def process_task(self, data: dict):
        """
        Process webhook task.
        Raises ValueError for validation errors.
        Raises other exceptions for infrastructure errors.
        """
        logger.info(f"Processing webhook task: {data}")
        
        # Contoh validasi
        if not data.get("url"):
            raise ValueError("Missing required field: 'url'")
        
        # TODO: Implementasi logika webhook
        # requests.post(data['url'], json=data.get('payload'))
        
        logger.info("Webhook task finished")