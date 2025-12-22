from baseapp.config import email_smtp
from baseapp.utils.logger import Logger
from baseapp.services._rabbitmq_worker.base_worker import BaseRabbitMQWorker

logger = Logger("baseapp.services._rabbitmq_worker.email_worker")

class EmailWorker(BaseRabbitMQWorker):
    def __init__(self, max_consecutive_errors: int = 3):
        super().__init__(max_consecutive_errors)
        self.mail_manager = email_smtp.EmailSender()

    def process_task(self, data: dict):
        """
        Process email sending task.
        Raises ValueError for validation errors.
        Raises other exceptions for infrastructure errors.
        """
        logger.debug(f"Processing email task: {data}")
        
        # Validasi data input
        if not data.get("email"):
            raise ValueError("Missing required field: 'email'")
        
        if not data.get("subject"):
            raise ValueError("Missing required field: 'subject'")
            
        if not data.get("body"):
            raise ValueError("Missing required field: 'body'")
        
        msg_val = {
            "to": data.get("email"),
            "subject": data.get("subject"),
            "body_mail": data.get("body")
        }

        # Send email - akan raise exception jika SMTP error
        body_mail, bcc_recipients = self.mail_manager.body_msg(msg_val)
        self.mail_manager.send_email(body_mail, bcc_recipients)
        
        logger.info(f"Email sent successfully to {data.get('email')}")