from baseapp.config import email_smtp
from baseapp.utils.logger import Logger
from baseapp.services._redis_worker.base_worker import BaseWorker

logger = Logger("baseapp.services._redis_worker.email_worker")

class EmailWorker(BaseWorker):
    def __init__(self, queue_manager, max_retries: int = 3):
        super().__init__(queue_manager, max_retries)
        self.mail_manager = email_smtp.EmailSender()

    def process_task(self, data: dict):
        """
        Process a task (e.g., send OTP).
        """
        logger.debug(f"data task: {data} type data: {type(data)}")
        try:
            
            if not data.get("email"):
                logger.error(f"Invalid task data: missing 'email' field. Data: {data}")
                raise ValueError("Missing required field: 'email'")
            
            if not data.get("subject"):
                logger.error(f"Invalid task data: missing 'subject' field. Data: {data}")
                raise ValueError("Missing required field: 'subject'")
                
            if not data.get("body"):
                logger.error(f"Invalid task data: missing 'body' field. Data: {data}")
                raise ValueError("Missing required field: 'body'")
            
            msg_val = {
                "to":data.get("email"), # mandatory | kalau lebih dari satu jadi array ["aldian@gai.co.id","charly@gai.co.id"]
                "subject": data.get("subject"),
                "body_mail": data.get("body")
            }

            body_mail, bcc_recipients = self.mail_manager.body_msg(msg_val)
            self.mail_manager.send_email(body_mail, bcc_recipients)

            logger.info(f"Email sent successfully to {data.get('email')}")
        except ValueError as ve:
            # Error validasi data - ini bukan error fatal, log dan skip task ini
            logger.error(f"Validation error: {ve}")
            return
            # Tidak raise agar tidak dihitung sebagai consecutive error
        except ConnectionError as ce:
            # Error koneksi SMTP - ini error infrastructure
            logger.error(f"SMTP connection error: {ce}")
            # Re-raise agar dihitung sebagai consecutive error (bisa crash worker)
            raise   
        except Exception as e:
            # Error lain yang lebih serius (SMTP error, network error, dll)
            logger.error(f"Error sending email to {data.get('email', 'unknown')}: {str(e)}")
            # Re-raise agar dihitung sebagai consecutive error
            raise
