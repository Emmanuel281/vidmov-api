import logging
logger = logging.getLogger("rabbit")

from baseapp.services._redis_worker.base_worker import BaseWorker
from baseapp.config import email_smtp

class EmailWorker(BaseWorker):
    def __init__(self, queue_manager):
        super().__init__(queue_manager)
        self.mail_manager = email_smtp.EmailSender()

    def process_task(self, data: dict):
        """
        Process a task (e.g., send OTP).
        """
        logger.debug(f"data task: {data} type data: {type(data)}")
        msg_val = {
            "to":data.get("email"), # mandatory | kalau lebih dari satu jadi array ["aldian@gai.co.id","charly@gai.co.id"]
            "subject": data.get("subject"),
            "body_mail": data.get("body")
        }

        body_mail, bcc_recipients = self.mail_manager.body_msg(msg_val)
        self.mail_manager.send_email(body_mail, bcc_recipients)
