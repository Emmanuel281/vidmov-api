import logging
logger = logging.getLogger()

from baseapp.config import email_smtp

class MailManager:
    def __init__(self):
        self.mail_manager = email_smtp.EmailSender()

    def send_mail(self, data: dict):
        logger.debug(f"data task: {data} type data: {type(data)}")
        msg_val = {
            "to":data.get("email"), # mandatory | kalau lebih dari satu jadi array ["aldian@gai.co.id","charly@gai.co.id"]
            "subject": data.get("subject"),
            "body_mail": data.get("body")
        }

        body_mail, bcc_recipients = self.mail_manager.body_msg(msg_val)
        self.mail_manager.send_email(body_mail, bcc_recipients)
