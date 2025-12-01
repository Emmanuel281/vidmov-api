# Email Libs
import os
import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

from baseapp.config import setting

config = setting.get_settings()
logger = logging.getLogger()

class EmailSender:
    def __init__(self, host=None, port=None, username=None, password=None, use_tls=True):
        self.smtp_server = host or config.smtp_host
        self.smtp_port = port or config.smtp_port
        self.email_user = username or config.smtp_username
        self.email_password = password or config.smtp_password
        self.use_tls = use_tls
        
    def body_msg(self, values):
        msg = MIMEMultipart("alternative")
        msg['From'] = self.email_user
        msg['Subject'] = values["subject"]

        # send To
        if isinstance(values["to"], list):
            msg['To'] = ','.join(values["to"])
        else:
            msg['To'] = values["to"]

        # add Cc
        if "cc" in values:
            if isinstance(values["cc"], list):
                if len(values["cc"]) > 0:
                    msg['Cc'] = ', '.join(values["cc"])
            else:
                if values["cc"] != "":
                    msg['Cc'] = values["cc"]
        
        # add reply-to
        if "reply_to" in values:
            if isinstance(values["reply_to"], list):
                if len(values["reply_to"]) > 0:
                    multi_reply_to = ', '.join(values["reply_to"])
                    msg.add_header('Reply-To', multi_reply_to)
            else:
                if values["reply_to"] != "":
                    msg.add_header('Reply-To', values["reply_to"])

        # add BCc
        bcc_recipients = None
        if "bcc" in values:
            if isinstance(values["bcc"], list):
                if len(values["bcc"]) > 0:
                    bcc_recipients = ', '.join(values["bcc"])
            else:
                if len(values["bcc"]) != "":
                    bcc_recipients = values["bcc"]

        # body message
        htmlPart = MIMEText(values["body_mail"], 'html')
        msg.attach(htmlPart)

        # Open the file to be sent
        if "attachment_path" in values and values["attachment_path"] != "":
            try:
                with open(values["attachment_path"], "rb") as attachment:
                    # Create a MIMEBase object
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(attachment.read())

                    # Encode the payload using base64
                    encoders.encode_base64(part)

                    # Add header to the attachment
                    part.add_header(
                        "Content-Disposition",
                        f"attachment; filename= {os.path.basename(values['attachment_path'])}",
                    )

                    # Attach the file to the message
                    msg.attach(part)
            except Exception as e:
                logger.error(f"Failed to attach file: {e}")
                raise

        return msg,bcc_recipients

    def send_email(self, msg, bcc_recipients=None):
        try:
            logger.info(f"Trying to connect to {self.smtp_server}")

            # Establish connection
            if self.smtp_port == 465:
                # For SSL
                connect_server = smtplib.SMTP_SSL(self.smtp_server, self.smtp_port)
            else:
                # For TLS
                connect_server = smtplib.SMTP(self.smtp_server, self.smtp_port)

            # Create an SMTP connection object
            with connect_server as server:
                if self.use_tls:
                    server.starttls()  # Secure the connection with TLS

                # Try to login to the server
                server.login(self.email_user, self.email_password)

                # Combine To, CC, and BCC recipients
                all_recipients = []
                if 'To' in msg:
                    all_recipients.extend(msg['To'].split(', '))
                if 'Cc' in msg:
                    all_recipients.extend(msg['Cc'].split(', '))
                if bcc_recipients:
                    all_recipients.extend(bcc_recipients.split(', '))
                
                server.sendmail(msg['From'], all_recipients, msg.as_string())
                logger.info("Successfully connected to the SMTP server and sent the email!")
                server.quit()

            return True
        except smtplib.SMTPAuthenticationError as e:
            logger.error(f"Failed to authenticate with the SMTP server. Check your credentials. {e}")
            raise ValueError("Failed to authenticate with the SMTP server. Check your credentials.")
        except smtplib.SMTPConnectError as e:
            logger.error(f"Failed to connect to the SMTP server. Check the server address and port. {e}")
            raise ValueError("Failed to connect to the SMTP server. Check the server address and port.")
        except smtplib.SMTPException as e:
            logger.error(f"An SMTP error occurred: {e}")
            raise ValueError("An SMTP error occurred")

# Email template design
def _processPlaceHolder(template, placeHolders, replacements):
    length = len(placeHolders)
    for i in range(0, length):
        template = template.replace(placeHolders[i], replacements[i])
    return template

def loadHtmlEmailTemplate(fileName, placeHolders, replacements):
    body = open(fileName, 'r').read()
    # replacing place holders
    body = _processPlaceHolder(body, placeHolders, replacements)
    subject = ''
    # get subject from <title>
    tb = body.find('<title>')
    if tb > 0:
        tb += 7
        te = body.find('</title>')
        if te > 0:
            subject = body[tb:te]
    return (subject, body)

# Usage example:
if __name__ == "__main__":
    smtp_server = 'smtp.gmail.com'
    smtp_port = 465  # Use 587 for TLS or 465 for SSL
    email_user = 'indo.kadinmms@gmail.com'
    email_password = 'k@d1n2019'

    email_sender = EmailSender(smtp_server, smtp_port, email_user, email_password)

    # email object
    msg_val = {
        "to":"aldian@gai.co.id", # mandatory | kalau lebih dari satu jadi array ["aldian@gai.co.id","charly@gai.co.id"]
        "cc":"aldian.putra0594@gmail.com", # opsional(kalau tidak ada tidak usah kirim datanya) | kalau lebih dari satu jadi array ["aldian@gai.co.id","charly@gai.co.id"]
        "bcc":"aldian.putra0594@gmail.com", # opsional(kalau tidak ada tidak usah kirim datanya) | kalau lebih dari satu jadi array ["aldian@gai.co.id","charly@gai.co.id"]
        "reply_to":"aldian.putra0594@gmail.com", # opsional(kalau tidak ada tidak usah kirim datanya) | kalau lebih dari satu jadi array ["aldian@gai.co.id","charly@gai.co.id"]
        "subject":"Selamat ya bro",
        "body_mail":"Selamat ya bro"
    }

    # with attachment
    # If there is a document upload, first create the upload process and take the file path after it is uploaded.
    # msg_val["attachment_path"] = "/tmp/bla/bla/image.png"

    # when use template
    # verifyURL = "https://e-commerce.gnusa.id/marketplace-service/auth/verify_register?token=blablabla"
    # cond = ['%zfullname%', '%zurl%']
    # cond2 = ["Tokoku", verifyURL]
    # (subject, message) = loadHtmlEmailTemplate("tplemail/emailVerifikasiOrg.html",cond,cond2)
    # msg_val["subject"] = subject
    # msg_val["body_mail"] = message
    
    body_mail, bcc_recipients = email_sender.body_msg(msg_val)
    mail_sending = email_sender.send_email(body_mail, bcc_recipients)
    
    logger.debug(mail_sending)