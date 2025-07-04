import logging, random, uuid
from datetime import datetime, timezone

from baseapp.model.common import REDIS_QUEUE_BASE_KEY, Status
from baseapp.config import setting, mongodb
from baseapp.config.redis import RedisConn
from baseapp.services.redis_queue import RedisQueueManager
from baseapp.services._forgot_password.model import OTPRequest, VerifyOTPRequest, ResetPasswordRequest
from baseapp.utils.utility import hash_password

config = setting.get_settings()

# In-memory storage for simplicity
user_data = {}

class CRUD:
    def __init__(self):
        self.logger = logging.getLogger()
        self.redis_conn = RedisConn()
        self.queue_manager = RedisQueueManager(queue_name=REDIS_QUEUE_BASE_KEY)  # Pass actual RedisConn here

    def is_valid_user(self,username: str) -> bool:
        client = mongodb.MongoConn()
        with client as mongo:
            collection = mongo._db["_user"]
            query = {"$or": [{"username": username}, {"email": username}]}
            user_info = collection.find_one(query)
            if not user_info:
                return False
            if user_info.get("status") != Status.ACTIVE.value:
                return False
            return user_info["_id"]
        
    def send_otp(self, req: OTPRequest):
        """
        API to enqueue OTP sending task.
        """
        try:
            if not self.is_valid_user(req.email):
                raise ValueError("User not found")
            
            otp = str(random.randint(100000, 999999))  # Generate random 6-digit OTP

            # msg_val = {
            #     "to":req.email, # mandatory | kalau lebih dari satu jadi array ["aldian@gai.co.id","charly@gai.co.id"]
            #     "subject": "Request Forgot Password",
            #     "body_mail": f"Berikut kode OTP Anda: {otp}"
            # }

            # body_mail, bcc_recipients = self.mail_manager.body_msg(msg_val)
            # mail_sending = self.mail_manager.send_email(body_mail, bcc_recipients)

            # Simpan OTP di Redis dengan TTL (misalnya 300 detik)
            with self.redis_conn as conn:
                conn.setex(f"otp:{req.email}", 300, otp)

            self.queue_manager.enqueue_task({"func":"otp","email": req.email, "otp": otp, "subject":"Request Forgot Password", "body":f"Berikut kode OTP Anda: {otp}"})
            return {"status": "queued", "message": "OTP has been sent"}
        except Exception as e:
            raise

    def verify_otp(self, req: VerifyOTPRequest):
        """
        API to enqueue OTP sending task.
        """
        try:
            # Simpan OTP di Redis dengan TTL (misalnya 300 detik)
            with self.redis_conn as conn:
                stored_otp = conn.get(f"otp:{req.email}")

            if stored_otp and stored_otp == req.otp:
                reset_token = str(uuid.uuid4())  # Use UUID for secure random token
                with self.redis_conn as conn:
                    conn.delete(f"otp:{req.email}")
                    conn.setex(f"reset_token:{req.email}", 900, reset_token)  # TTL: 15 minutes
                
                return {"status": "verified", "message": "OTP verified", "reset_token": reset_token}            

            raise ValueError("Invalid or expired OTP")
        except Exception as e:
            raise

    def reset_password(self, req: ResetPasswordRequest):
        """
        API to enqueue OTP sending task.
        """
        try:
            # Simpan OTP di Redis dengan TTL (misalnya 300 detik)
            with self.redis_conn as conn:
                stored_token = conn.get(f"reset_token:{req.email}")

            if stored_token and stored_token == req.reset_token:
                userinfo = self.is_valid_user(req.email)
                if not userinfo:
                    raise ValueError("User not found")
                
                salt, hashed_password = hash_password(req.new_password)

                client = mongodb.MongoConn()
                with client as mongo:
                    collection = mongo._db["_user"]
                    obj = {}
                    obj["password"] = hashed_password
                    obj["salt"] = salt
                    obj["mod_by"] = userinfo
                    obj["mod_date"] = datetime.now(timezone.utc)

                    reset_password = collection.find_one_and_update({"_id": userinfo}, {"$set": obj}, return_document=True)
                    if not reset_password:
                        raise ValueError("Reset password failed")
                    
                    with self.redis_conn as conn:
                        conn.delete(f"reset_token:{req.email}")

                    return {"status": "success", "message": "Password has been reset"}
            
            raise ValueError("Invalid or expired reset token")
        except Exception as e:
            raise