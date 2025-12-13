from pymongo.errors import PyMongoError
from typing import Optional
import random
import json

from baseapp.utils.utility import generate_uuid, hash_password
from baseapp.config import setting, mongodb, redis
from baseapp.model.common import Status, Authority
from baseapp.services.redis_queue import RedisQueueManager
from baseapp.services.register.model import Register, RegisterResponse, ResendOtpRequest, VerifyOtp
from baseapp.services._org.model import Organization, User, InitResponse
from baseapp.services._org.crud import CRUD as OrgCRUD
from baseapp.utils.logger import Logger, sanitize_log_data

config = setting.get_settings()
logger = Logger("baseapp.services.register.crud")

class CRUD:
    def __init__(self):
        self.collection_org = "_organization"
        self.collection_user = "_user"

    def __enter__(self):
        self._mongo_context = mongodb.MongoConn()
        self.mongo = self._mongo_context.__enter__()

        self._redis_context = redis.RedisConn()
        self.redis = self._redis_context.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if hasattr(self, '_mongo_context'):
            return self._mongo_context.__exit__(exc_type, exc_value, traceback)
        if hasattr(self, '_redis_context'):
            return self._redis_context.__exit__(exc_type, exc_value, traceback)
        return False

    def set_context(self, user_id: str, org_id: str, ip_address: Optional[str] = None, user_agent: Optional[str] = None):
        """
        Memperbarui konteks pengguna dan menginisialisasi AuditTrailService.
        """
        self.user_id = user_id
        self.org_id = org_id
        self.ip_address = ip_address
        self.user_agent = user_agent
    
    def register_member(self, data: Register):
        """
        Insert a new role into the collection.
        """
    
        collection_org = self.mongo.get_database()[self.collection_org]
        collection_user = self.mongo.get_database()[self.collection_user]

        obj = data.model_dump()
        
        try:
            owner_user_is_exist = collection_org.find_one({"org_email":obj["email"]})
            if owner_user_is_exist:
                logger.warning(f"Organization already exists.", email=obj['email'])
                raise ValueError("User with this email already exists.")

            owner_user_is_exist = collection_user.find_one({"username":obj["email"]})
            if owner_user_is_exist:
                logger.warning(f"Organization already exists.", email=obj['email'])
                raise ValueError("User with this email already exists.")

            session_id = generate_uuid()
            otp = str(random.randint(100000, 999999))
            hashed_password = hash_password(obj["password"])
            register_data = {
                "session": session_id,
                "email": obj["email"],
                "phone_number": obj["phone_number"],
                "password": hashed_password,
                "otp": otp
            }

            self.redis.setex(f"reg:{session_id}", 300, json.dumps(register_data))
        
            queue_manager = RedisQueueManager(self.redis, queue_name="otp_tasks")  # Pass actual RedisConn here
            queue_manager.enqueue_task({"email": obj["email"], "otp": otp, "subject":"Arena member registration", "body":f"OTP: {otp}"})
            
            return RegisterResponse(**register_data)
        except PyMongoError as pme:
            logger.error(f"Database error occurred: {str(pme)}")
            raise ValueError("Database error occurred while creating document.") from pme
        except Exception as e:
            logger.exception(f"Unexpected error occurred while creating document: {str(e)}")
            raise

    def resend_otp(self, data: ResendOtpRequest):
        """
        Resend OTP for registration.
        """
        obj = data.model_dump()
        
        try:
            otp = str(random.randint(100000, 999999))
            stored_data = self.redis.get(f"reg:{obj['session']}")
            if not stored_data:
                raise ValueError("Session expired or invalid.")
            register_data = json.loads(stored_data)
            register_data["otp"] = otp
            logger.info(f"Resending OTP for session {obj['session']}", **sanitize_log_data(register_data))
            self.redis.setex(f"reg:{obj['session']}", 300, json.dumps(register_data))

            queue_manager = RedisQueueManager(self.redis, queue_name="otp_tasks")  # Pass actual RedisConn here
            queue_manager.enqueue_task({"email": register_data["email"], "otp": otp, "subject":"Arena member registration", "body":f"OTP: {otp}"})
            
            return RegisterResponse(**register_data)
        except Exception as e:
            logger.exception(f"Unexpected error occurred while creating document: {str(e)}")
            raise

    def verify_member(self, data: VerifyOtp) -> InitResponse:
        """
        Verify OTP for registration.
        """
        obj = data.model_dump()
        try:
            stored_data = self.redis.get(f"reg:{obj['session']}")
            if not stored_data:
                raise ValueError("Session expired or invalid.")
            register_data = json.loads(stored_data)
            if register_data["otp"] != obj["otp"]:
                logger.warning(f"Invalid OTP", session=obj['session'])
                raise ValueError("Invalid OTP provided.")
            
            payload_data = {
                "org": {
                    "org_name": register_data["email"], # Default nama org = email
                    "org_email": register_data["email"],
                    "org_phone": register_data["phone_number"],
                    "authority": Authority.MEMBER.value, # Default Authority Member
                    "status": Status.ACTIVE # Atau Status.ACTIVE jika pakai enum
                },
                "user": {
                    "username": register_data["email"], # Default username = email
                    "email": register_data["email"],
                    "password": register_data["password"], # Password ini SUDAH di-hash di register_member
                    "status": Status.ACTIVE
                }
            }

            org_svc = OrgCRUD()
            org_svc.set_context(
                user_id=None,
                org_id=None,
                ip_address=self.ip_address,
                user_agent=self.user_agent
            )

            result = org_svc.reg_member(org_data=Organization.model_validate(payload_data["org"]), user_data=User.model_validate(payload_data["user"]))
            logger.info(f"Organization and User created successfully", org_id=result.org.id, user_id=result.user.id)
            self.redis.delete(f"reg:{obj['session']}")
            return result
        except Exception as e:
            logger.exception(f"Unexpected error occurred while creating document: {str(e)}")
            raise