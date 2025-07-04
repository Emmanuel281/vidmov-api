import logging,uuid

from pymongo.errors import PyMongoError
from typing import Optional
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, timezone

from baseapp.config.setting import get_settings
config = get_settings()

class AuditTrailModel(BaseModel):
    rec_date: Optional[datetime] = Field(default=datetime.now(timezone.utc), description="This enum is created at.")
    org_id: Optional[str] = Field(default=None, description="Organization associated with the enum.")
    uid: str = Field(..., description="User ID performing the action")
    action: str = Field(..., description="Action performed, e.g., create, update, delete")
    target: str = Field(..., description="Target entity of the action, e.g., user, product")
    target_id: str = Field(..., description="ID of the target entity")
    details: dict = Field(default_factory=dict, description="Additional details about the action")
    ip_address: str = Field(None, description="IP address of the user or system performing the action")
    user_agent: str = Field(None, description="User agent string of the device used for the action")
    status: str = Field("success", description="Status of the operation, e.g., success, failure")
    error_message: str = Field(None, description="Error message if the operation failed")

class AuditTrailService:
    def __init__(self, user_id, org_id, ip_address=None, user_agent=None, collection_name="_audittrail"):
        self.user_id = user_id
        self.org_id = org_id
        self.ip_address = ip_address
        self.user_agent = user_agent
        self.collection_name = collection_name
        self.logger = logging.getLogger()

    def create(self, mongo_conn, data: AuditTrailModel):
        """
        Insert a new audittrail into the collection.
        """
        collection = mongo_conn._db[self.collection_name]
        data["_id"] = str(uuid.uuid4())
        try:
            result = collection.insert_one(data)
            self.logger.info(f"Inserted document with ID: {result.inserted_id}")
            return {"inserted_id": str(result.inserted_id)}
        except PyMongoError as pme:
            self.logger.error(f"Database error occurred: {str(pme)}")
            raise ValueError("Database error occurred while creating document.") from pme
        except Exception as e:
            self.logger.exception(f"Unexpected error occurred while creating document: {str(e)}")
            raise

    def log_audittrail(self, mongo_conn, action, target, target_id, details=None, status="success", error_message=None):
        data = {
            "org_id": self.org_id,
            "uid": self.user_id,
            "action": action,
            "target": target,
            "target_id": target_id,
            "details": details or {},
            "ip_address": self.ip_address,
            "user_agent": self.user_agent,
            "status": status,
            "error_message": error_message
        }
        return self.create(mongo_conn, data)
