import secrets

from pymongo.errors import PyMongoError
from typing import Optional, Dict, Any
from pymongo import ASCENDING, DESCENDING
from datetime import datetime, timezone

from baseapp.config import setting, mongodb
from baseapp.utils.logger import Logger
from baseapp.services._api_credentials.model import ApiCredential, ApiCredentialCreate
from baseapp.services.audit_trail_service import AuditTrailService
from baseapp.utils.utility import hash_password, generate_uuid

config = setting.get_settings()
logger = Logger("baseapp.services._api_credentials.crud")

class CRUD:
    def __init__(self, collection_name="_api_credentials"):
        self.collection_name = collection_name

    def set_context(self, user_id: str, org_id: str, ip_address: Optional[str] = None, user_agent: Optional[str] = None):
        """
        Memperbarui konteks pengguna dan menginisialisasi AuditTrailService.
        """
        self.user_id = user_id
        self.org_id = org_id
        self.ip_address = ip_address
        self.user_agent = user_agent

        # Inisialisasi atau perbarui AuditTrailService dengan konteks terbaru
        self.audit_trail = AuditTrailService(
            user_id=self.user_id,
            org_id=self.org_id,
            ip_address=self.ip_address,
            user_agent=self.user_agent
        )

    def create(self, data: ApiCredential):
        """
        Insert a new api credential into the collection.
        """
        with mongodb.MongoConn() as mongo:
            collection = mongo.get_database()[self.collection_name]

            obj = data.model_dump()
            obj["_id"] = generate_uuid()
            obj["rec_by"] = self.user_id
            obj["rec_date"] = datetime.now(timezone.utc)
            obj["org_id"] = self.org_id
            try:
                obj["client_id"] = f"client_pub_{secrets.token_urlsafe(32)}"
                plain_text_secret = f"client_sec_{secrets.token_urlsafe(48)}"
                hashed_secret = hash_password(plain_text_secret)
                obj["client_secret_hash"] = hashed_secret
                result = collection.insert_one(obj)
                return {
                    "id": str(result.inserted_id),
                    "key_name": obj["key_name"],
                    "client_id": obj["client_id"],
                    "client_secret": plain_text_secret,
                    "status": obj["status"]
                }
            except PyMongoError as pme:
                logger.error(f"Database error occurred: {str(pme)}")
                raise ValueError("Database error occurred while creating document.") from pme
            except Exception as e:
                logger.exception(f"Unexpected error occurred while creating document: {str(e)}")
                raise

    def create_by_owner(self, data: ApiCredentialCreate):
        """
        Insert a new api credential into the collection.
        """
        
        with mongodb.MongoConn() as mongo:
            collection = mongo.get_database()[self.collection_name]

            obj = data.model_dump()
            obj["_id"] = generate_uuid()
            obj["rec_by"] = self.user_id
            obj["rec_date"] = datetime.now(timezone.utc)
            try:
                obj["client_id"] = f"client_pub_{secrets.token_urlsafe(32)}"
                plain_text_secret = f"client_sec_{secrets.token_urlsafe(48)}"
                hashed_secret = hash_password(plain_text_secret)
                obj["client_secret_hash"] = hashed_secret
                result = collection.insert_one(obj)
                return {
                    "id": str(result.inserted_id),
                    "org_id": obj["org_id"],
                    "key_name": obj["key_name"],
                    "client_id": obj["client_id"],
                    "client_secret": plain_text_secret,
                    "status": obj["status"]
                }
            except PyMongoError as pme:
                logger.error(f"Database error occurred: {str(pme)}")
                raise ValueError("Database error occurred while creating document.") from pme
            except Exception as e:
                logger.exception(f"Unexpected error occurred while creating document: {str(e)}")
                raise

    def get_by_id(self, api_cred_id: str):
        """
        Retrieve a api credential by ID.
        """
        
        with mongodb.MongoConn() as mongo:
            collection = mongo.get_database()[self.collection_name]
            try:
                credential = collection.find_one({"_id": api_cred_id})
                if not credential:
                    # write audit trail for fail
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="retrieve",
                        target=self.collection_name,
                        target_id=api_cred_id,
                        details={"_id": api_cred_id},
                        status="failure",
                        error_message="API Credential not found"
                    )
                    raise ValueError("API Credential not found")
                # write audit trail for success
                self.audit_trail.log_audittrail(
                    mongo,
                    action="retrieve",
                    target=self.collection_name,
                    target_id=api_cred_id,
                    details={"_id": api_cred_id, "retrieved_data": credential},
                    status="success"
                )
                return credential
            except PyMongoError as pme:
                logger.error(f"Database error occurred: {str(pme)}")
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    mongo,
                    action="retrieve",
                    target=self.collection_name,
                    target_id=api_cred_id,
                    details={"_id": api_cred_id},
                    status="failure",
                    error_message=str(pme)
                )
                raise ValueError("Database error occurred while find document.") from pme
            except Exception as e:
                logger.exception(f"Unexpected error occurred while finding document: {str(e)}")
                raise

    def update_by_id(self, api_cred_id: str, data):
        """
        Update a api credential's data by ID.
        """
        with mongodb.MongoConn() as mongo:
            collection = mongo.get_database()[self.collection_name]
            obj = data.model_dump()
            obj["mod_by"] = self.user_id
            obj["mod_date"] = datetime.now(timezone.utc)
            try:
                update_api_credential = collection.find_one_and_update({"_id": api_cred_id}, {"$set": obj}, return_document=True)
                if not update_api_credential:
                    # write audit trail for fail
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="update",
                        target=self.collection_name,
                        target_id=api_cred_id,
                        details={"$set": obj},
                        status="failure",
                        error_message="API Credential not found"
                    )
                    raise ValueError("API Credential not found")
                # write audit trail for success
                self.audit_trail.log_audittrail(
                    mongo,
                    action="update",
                    target=self.collection_name,
                    target_id=api_cred_id,
                    details={"$set": obj},
                    status="success"
                )
                return {
                    "id": str(update_api_credential["_id"]),
                    "key_name": update_api_credential["key_name"],
                    "client_id": update_api_credential["client_id"],
                    "status": update_api_credential["status"]
                }
            except PyMongoError as pme:
                logger.error(f"Database error occurred: {str(pme)}")
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    mongo,
                    action="update",
                    target=self.collection_name,
                    target_id=api_cred_id,
                    details={"$set": obj},
                    status="failure",
                    error_message=str(pme)
                )
                raise ValueError("Database error occurred while update document.") from pme
            except Exception as e:
                logger.exception(f"Error updating api credential: {str(e)}")
                raise

    def get_all(self, filters: Optional[Dict[str, Any]] = None, page: int = 1, per_page: int = 10, sort_field: str = "_id", sort_order: str = "asc"):
        """
        Retrieve all documents from the collection with optional filters, pagination, and sorting.
        """
        with mongodb.MongoConn() as mongo:
            collection = mongo.get_database()[self.collection_name]
            try:
                # Apply filters
                query_filter = filters or {}

                # Pagination
                skip = (page - 1) * per_page
                limit = per_page

                # Sorting
                order = ASCENDING if sort_order == "asc" else DESCENDING

                # Selected fields
                selected_fields = {
                    "id": "$_id",
                    "key_name": 1,
                    "client_id": 1,
                    "status": 1,
                    "_id": 0
                }

                # Aggregation pipeline
                pipeline = [
                    {"$match": query_filter},  # Filter stage
                    {"$sort": {sort_field: order}},  # Sorting stage
                    {"$skip": skip},  # Pagination skip stage
                    {"$limit": limit},  # Pagination limit stage
                    {"$project": selected_fields}  # Project only selected fields
                ]

                # Execute aggregation pipeline
                cursor = collection.aggregate(pipeline)
                results = list(cursor)

                # Total count
                total_count = collection.count_documents(query_filter)

                # write audit trail for success
                self.audit_trail.log_audittrail(
                    mongo,
                    action="retrieve",
                    target=self.collection_name,
                    target_id="agregate",
                    details={"aggregate": pipeline},
                    status="success"
                )

                return {
                    "data": results,
                    "pagination": {
                        "current_page": page,
                        "items_per_page": per_page,
                        "total_items": total_count,
                        "total_pages": (total_count + per_page - 1) // per_page,  # Ceiling division
                    },
                }
            except PyMongoError as pme:
                logger.error(f"Error retrieving api credential with filters and pagination: {str(e)}")
                # write audit trail for success
                self.audit_trail.log_audittrail(
                    mongo,
                    action="retrieve",
                    target=self.collection_name,
                    target_id="agregate",
                    details={"aggregate": pipeline},
                    status="failure"
                )
                raise ValueError("Database error while retrieve document") from pme
            except Exception as e:
                logger.exception(f"Unexpected error during deletion: {str(e)}")
                raise