import logging,uuid

from pymongo.errors import PyMongoError, DuplicateKeyError
from typing import Optional, Dict, Any
from pymongo import ASCENDING, DESCENDING
from datetime import datetime, timezone

from baseapp.config import setting, mongodb
from baseapp.services._enum import model
from baseapp.services.audit_trail_service import AuditTrailService

config = setting.get_settings()

class CRUD:
    def __init__(self, collection_name="_enum"):
        self.collection_name = collection_name
        self.logger = logging.getLogger()

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

    def create(self, data: model.Enum):
        """
        Insert a new enum into the collection.
        """
        client = mongodb.MongoConn()
        with client as mongo:
            collection = mongo._db[self.collection_name]

            obj = data.model_dump()
            obj["_id"] = data.id or str(uuid.uuid4())
            obj["rec_by"] = self.user_id
            obj["rec_date"] = datetime.now(timezone.utc)
            obj["org_id"] = self.org_id
            del obj["id"]
            try:
                result = collection.insert_one(obj)
                return obj
            except DuplicateKeyError as dke:
                self.logger.error(f"Duplicate entry detected: {str(dke)}")
                raise ValueError("A document with the same ID already exists.") from dke
            except PyMongoError as pme:
                self.logger.error(f"Database error occurred: {str(pme)}")
                raise ValueError("Database error occurred while creating document.") from pme
            except Exception as e:
                self.logger.exception(f"Unexpected error occurred while creating document: {str(e)}")
                raise

    def get_by_id(self, enum_id: str):
        """
        Retrieve a enum by ID.
        """
        client = mongodb.MongoConn()
        with client as mongo:
            collection = mongo._db[self.collection_name]
            try:
                enum = collection.find_one({"_id": enum_id})
                if not enum:
                    # write audit trail for fail
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="retrieve",
                        target=self.collection_name,
                        target_id=enum_id,
                        details={"_id": enum_id},
                        status="failure",
                        error_message="Enum not found"
                    )
                    raise ValueError("Enum not found")
                # write audit trail for success
                self.audit_trail.log_audittrail(
                    mongo,
                    action="retrieve",
                    target=self.collection_name,
                    target_id=enum_id,
                    details={"_id": enum_id, "retrieved_enum": enum},
                    status="success"
                )
                return enum
            except PyMongoError as pme:
                self.logger.error(f"Database error occurred: {str(pme)}")
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    mongo,
                    action="retrieve",
                    target=self.collection_name,
                    target_id=enum_id,
                    details={"_id": enum_id},
                    status="failure",
                    error_message=str(pme)
                )
                raise ValueError("Database error occurred while find document.") from pme
            except Exception as e:
                self.logger.exception(f"Unexpected error occurred while finding document: {str(e)}")
                raise

    def update_by_id(self, enum_id: str, data: model.EnumUpdate):
        """
        Update a enum's data by ID.
        """
        client = mongodb.MongoConn()
        with client as mongo:
            collection = mongo._db[self.collection_name]
            obj = data.model_dump()
            obj["mod_by"] = self.user_id
            obj["mod_date"] = datetime.now(timezone.utc)
            try:
                update_enum = collection.find_one_and_update({"_id": enum_id}, {"$set": obj}, return_document=True)
                if not update_enum:
                    # write audit trail for fail
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="update",
                        target=self.collection_name,
                        target_id=enum_id,
                        details={"$set": obj},
                        status="failure",
                        error_message="Enum not found"
                    )
                    raise ValueError("Enum not found")
                # write audit trail for success
                self.audit_trail.log_audittrail(
                    mongo,
                    action="update",
                    target=self.collection_name,
                    target_id=enum_id,
                    details={"$set": obj},
                    status="success"
                )
                return update_enum
            except PyMongoError as pme:
                self.logger.error(f"Database error occurred: {str(pme)}")
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    mongo,
                    action="update",
                    target=self.collection_name,
                    target_id=enum_id,
                    details={"$set": obj},
                    status="failure",
                    error_message=str(pme)
                )
                raise ValueError("Database error occurred while update document.") from pme
            except Exception as e:
                self.logger.exception(f"Error updating enum: {str(e)}")
                raise

    def delete_by_id(self, enum_id: str):
        """
        Delete a enum by ID.
        """
        client = mongodb.MongoConn()
        with client as mongo:
            collection = mongo._db[self.collection_name]
            try:
                result = collection.delete_one({"_id": enum_id})
                if result.deleted_count == 0:
                    # write audit trail for fail
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="delete",
                        target=self.collection_name,
                        target_id=enum_id,
                        status="failure",
                        error_message="No matching document found to delete."
                    )
                    raise ValueError("No matching document found to delete.")
                # write audit trail for success
                self.audit_trail.log_audittrail(
                    mongo,
                    action="delete",
                    target=self.collection_name,
                    target_id=enum_id,
                    status="success"
                )
                return result.deleted_count
            except PyMongoError as pme:
                self.logger.error(f"Database error while deleting document with ID {enum_id}: {str(pme)}")
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    mongo,
                    action="delete",
                    target=self.collection_name,
                    target_id=enum_id,
                    status="failure",
                    error_message=str(pme)
                )
                raise ValueError("Database error while deleting document") from pme
            except Exception as e:
                self.logger.exception(f"Unexpected error during deletion: {str(e)}")
                raise
            
    def get_all(self, filters: Optional[Dict[str, Any]] = None, page: int = 1, per_page: int = 10, sort_field: str = "_id", sort_order: str = "asc"):
        """
        Retrieve all documents from the collection with optional filters, pagination, and sorting.
        """
        client = mongodb.MongoConn()
        with client as mongo:
            collection = mongo._db[self.collection_name]
            try:
                # Apply filters
                query_filter = filters or {}

                # Pagination
                skip = (page - 1) * per_page
                limit = per_page

                # Sorting
                order = ASCENDING if sort_order == "asc" else DESCENDING

                # Selected field
                selected_fields={
                    "id": "$_id",
                    "app":1,
                    "mod":1,
                    "code":1,
                    "type":1,
                    "value":1,
                    "sort":1,
                    "parent_mod":1,
                    "_id": 0
                }
                
                self.logger.debug(f"Retrieving enums with filters: {query_filter}, page: {page}, per_page: {per_page}, sort_field: {sort_field}, sort_order: {sort_order}")
                # Aggregation pipeline
                pipeline = [
                    {"$match": query_filter},  # Filter stage
                    {"$sort": {sort_field: order}},  # Sorting stage
                    # Lookup to self for parent-child relationship
                    {
                        "$lookup": {
                            "from": self.collection_name,  # Lookup to the same collection
                            "let": {"mod_id": "$mod"},  # Define variable from current doc
                            "pipeline": [
                                {
                                    "$match": {
                                        "$expr": {
                                            "$eq": ["$_id", "$$mod_id"]  # Match where _id == current doc's mod
                                        }
                                    }
                                },
                                {
                                    "$project": {  # Only include needed fields
                                        "id": "$_id",
                                        "app": 1,
                                        "mod": 1,
                                        "code": 1,
                                        "type": 1,
                                        "value": 1,
                                        "sort": 1,
                                        "_id": 0
                                    }
                                }
                            ],
                            "as": "parent_mod"
                        }
                    },
                    {
                        "$addFields": {
                            "parent_mod": {
                                "$arrayElemAt": ["$parent_mod", 0]  # Convert array to single object
                            }
                        }
                    },
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
                self.logger.error(f"Error retrieving enum with filters and pagination: {str(e)}")
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
                self.logger.exception(f"Unexpected error during deletion: {str(e)}")
                raise
