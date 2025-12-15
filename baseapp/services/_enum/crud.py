from pymongo.errors import PyMongoError, DuplicateKeyError
from typing import Optional, Dict, Any
from pymongo import ASCENDING, DESCENDING
from datetime import datetime, timezone
from enum import Enum

from baseapp.utils.utility import generate_uuid
from baseapp.config import setting, mongodb
from baseapp.utils.logger import Logger
from baseapp.services._enum import model
from baseapp.services.audit_trail_service import AuditTrailService
from baseapp.model import common

model_map = {
    "common": common
}

config = setting.get_settings()
logger = Logger("baseapp.services._enum.crud")

class CRUD:
    def __init__(self, collection_name="_enum"):
        self.collection_name = collection_name

    def __enter__(self):
        self._mongo_context = mongodb.MongoConn()
        self.mongo = self._mongo_context.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if hasattr(self, '_mongo_context'):
            return self._mongo_context.__exit__(exc_type, exc_value, traceback)
        return False
    
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
        collection = self.mongo.get_database()[self.collection_name]
        obj = data.model_dump()
        obj["_id"] = data.id or generate_uuid()
        obj["rec_by"] = self.user_id
        obj["rec_date"] = datetime.now(timezone.utc)
        obj["org_id"] = self.org_id
        del obj["id"]
        try:
            result = collection.insert_one(obj)
            return obj
        except DuplicateKeyError as dke:
            logger.error(f"Duplicate entry detected: {str(dke)}")
            raise ValueError("A document with the same ID already exists.") from dke
        except PyMongoError as pme:
            logger.error(f"Database error occurred: {str(pme)}")
            raise ValueError("Database error occurred while creating document.") from pme
        except Exception as e:
            logger.exception(f"Unexpected error occurred while creating document: {str(e)}")
            raise

    def get_by_id(self, enum_id: str):
        """
        Retrieve a enum by ID.
        """
        collection = self.mongo.get_database()[self.collection_name]
        try:
            enum = collection.find_one({"_id": enum_id})
            if not enum:
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    self.mongo,
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
                self.mongo,
                action="retrieve",
                target=self.collection_name,
                target_id=enum_id,
                details={"_id": enum_id, "retrieved_enum": enum},
                status="success"
            )
            return enum
        except PyMongoError as pme:
            logger.error(f"Database error occurred: {str(pme)}")
            # write audit trail for fail
            self.audit_trail.log_audittrail(
                self.mongo,
                action="retrieve",
                target=self.collection_name,
                target_id=enum_id,
                details={"_id": enum_id},
                status="failure",
                error_message=str(pme)
            )
            raise ValueError("Database error occurred while find document.") from pme
        except Exception as e:
            logger.exception(f"Unexpected error occurred while finding document: {str(e)}")
            raise
    
    def get_from_model(self, model_from, model_name: str):
        """
        Retrieve a enum by ID.
        """
        try:
            module = model_map[model_from]
            enum_class = getattr(module, model_name)
            if not (isinstance(enum_class, type) and issubclass(enum_class, Enum)):
                raise ValueError(f"{model_name} is not an Enum class")
            enum_data = {}
            for item in enum_class:
                # Cek apakah enum member memiliki property 'label' (seperti pada LanguageCode)
                # hasattr akan mentrigger @property jika ada
                if hasattr(item, "label"):
                    enum_data[item.name] = {
                        "value": item.value,
                        "label": item.label
                    }
                else:
                    # Fallback untuk Enum biasa (hanya return value)
                    enum_data[item.name] = item.value
                    
            return enum_data
        except AttributeError:
            error_message = f"Model {model_name} not found in model"
            logger.error(f"get_enum_from_model, Error: {error_message}")
            raise ValueError(error_message)
        except ValueError as err:
            error_message = f"Invalid enum class: {str(err)}"
            logger.error(
                f"get_enum_from_model, Error: {error_message}"
            )
            raise ValueError(error_message)
        except Exception as err:
            error_message = f"Unexpected error: {str(err)}"
            logger.error(
                f"get_enum_from_model, Error: {error_message}"
            )
            raise

    def update_by_id(self, enum_id: str, data: model.EnumUpdate):
        """
        Update a enum's data by ID.
        """
        collection = self.mongo.get_database()[self.collection_name]
        obj = data.model_dump()
        obj["mod_by"] = self.user_id
        obj["mod_date"] = datetime.now(timezone.utc)
        try:
            update_enum = collection.find_one_and_update({"_id": enum_id}, {"$set": obj}, return_document=True)
            if not update_enum:
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    self.mongo,
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
                self.mongo,
                action="update",
                target=self.collection_name,
                target_id=enum_id,
                details={"$set": obj},
                status="success"
            )
            return update_enum
        except PyMongoError as pme:
            logger.error(f"Database error occurred: {str(pme)}")
            # write audit trail for fail
            self.audit_trail.log_audittrail(
                self.mongo,
                action="update",
                target=self.collection_name,
                target_id=enum_id,
                details={"$set": obj},
                status="failure",
                error_message=str(pme)
            )
            raise ValueError("Database error occurred while update document.") from pme
        except Exception as e:
            logger.exception(f"Error updating enum: {str(e)}")
            raise

    def delete_by_id(self, enum_id: str):
        """
        Delete a enum by ID.
        """
        collection = self.mongo.get_database()[self.collection_name]
        try:
            result = collection.delete_one({"_id": enum_id})
            if result.deleted_count == 0:
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    self.mongo,
                    action="delete",
                    target=self.collection_name,
                    target_id=enum_id,
                    status="failure",
                    error_message="No matching document found to delete."
                )
                raise ValueError("No matching document found to delete.")
            # write audit trail for success
            self.audit_trail.log_audittrail(
                self.mongo,
                action="delete",
                target=self.collection_name,
                target_id=enum_id,
                status="success"
            )
            return result.deleted_count
        except PyMongoError as pme:
            logger.error(f"Database error while deleting document with ID {enum_id}: {str(pme)}")
            # write audit trail for fail
            self.audit_trail.log_audittrail(
                self.mongo,
                action="delete",
                target=self.collection_name,
                target_id=enum_id,
                status="failure",
                error_message=str(pme)
            )
            raise ValueError("Database error while deleting document") from pme
        except Exception as e:
            logger.exception(f"Unexpected error during deletion: {str(e)}")
            raise
            
    def get_all(self, filters: Optional[Dict[str, Any]] = None, page: int = 1, per_page: int = 10, sort_field: str = "_id", sort_order: str = "asc"):
        """
        Retrieve all documents from the collection with optional filters, pagination, and sorting.
        """
        collection = self.mongo.get_database()[self.collection_name]
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
                self.mongo,
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
            logger.error(f"Error retrieving enum with filters and pagination: {str(e)}")
            # write audit trail for success
            self.audit_trail.log_audittrail(
                self.mongo,
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