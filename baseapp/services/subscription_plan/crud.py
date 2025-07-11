import logging,uuid

from pymongo.errors import PyMongoError
from typing import Optional, Dict, Any
from pymongo import ASCENDING, DESCENDING
from datetime import datetime, timezone

from baseapp.config import setting, mongodb
from baseapp.services.subscription_plan.model import SubscriptionPlan
from baseapp.services.audit_trail_service import AuditTrailService

config = setting.get_settings()

class CRUD:
    def __init__(self, collection_name="subscription_plan"):
        self.collection_name = collection_name
        self.logger = logging.getLogger()
        self.audit_trail = None

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

    def create(self, data: SubscriptionPlan):
        """
        Insert a new sub plan into the collection.
        """
        client = mongodb.MongoConn()
        with client as mongo:
            collection = mongo._db[self.collection_name]

            obj = data.model_dump()
            obj["_id"] = str(uuid.uuid4())
            obj["rec_by"] = self.user_id
            try:
                result = collection.insert_one(obj)
                return obj
            except PyMongoError as pme:
                self.logger.error(f"Database error occurred: {str(pme)}")
                raise ValueError("Database error occurred while creating document.") from pme
            except Exception as e:
                self.logger.exception(f"Unexpected error occurred while creating document: {str(e)}")
                raise

    def get_by_id(self, subplan_id: str):
        """
        Retrieve a sub plan by ID.
        """
        client = mongodb.MongoConn()
        with client as mongo:
            collection = mongo._db[self.collection_name]
            try:
                subplan = collection.find_one({"_id": subplan_id})
                if not subplan:
                    # write audit trail for fail
                    if self.audit_trail:
                        self.audit_trail.log_audittrail(
                            mongo,
                            action="retrieve",
                            target=self.collection_name,
                            target_id=subplan_id,
                            details={"_id": subplan_id},
                            status="failure",
                            error_message="Subscription plan not found"
                        )
                    raise ValueError("Role not found")
                # write audit trail for success
                if self.audit_trail:
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="retrieve",
                        target=self.collection_name,
                        target_id=subplan_id,
                        details={"_id": subplan_id, "retrieved_data": subplan},
                        status="success"
                    )
                return subplan
            except PyMongoError as pme:
                self.logger.error(f"Database error occurred: {str(pme)}")
                # write audit trail for fail
                if self.audit_trail:
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="retrieve",
                        target=self.collection_name,
                        target_id=subplan_id,
                        details={"_id": subplan_id},
                        status="failure",
                        error_message=str(pme)
                    )
                raise ValueError("Database error occurred while find document.") from pme
            except Exception as e:
                self.logger.exception(f"Unexpected error occurred while finding document: {str(e)}")
                raise

    def update_by_id(self, subplan_id: str, data):
        """
        Update a sub plan's data by ID.
        """
        client = mongodb.MongoConn()
        with client as mongo:
            collection = mongo._db[self.collection_name]
            obj = data.model_dump()
            obj["mod_by"] = self.user_id
            obj["mod_date"] = datetime.now(timezone.utc)
            try:
                update_sub_plan = collection.find_one_and_update({"_id": subplan_id}, {"$set": obj}, return_document=True)
                if not update_sub_plan:
                    # write audit trail for fail
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="update",
                        target=self.collection_name,
                        target_id=subplan_id,
                        details={"$set": obj},
                        status="failure",
                        error_message="Role not found"
                    )
                    raise ValueError("Role not found")
                # write audit trail for success
                self.audit_trail.log_audittrail(
                    mongo,
                    action="update",
                    target=self.collection_name,
                    target_id=subplan_id,
                    details={"$set": obj},
                    status="success"
                )
                return update_sub_plan
            except PyMongoError as pme:
                self.logger.error(f"Database error occurred: {str(pme)}")
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    mongo,
                    action="update",
                    target=self.collection_name,
                    target_id=subplan_id,
                    details={"$set": obj},
                    status="failure",
                    error_message=str(pme)
                )
                raise ValueError("Database error occurred while update document.") from pme
            except Exception as e:
                self.logger.exception(f"Error updating role: {str(e)}")
                raise

    def get_all(self, filters: Optional[Dict[str, Any]] = None, page: int = 1, per_page: int = 10, sort_field: str = "sort", sort_order: str = "asc"):
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

                # Selected fields
                selected_fields = {
                    "id": "$_id",
                    "name": 1,
                    "tier": 1,
                    "max_user": 1,
                    "price": 1,
                    "sort": 1,
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
                if self.audit_trail:
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
                self.logger.error(f"Error retrieving role with filters and pagination: {str(e)}")
                # write audit trail for success
                if self.audit_trail:
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
