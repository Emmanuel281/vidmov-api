import logging
from pymongo.errors import PyMongoError, DuplicateKeyError
from typing import Optional, Dict, Any
from pymongo import ASCENDING, DESCENDING
from datetime import datetime, timezone

from baseapp.model.common import Status, UpdateStatus
from baseapp.config import setting, mongodb
from baseapp.services._user.model import User, UpdateUsername, UpdateEmail, UpdateRoles, UpdateByAdmin, ChangePassword, ResetPassword

from baseapp.services.audit_trail_service import AuditTrailService

from baseapp.utils.utility import hash_password, is_none, generate_password, generate_uuid, check_password

config = setting.get_settings()
logger = logging.getLogger(__name__)

class CRUD:
    def __init__(self, collection_name="_user"):
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

    def create(self, data: User):
        """
        Insert a new user into the collection.
        """
        with mongodb.MongoConn() as mongo:
            collection = mongo.get_database()[self.collection_name]

            obj = data.model_dump()
            obj["_id"] = generate_uuid()
            obj["rec_by"] = self.user_id
            obj["rec_date"] = datetime.now(timezone.utc)
            obj["org_id"] = self.org_id

            # Generate hash password
            hashed_password = hash_password(data.password)
            obj["password"] = hashed_password
            try:
                result = collection.insert_one(obj)
                del obj["password"]
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

    def get_by_id(self, user_id: str):
        """
        Retrieve a user by ID.
        """
        with mongodb.MongoConn() as mongo:
            collection = mongo.get_database()[self.collection_name]
            try:
                # Apply filters
                query_filter = {"_id": user_id}

                # Selected field
                selected_fields={
                    "id": "$_id",
                    "username":1,
                    "email":1,
                    "roles":1,
                    "status":1,
                    "org_id":1,
                    "org_data":1,
                    "google":1,
                    "_id": 0
                }

                # Aggregation pipeline
                pipeline = [
                    {"$match": query_filter},  # Filter stage
                    # Lookup stage to join with org data
                    {
                        "$lookup": {
                            "from": "_organization",  # The collection to join with
                            "localField": "org_id",  # Array field in users collection
                            "foreignField": "_id",  # Field in role_groups collection
                            "as": "org_data"  # Output array field
                        }
                    },
                    {
                        "$addFields": {
                            "org_data": {
                                "$let": {
                                    "vars": {
                                        "firstOrg": {"$arrayElemAt": ["$org_data", 0]}
                                    },
                                    "in": {
                                        "$cond": [
                                            {"$gt": [{"$size": "$org_data"}, 0]},
                                            {
                                                "id": "$$firstOrg._id",
                                                "name": "$$firstOrg.org_name",
                                                "initial": "$$firstOrg.org_initial"
                                            },
                                            None
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    # Lookup stage to join with role groups
                    {
                        "$lookup": {
                            "from": "_role",  # The collection to join with
                            "localField": "roles",  # Array field in users collection
                            "foreignField": "_id",  # Field in role_groups collection
                            "as": "role_details"  # Output array field
                        }
                    },
                    {
                        "$addFields": {
                            "role_details": {
                                "$map": {
                                    "input": "$role_details",
                                    "as": "role",
                                    "in": {
                                        "id": "$$role._id",
                                        "name": "$$role.name",
                                        "color": "$$role.color",
                                        "status": "$$role.status"
                                    }
                                }
                            }
                        }
                    },
                    {"$project": selected_fields}  # Project only selected fields
                ]

                # Execute aggregation pipeline
                cursor = collection.aggregate(pipeline)
                results = list(cursor)

                if len(results) > 0:
                    user_data = results[0]  # Get the first (and only) document
                else:
                    # write audit trail for fail
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="retrieve",
                        target=self.collection_name,
                        target_id=user_id,
                        details={"_id": user_id},
                        status="failure",
                        error_message="User not found"
                    )
                    raise ValueError("User not found")

                # write audit trail for success
                self.audit_trail.log_audittrail(
                    mongo,
                    action="retrieve",
                    target=self.collection_name,
                    target_id=user_id,
                    details={"_id": user_id, "retrieved_user": user_data},
                    status="success"
                )

                return user_data
            except PyMongoError as pme:
                logger.error(f"Database error occurred: {str(pme)}")
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    mongo,
                    action="retrieve",
                    target=self.collection_name,
                    target_id=user_id,
                    details={"_id": user_id},
                    status="failure",
                    error_message=str(pme)
                )
                raise ValueError("Database error occurred while find document.") from pme
            except Exception as e:
                logger.exception(f"Unexpected error occurred while finding document: {str(e)}")
                raise

    def update_all_by_admin(self, user_id: str, data: UpdateByAdmin):
        """
        Update a user's data [username,email,roles,status] by ID.
        """
        with mongodb.MongoConn() as mongo:
            collection = mongo.get_database()[self.collection_name]
            obj = data.model_dump()
            obj["mod_by"] = self.user_id
            obj["mod_date"] = datetime.now(timezone.utc)
            logger.debug(f"update data user: {obj}")
            try:
                update_user = collection.find_one_and_update({"_id": user_id}, {"$set": obj}, return_document=True)
                if not update_user:
                    # write audit trail for fail
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="update",
                        target=self.collection_name,
                        target_id=user_id,
                        details={"$set": obj},
                        status="failure",
                        error_message="User not found"
                    )
                    raise ValueError("User not found")
                # write audit trail for success
                self.audit_trail.log_audittrail(
                    mongo,
                    action="update",
                    target=self.collection_name,
                    target_id=user_id,
                    details={"$set": obj},
                    status="success"
                )
                return update_user
            except PyMongoError as pme:
                logger.error(f"Database error occurred: {str(pme)}")
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    mongo,
                    action="update",
                    target=self.collection_name,
                    target_id=user_id,
                    details={"$set": obj},
                    status="failure",
                    error_message=str(pme)
                )
                raise ValueError("Database error occurred while update document.") from pme
            except Exception as e:
                logger.exception(f"Error updating username: {str(e)}")
                raise

    def update_username(self, user_id: str, data: UpdateUsername):
        """
        Update a user's data [username] by ID.
        """
        with mongodb.MongoConn() as mongo:
            collection = mongo.get_database()[self.collection_name]
            obj = data.model_dump()
            obj["mod_by"] = self.user_id
            obj["mod_date"] = datetime.now(timezone.utc)
            try:
                update_user = collection.find_one_and_update({"_id": user_id}, {"$set": obj}, return_document=True)
                if not update_user:
                    # write audit trail for fail
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="update",
                        target=self.collection_name,
                        target_id=user_id,
                        details={"$set": obj},
                        status="failure",
                        error_message="User not found"
                    )
                    raise ValueError("User not found")
                # write audit trail for success
                self.audit_trail.log_audittrail(
                    mongo,
                    action="update",
                    target=self.collection_name,
                    target_id=user_id,
                    details={"$set": obj},
                    status="success"
                )
                return update_user
            except PyMongoError as pme:
                logger.error(f"Database error occurred: {str(pme)}")
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    mongo,
                    action="update",
                    target=self.collection_name,
                    target_id=user_id,
                    details={"$set": obj},
                    status="failure",
                    error_message=str(pme)
                )
                raise ValueError("Database error occurred while update document.") from pme
            except Exception as e:
                logger.exception(f"Error updating username: {str(e)}")
                raise
    
    def update_email(self, user_id: str, data: UpdateEmail):
        """
        Update a user's data [email] by ID.
        """
        with mongodb.MongoConn() as mongo:
            collection = mongo.get_database()[self.collection_name]
            obj = data.model_dump()
            obj["mod_by"] = self.user_id
            obj["mod_date"] = datetime.now(timezone.utc)
            try:
                update_user = collection.find_one_and_update({"_id": user_id}, {"$set": obj}, return_document=True)
                if not update_user:
                    # write audit trail for fail
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="update",
                        target=self.collection_name,
                        target_id=user_id,
                        details={"$set": obj},
                        status="failure",
                        error_message="User not found"
                    )
                    raise ValueError("User not found")
                # write audit trail for success
                self.audit_trail.log_audittrail(
                    mongo,
                    action="update",
                    target=self.collection_name,
                    target_id=user_id,
                    details={"$set": obj},
                    status="success"
                )
                return update_user
            except PyMongoError as pme:
                logger.error(f"Database error occurred: {str(pme)}")
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    mongo,
                    action="update",
                    target=self.collection_name,
                    target_id=user_id,
                    details={"$set": obj},
                    status="failure",
                    error_message=str(pme)
                )
                raise ValueError("Database error occurred while update document.") from pme
            except Exception as e:
                logger.exception(f"Error updating email: {str(e)}")
                raise
    
    def update_role(self, user_id: str, data: UpdateRoles):
        """
        Update a user's data [roles] by ID.
        """
        with mongodb.MongoConn() as mongo:
            collection = mongo.get_database()[self.collection_name]
            obj = data.model_dump()
            obj["mod_by"] = self.user_id
            obj["mod_date"] = datetime.now(timezone.utc)
            try:
                update_user = collection.find_one_and_update({"_id": user_id}, {"$set": obj}, return_document=True)
                if not update_user:
                    # write audit trail for fail
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="update",
                        target=self.collection_name,
                        target_id=user_id,
                        details={"$set": obj},
                        status="failure",
                        error_message="User not found"
                    )
                    raise ValueError("User not found")
                logger.info(f"User {user_id} roles updated.")
                # write audit trail for success
                self.audit_trail.log_audittrail(
                    mongo,
                    action="update",
                    target=self.collection_name,
                    target_id=user_id,
                    details={"$set": obj},
                    status="success"
                )
                return update_user
            except PyMongoError as pme:
                logger.error(f"Database error occurred: {str(pme)}")
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    mongo,
                    action="update",
                    target=self.collection_name,
                    target_id=user_id,
                    details={"$set": obj},
                    status="failure",
                    error_message=str(pme)
                )
                raise ValueError("Database error occurred while update document.") from pme
            except Exception as e:
                logger.exception(f"Error updating roles: {str(e)}")
                raise
    
    def update_status(self, user_id: str, data: UpdateStatus):
        """
        Update a user's data [status] by ID.
        """
        with mongodb.MongoConn() as mongo:
            collection = mongo.get_database()[self.collection_name]
            obj = data.model_dump()
            obj["mod_by"] = self.user_id
            obj["mod_date"] = datetime.now(timezone.utc)
            try:
                update_user = collection.find_one_and_update({"_id": user_id}, {"$set": obj}, return_document=True)
                if not update_user:
                    # write audit trail for fail
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="update",
                        target=self.collection_name,
                        target_id=user_id,
                        details={"$set": obj},
                        status="failure",
                        error_message="User not found"
                    )
                    raise ValueError("User not found")
                logger.info(f"User {user_id} status updated.")
                # write audit trail for success
                self.audit_trail.log_audittrail(
                    mongo,
                    action="update",
                    target=self.collection_name,
                    target_id=user_id,
                    details={"$set": obj},
                    status="success"
                )
                return update_user
            except PyMongoError as pme:
                logger.error(f"Database error occurred: {str(pme)}")
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    mongo,
                    action="update",
                    target=self.collection_name,
                    target_id=user_id,
                    details={"$set": obj},
                    status="failure",
                    error_message=str(pme)
                )
                raise ValueError("Database error occurred while update document.") from pme
            except Exception as e:
                logger.exception(f"Error updating status: {str(e)}")
                raise
    
    def _validate_user(self,mongo,old_password):
        collection = mongo.get_database()[self.collection_name]
        query = {"_id": self.user_id}
        user_info = collection.find_one(query)
        if not user_info:
            logger.warning(f"User with ID'{self.user_id}' not found.")
            raise ValueError("User not found")

        if user_info.get("status") != Status.ACTIVE.value:
            logger.warning(f"User {user_info.get('username')} is not active.")
            raise ValueError("User is not active.")
        
        stored_hash = user_info.get("password")
        if not stored_hash:
            logger.error(f"Password missing for user {user_info.get('username')}.")
            raise ValueError("User data is invalid.")
        
        if not check_password(old_password, stored_hash):
            logger.warning(f"User {user_info.get('username')} provided invalid password.")
            raise ValueError("Invalid old password.")
        
        return user_info
    
    def change_password(self, data: ChangePassword):
        """
        Change password
        """
        if data.new_password != data.verify_password:
            raise ValueError("New password is not match with verify password.")
        
        with mongodb.MongoConn() as mongo:
            collection = mongo.get_database()[self.collection_name]
            try:
                validate_old_password = self._validate_user(mongo,data.old_password)

                password = is_none(data.new_password, generate_password())
                hashed_password = hash_password(password)

                obj = {}
                obj["password"] = hashed_password
                obj["mod_by"] = self.user_id
                obj["mod_date"] = datetime.now(timezone.utc)

                update_user = collection.find_one_and_update({"_id": self.user_id}, {"$set": obj}, return_document=True)
                if not update_user:
                    # write audit trail for fail
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="change_password",
                        target=self.collection_name,
                        target_id=self.user_id,
                        status="failure",
                        error_message="Change password failed"
                    )
                    raise ValueError("Change password failed")
                # write audit trail for success
                self.audit_trail.log_audittrail(
                    mongo,
                    action="change_password",
                    target=self.collection_name,
                    target_id=self.user_id,
                    status="success"
                )
                return {"id":update_user["_id"],"username":update_user["username"],"email":update_user["email"]}
            except PyMongoError as pme:
                logger.error(f"Database error occurred: {str(pme)}")
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    mongo,
                    action="change_password",
                    target=self.collection_name,
                    target_id=self.user_id,
                    status="failure",
                    error_message=str(pme)
                )
                raise ValueError("Database error occurred while update document.") from pme
            except Exception as e:
                logger.exception(f"Error updating status: {str(e)}")
                raise

    def reset_password(self, user_id:str , data: ResetPassword):
        """
        Reset password
        """
        if data.new_password != data.verify_password:
            raise ValueError("New password is not match with verify password.")
        
        with mongodb.MongoConn() as mongo:
            collection = mongo.get_database()[self.collection_name]
            try:
                password = is_none(data.new_password, generate_password())
                hashed_password = hash_password(password)

                obj = {}
                obj["password"] = hashed_password
                obj["mod_by"] = self.user_id
                obj["mod_date"] = datetime.now(timezone.utc)

                update_user = collection.find_one_and_update({"_id": user_id}, {"$set": obj}, return_document=True)
                if not update_user:
                    # write audit trail for fail
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="reset_password",
                        target=self.collection_name,
                        target_id=user_id,
                        status="failure",
                        error_message="Reset password failed"
                    )
                    raise ValueError("Reset password failed")
                # write audit trail for success
                self.audit_trail.log_audittrail(
                    mongo,
                    action="reset_password",
                    target=self.collection_name,
                    target_id=self.user_id,
                    status="success"
                )
                return {"id":update_user["_id"],"username":update_user["username"],"email":update_user["email"]}
            except PyMongoError as pme:
                logger.error(f"Database error occurred: {str(pme)}")
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    mongo,
                    action="reset_password",
                    target=self.collection_name,
                    target_id=self.user_id,
                    status="failure",
                    error_message={str(pme)}
                )
                raise ValueError("Database error occurred while update document.") from pme
            except Exception as e:
                logger.exception(f"Error updating status: {str(e)}")
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

                # Handle role filter specifically
                if 'roles' in query_filter:
                    # Jika roles adalah string, konversi ke format $in
                    if isinstance(query_filter['roles'], str):
                        query_filter['roles'] = {"$in": [query_filter['roles']]}
                    # Jika roles adalah list, gunakan $in
                    elif isinstance(query_filter['roles'], list):
                        query_filter['roles'] = {"$in": query_filter['roles']}

                # Pagination
                skip = (page - 1) * per_page
                limit = per_page

                # Sorting
                order = ASCENDING if sort_order == "asc" else DESCENDING

                # Selected field
                selected_fields={
                    "id": "$_id",
                    "username":1,
                    "email":1,
                    "roles":1,
                    "role_details":1,
                    "status":1,
                    "org_id":1,
                    "_id": 0
                }

                # Aggregation pipeline
                pipeline = [
                    {"$match": query_filter},  # Filter stage
                    {"$sort": {sort_field: order}},  # Sorting stage
                    # Lookup stage to join with role groups
                    {
                        "$lookup": {
                            "from": "_role",  # The collection to join with
                            "localField": "roles",  # Array field in users collection
                            "foreignField": "_id",  # Field in role_groups collection
                            "as": "role_details"  # Output array field
                        }
                    },
                    {
                        "$addFields": {
                            "role_details": {
                                "$map": {
                                    "input": "$role_details",
                                    "as": "role",
                                    "in": {
                                        "id": "$$role._id",
                                        "name": "$$role.name",
                                        "color": "$$role.color",
                                        "status": "$$role.status"
                                    }
                                }
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
                logger.error(f"Error retrieving user with filters and pagination: {str(e)}")
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