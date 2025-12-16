from datetime import datetime,timezone

from pymongo.errors import DuplicateKeyError, PyMongoError
from pymongo import ASCENDING, DESCENDING

from typing import Optional, Dict, Any

from baseapp.config import setting, mongodb
from baseapp.services._org import model
from baseapp.model.common import UpdateStatus, MINIO_STORAGE_SIZE_LIMIT, Authority, RoleAction
from baseapp.utils.utility import hash_password, get_enum, generate_uuid
from baseapp.services.audit_trail_service import AuditTrailService
from baseapp.utils.logger import Logger

config = setting.get_settings()
logger = Logger("baseapp.services._org.crud")

class CRUD:
    def __init__(self):
        self.collection_org = "_organization"
        self.collection_user = "_user"
        self.collection_role = "_role"
        self.storage = MINIO_STORAGE_SIZE_LIMIT
        self.usedstorage = 0

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

    def init_owner_org(self, org_data: model.Organization, user_data: model.User) -> model.InitResponse:
        """
        Insert a new owner into the collection.
        """
        
        collection = self.mongo.get_database()[self.collection_org]
        collection_user = self.mongo.get_database()[self.collection_user]

        org_data = org_data.model_dump()
        user_data = user_data.model_dump()

        org_data["_id"] = generate_uuid()
        org_data["rec_date"] = datetime.now(timezone.utc)
        org_data["storage"] = self.storage
        org_data["usedstorage"] = self.usedstorage
        try:
            # check owner is exist or not
            owner_is_exist = collection.find_one({"authority":Authority.OWNER.value})
            if owner_is_exist:
                raise ValueError("The owner already exists, and there is only one owner in the structure.")
            
            # check owner user is exist or not
            owner_user_is_exist = collection_user.find_one({"username":user_data["username"]})
            if owner_user_is_exist:
                raise ValueError("The owner user already exists, please fill other username or email.")
            
            # insert owner data to the table
            result = collection.insert_one(org_data)
            logger.info(f"Owner created with id: {result.inserted_id}")

            # insert owner role data to the table
            obj_role = model.Role(name="Admin",org_id=result.inserted_id,status="ACTIVE")
            init_role = self.init_role(org_data,role_data=obj_role)

            # insert user data to the table
            user_data["roles"] = [init_role["_id"]]
            init_user = self.init_user(org_data, user_data)

            org_data["id"] = org_data.pop("_id")
            init_user["id"] = init_user.pop("_id")
            org_response = model.OrganizationResponse(**org_data)
            user_response = model.UserResponse(**init_user)

            return model.InitResponse(org=org_response, user=user_response)
        except DuplicateKeyError:
            logger.error("Duplicate ID detected.")
            raise ValueError("the same ID already exists.")
        except PyMongoError as pme:
            logger.error(f"Database error occurred: {str(pme)}")
            raise ValueError("Database error occurred while init owner.") from pme
        except Exception as e:
            logger.exception(f"Unexpected error occurred while init owner: {e}")
            raise
    
    def init_partner_client_org(self, org_data: model.Organization, user_data: model.User) -> model.InitResponse:
        """
        Insert a new partner into the collection.
        """
        
        collection = self.mongo.get_database()[self.collection_org]
        collection_user = self.mongo.get_database()[self.collection_user]

        org_data = org_data.model_dump()
        user_data = user_data.model_dump()

        org_data["_id"] = generate_uuid()
        org_data["rec_by"] = self.user_id
        org_data["rec_date"] = datetime.now(timezone.utc)
        org_data["ref_id"] = self.org_id
        org_data["storage"] = self.storage
        org_data["usedstorage"] = self.usedstorage
        try:
            # check owner user is exist or not
            owner_user_is_exist = collection_user.find_one({"username":user_data["username"]})
            if owner_user_is_exist:
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    self.mongo,
                    action="init_partner_client",
                    target=self.collection_user,
                    target_id=None,
                    details={"username":user_data["username"]},
                    status="failure",
                    error_message="Role not found"
                )
                raise ValueError("The partner user already exists, please fill other username or email.")
            
            # insert owner data to the table
            result = collection.insert_one(org_data)
            logger.info(f"Partner created with id: {result.inserted_id}")

            # insert owner role data to the table
            obj_role = model.Role(name="Admin",org_id=result.inserted_id,status="ACTIVE")
            init_role = self.init_role(org_data,role_data=obj_role)

            # insert user data to the table
            user_data["roles"] = [init_role["_id"]]
            init_user = self.init_user(org_data, user_data)

            org_data["id"] = org_data.pop("_id")
            init_user["id"] = init_user.pop("_id")
            org_response = model.OrganizationResponse(**org_data)
            user_response = model.UserResponse(**init_user)

            return model.InitResponse(org=org_response, user=user_response)
        except DuplicateKeyError:
            logger.error("Duplicate ID detected.")
            # write audit trail for fail
            self.audit_trail.log_audittrail(
                self.mongo,
                action="init_partner_client",
                target=self.collection_org,
                target_id=None,
                details=org_data,
                status="failure",
                error_message="Duplicate ID"
            )
            raise ValueError("the same ID already exists.")
        except PyMongoError as pme:
            logger.error(f"Database error occurred: {str(pme)}")
            # write audit trail for fail
            self.audit_trail.log_audittrail(
                self.mongo,
                action="init_partner_client",
                target=self.collection_org,
                target_id=None,
                details=org_data,
                status="failure",
                error_message=str(pme)
            )
            raise ValueError("Database error occurred while init partner.") from pme
        except Exception as e:
            logger.exception(f"Unexpected error occurred while init partner: {e}")
            raise

    def init_role(self, org_data, role_data:model.Role):
        """
        Insert a new role into the collection.
        """
        collection = self.mongo.get_database()[self.collection_role]

        role_data = role_data.model_dump()

        role_data["_id"] = generate_uuid()
        role_data["rec_date"] = datetime.now(timezone.utc)
        role_data["mod_date"] = datetime.now(timezone.utc)
        role_data["org_id"] = org_data["_id"]

        try:
            result = collection.insert_one(role_data)
            logger.info(f"Role created with id: {result.inserted_id}")

            # trigger insert role on featuers
            self.init_role_in_feature(org_data,result.inserted_id)

            return role_data
        except PyMongoError as pme:
            logger.error(f"Database error occurred while init role of owner: {str(pme)}")
            # write audit trail for fail
            self.audit_trail.log_audittrail(
                self.mongo,
                action="init_partner_client:role",
                target=self.collection_role,
                target_id=None,
                details=role_data,
                status="failure",
                error_message=str(pme)
            )
            raise
        except Exception as e:
            logger.exception(f"Unexpected error occurred while init owner: {e}")
            raise

    def init_role_in_feature(self, org_data, role_id):
        """
        Generate role in feature into the collection.
        """
        collection = self.mongo.get_database()["_featureonrole"]
        collection_features = self.mongo.get_database()["_feature"]
        initial_data = []
        try:
            # get enum bit of roleaction
            bitRA = {item.name: item.value for item in RoleAction}
            totalBitRA = sum(bitRA.values())

            # list of features
            filters = {
                "authority": { "$bitsAnySet": org_data["authority"] }
            }
            get_features = collection_features.find(filters)
            for feature in get_features:
                initial_data.append({
                    "_id":generate_uuid(),
                    "org_id": org_data["_id"],
                    "r_id": role_id,
                    "f_id": feature["_id"],
                    "permission": totalBitRA-feature["negasiperm"][str(org_data['authority'])]
                })
            if len(initial_data) == 0:
                logger.warning("No features found matching the authority criteria.")
                raise ValueError("No features found matching the authority criteria.")
            collection.insert_many(initial_data, ordered=False)
            logger.info(f"Inserted {len(initial_data)} documents into _featureonrole")
            
            return initial_data
        except PyMongoError as pme:
            logger.error(f"Database error occurred while init role feature of owner.: {str(pme)}")
            # write audit trail for fail
            self.audit_trail.log_audittrail(
                self.mongo,
                action="init_partner_client:_featureonrole",
                target="_featureonrole",
                target_id=None,
                details=initial_data,
                status="failure",
                error_message=str(pme)
            )
            raise
        except Exception as e:
            logger.exception(f"Unexpected error occurred while init owner: {e}")
            raise

    def init_user(self, org_data, user_data):
        """
        Insert a new user into the collection.
        """
        collection = self.mongo.get_database()[self.collection_user]

        user_data["_id"] = generate_uuid()
        user_data["rec_date"] = datetime.now(timezone.utc)
        user_data["mod_date"] = datetime.now(timezone.utc)
        user_data["org_id"] = org_data["_id"]

        # Generate hash password
        hashed_password = hash_password(user_data["password"])
        user_data["password"] = hashed_password

        try:
            result = collection.insert_one(user_data)
            logger.info(f"User created with id: {result.inserted_id}")
            return user_data
        except PyMongoError as pme:
            logger.error(f"Database error occurred while init user.: {str(pme)}")
            # write audit trail for fail
            self.audit_trail.log_audittrail(
                self.mongo,
                action="init_partner_client:_user",
                target="_featureonrole",
                target_id=None,
                details=user_data,
                status="failure",
                error_message=str(pme)
            )
            raise
        except Exception as e:
            logger.exception(f"Unexpected error occurred while init user: {e}")
            raise

    def get_by_id(self, org_id: str) -> model.OrganizationResponse:
        """
        Retrieve a organization by ID.
        """
        collection = self.mongo.get_database()[self.collection_org]
        try:
            data = collection.find_one({"_id": org_id})
            if not data:
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    self.mongo,
                    action="retrieve",
                    target=self.collection_org,
                    target_id=org_id,
                    details={"_id": org_id},
                    status="failure",
                    error_message="Organization not found"
                )
                raise ValueError("Organization not found")
            # write audit trail for success
            self.audit_trail.log_audittrail(
                self.mongo,
                action="retrieve",
                target=self.collection_org,
                target_id=org_id,
                details={"_id": org_id, "retrieved": data},
                status="success"
            )
            data["id"] = data.pop("_id")
            return model.OrganizationResponse(**data)
        except PyMongoError as pme:
            logger.error(f"Database error occurred: {str(pme)}")
            # write audit trail for fail
            self.audit_trail.log_audittrail(
                self.mongo,
                action="retrieve",
                target=self.collection_org,
                target_id=org_id,
                details={"_id": org_id},
                status="failure",
                error_message=str(pme)
            )
            raise ValueError("Database error occurred while find document.") from pme
        except Exception as e:
            logger.exception(f"Unexpected error occurred while finding document: {str(e)}")
            raise

    def update_by_id(self, org_id: str, data: model.OrganizationUpdate) -> model.OrganizationResponse:
        """
        Update a organization's data by ID.
        """
        collection = self.mongo.get_database()[self.collection_org]
        obj = data.model_dump()
        obj["mod_by"] = self.user_id
        obj["mod_date"] = datetime.now(timezone.utc)
        try:
            update_data = collection.find_one_and_update({"_id": org_id}, {"$set": obj}, return_document=True)
            if not update_data:
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    self.mongo,
                    action="update",
                    target=self.collection_org,
                    target_id=org_id,
                    details={"$set": obj},
                    status="failure",
                    error_message="Organization not found"
                )
                raise ValueError("Organization not found")
            # write audit trail for success
            self.audit_trail.log_audittrail(
                self.mongo,
                action="update",
                target=self.collection_org,
                target_id=org_id,
                details={"$set": obj},
                status="success"
            )
            return model.OrganizationResponse(**update_data)
        except PyMongoError as pme:
            logger.error(f"Database error occurred: {str(pme)}")
            # write audit trail for fail
            self.audit_trail.log_audittrail(
                self.mongo,
                action="update",
                target=self.collection_org,
                target_id=org_id,
                details={"$set": obj},
                status="failure",
                error_message=str(pme)
            )
            raise ValueError("Database error occurred while update document.") from pme
        except Exception as e:
            logger.exception(f"Error updating role: {str(e)}")
            raise
    
    def update_status(self, org_id: str, data: UpdateStatus) -> model.OrganizationResponse:
        """
        Update a organization's data [status] by ID.
        """
        collection = self.mongo.get_database()[self.collection_org]
        collection_user = self.mongo.get_database()[self.collection_user]
        collection_role = self.mongo.get_database()[self.collection_role]
        obj = data.model_dump()
        obj["mod_by"] = self.user_id
        obj["mod_date"] = datetime.now(timezone.utc)
        try:
            update_org = collection.find_one_and_update({"_id": org_id}, {"$set": obj}, return_document=True)
            if not update_org:
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    self.mongo,
                    action="update",
                    target=self.collection_org,
                    target_id=org_id,
                    details={"$set": obj},
                    status="failure",
                    error_message="Organization not found"
                )
                raise ValueError("Organization not found")
            update_user = collection_user.find_one_and_update({"org_id": org_id}, {"$set": obj}, return_document=True)
            update_role = collection_role.find_one_and_update({"org_id": org_id}, {"$set": obj}, return_document=True)
            logger.info(f"Organization {org_id} status updated.")
            # write audit trail for success
            self.audit_trail.log_audittrail(
                self.mongo,
                action="update",
                target=self.collection_org,
                target_id=org_id,
                details={"$set": obj},
                status="success"
            )
            update_org["id"] = update_org.pop("_id")
            return model.OrganizationResponse(**update_org)
        except PyMongoError as pme:
            logger.error(f"Database error occurred: {str(pme)}")
            # write audit trail for fail
            self.audit_trail.log_audittrail(
                self.mongo,
                action="update",
                target=self.collection_org,
                target_id=org_id,
                details={"$set": obj},
                status="failure",
                error_message=str(pme)
            )
            raise ValueError("Database error occurred while update document.") from pme
        except Exception as e:
            logger.exception(f"Error updating status: {str(e)}")
            raise

    def get_all(self, filters: Optional[Dict[str, Any]] = None, page: int = 1, per_page: int = 10, sort_field: str = "_id", sort_order: str = "asc"):
        """
        Retrieve all documents from the collection with optional filters, pagination, and sorting.
        """
        collection = self.mongo.get_database()[self.collection_org]
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
                "org_name":1,
                "org_initial":1,
                "org_phone":1,
                "org_address":1,
                "org_desc":1,
                "org_email":1,
                "status":1,
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
            parsed_results = [model.OrganizationListItem(**item) for item in results]

            # Total count
            total_count = collection.count_documents(query_filter)

            # write audit trail for success
            self.audit_trail.log_audittrail(
                self.mongo,
                action="retrieve",
                target=self.collection_org,
                target_id="agregate",
                details={"aggregate": pipeline},
                status="success"
            )

            return {
                "data": parsed_results,
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
                self.mongo,
                action="retrieve",
                target=self.collection_org,
                target_id="agregate",
                details={"aggregate": pipeline},
                status="failure"
            )
            raise ValueError("Database error while retrieve document") from pme
        except Exception as e:
            logger.exception(f"Unexpected error during deletion: {str(e)}")
            raise
    
    def reg_member(self, org_data: model.Organization, user_data: model.User):
        """
        Insert a new partner/client into the collection.
        """
        collection = self.mongo.get_database()[self.collection_org]
        collection_user = self.mongo.get_database()[self.collection_user]

        org_data = org_data.model_dump()
        user_data = user_data.model_dump()

        org_data["_id"] = generate_uuid()
        org_data["rec_date"] = datetime.now(timezone.utc)
        org_data["storage"] = self.storage
        org_data["usedstorage"] = self.usedstorage

        try:
            # check organization is exist or not
            owner_user_is_exist = collection.find_one({"org_email":org_data["org_email"]})
            if owner_user_is_exist:
                raise ValueError("User already exists, please fill email.")
            
            # check owner user is exist or not
            owner_user_is_exist = collection_user.find_one({"username":user_data["username"]})
            if owner_user_is_exist:
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    self.mongo,
                    action="reg_member",
                    target=self.collection_user,
                    target_id=None,
                    details={"username":user_data["username"]},
                    status="failure",
                    error_message="Role not found"
                )
                raise ValueError("User already exists, please fill email.")
            
            # insert owner data to the table
            result = collection.insert_one(org_data)
            logger.info(f"Partner created with id: {result.inserted_id}")

            # insert owner role data to the table
            obj_role = model.Role(name="Admin",org_id=result.inserted_id,status="ACTIVE")
            init_role = self.init_role(org_data,role_data=obj_role)

            # insert user data to the table
            user_data["roles"] = [init_role["_id"]]
            user_data["balance_coin"] = 0
            init_user = self.init_user(org_data, user_data)
            
            org_data["id"] = org_data.pop("_id")
            init_user["id"] = init_user.pop("_id")
            org_response = model.OrganizationResponse(**org_data)
            user_response = model.UserResponse(**init_user)

            return model.InitResponse(org=org_response, user=user_response)
        except DuplicateKeyError:
            logger.error("Duplicate ID detected.")
            raise ValueError("the same ID already exists.")
        except PyMongoError as pme:
            logger.error(f"Database error occurred: {str(pme)}")
            raise ValueError("Database error occurred while init partner.") from pme
        except Exception as e:
            logger.exception(f"Unexpected error occurred while init partner: {e}")
            raise