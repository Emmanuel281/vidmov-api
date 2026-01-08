from pymongo.errors import PyMongoError
from minio.error import S3Error
from typing import Optional, Dict, Any
from pymongo import ASCENDING, DESCENDING
from datetime import datetime, timezone

from baseapp.utils.utility import generate_uuid
from baseapp.model.common import UpdateStatus
from baseapp.config import setting, mongodb, minio
from baseapp.services.brand.model import Brand, BrandCreateByOwner, BrandListResponse, BrandDetailResponse
from baseapp.services.audit_trail_service import AuditTrailService
from baseapp.utils.logger import Logger

config = setting.get_settings()
logger = Logger("baseapp.services.brand.crud")

class CRUD:
    def __init__(self, collection_name="brand"):
        self.collection_name = collection_name

    def __enter__(self):
        self._mongo_context = mongodb.MongoConn()
        self.mongo = self._mongo_context.__enter__()

        self._minio_context = minio.MinioConn()
        self.minio = self._minio_context.__enter__()
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

    def create(self, data: BrandCreateByOwner):
        """
        Insert a new brand into the collection.
        """
        collection = self.mongo.get_database()[self.collection_name]
        obj = data.model_dump()
        obj["_id"] = generate_uuid()
        obj["rec_by"] = self.user_id
        obj["rec_date"] = datetime.now(timezone.utc)
        try:
            result = collection.insert_one(obj)
            obj["id"] = obj.pop("_id")
            return BrandDetailResponse(**obj)
        except PyMongoError as pme:
            logger.error(f"Database error occurred: {str(pme)}")
            raise ValueError("Database error occurred while creating document.") from pme
        except Exception as e:
            logger.exception(f"Unexpected error occurred while creating document: {str(e)}")
            raise

    def get_by_id(self, brand_id: str, content_id: str = None):
        """
        Retrieve a brand by ID.
        """
        collection = self.mongo.get_database()[self.collection_name]
        try:
            # Apply filters
            query_filter = {"_id": brand_id}

            # Selected field
            selected_fields = {
                "id": "$_id",
                "name": 1,
                "org_id": 1,
                "logo": 1,
                "ads_coin_mining": 1,
                "ads_brand_placement": 1,
                "_id": 0
            }

            # Aggregation pipeline
            pipeline = [
                {"$match": query_filter},  # Filter stage
                {
                    "$lookup": {
                        "from": "_dmsfile",
                        "let": { "brand_id": { "$toString": "$_id" } },
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": { "$eq": ["$refkey_id", "$$brand_id"] },
                                    "doctype": "28f0634cdbea43f89010a147e365ae98" 
                                }
                            },
                            {
                                "$project": {
                                    "id": "$_id",
                                    "_id": 0,
                                    "filename": "$filename",
                                    "metadata": "$metadata",
                                    "path": "$folder_path",
                                    "info_file": "$filestat"
                                }
                            }
                        ],
                        "as": "brand_logo_data"
                    }
                },
                # Lookup for ads coin mining data
                {
                    "$lookup": {
                        "from": "_dmsfile",
                        "let": { "brand_id": { "$toString": "$_id" } },
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": { "$eq": ["$refkey_id", "$$brand_id"] },
                                    "doctype": "d7fc34c825034be9a99763f1f0c4ad70"
                                }
                            },
                            {
                                "$project": {
                                    "id": "$_id",
                                    "_id": 0,
                                    "filename": "$filename",
                                    "metadata": "$metadata",
                                    "path": "$folder_path",
                                    "info_file": "$filestat"
                                }
                            }
                        ],
                        "as": "ads_coin_mining_data"
                    }
                },
                # Lookup for ads brand placement
                {
                    "$lookup": {
                        "from": "_dmsfile",
                        "let": { "brand_id": { "$toString": "$_id" } },
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": { "$eq": ["$refkey_id", "$$brand_id"] },
                                    "doctype": "fdbbeaa7258844149c86cd1a283c541c",
                                    "metadata": { "Content ID": content_id }  # Filter by Content ID if provided
                                }
                            },
                            {
                                "$project": {
                                    "id": "$_id",
                                    "_id": 0,
                                    "filename": "$filename",
                                    "metadata": "$metadata",
                                    "path": "$folder_path",
                                    "info_file": "$filestat"
                                }
                            }
                        ],
                        "as": "ads_brand_placement_data"
                    }
                },
                # Add fields for poster, fyp, and main_sponsor with logo
                {
                    "$addFields": {
                        "logo": "$brand_logo_data",
                        "ads_coin_mining": "$ads_coin_mining_data",
                        "ads_brand_placement": "$ads_brand_placement_data",
                    }
                },
                {"$project": selected_fields}  # Project only selected fields
            ]

            # Execute aggregation pipeline
            cursor = collection.aggregate(pipeline)
            results = list(cursor)

            if len(results) > 0:
                content_data = results[0]  # Get the first (and only) document
            else:
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    self.mongo,
                    action="retrieve",
                    target=self.collection_name,
                    target_id=brand_id,
                    details={"_id": brand_id},
                    status="failure",
                    error_message="Brand not found"
                )
                raise ValueError("Brand not found")

            # write audit trail for success
            self.audit_trail.log_audittrail(
                self.mongo,
                action="retrieve",
                target=self.collection_name,
                target_id=brand_id,
                details={"_id": brand_id, "retrieved_data": content_data},
                status="success"
            )

            return BrandListResponse(**content_data)
        except PyMongoError as pme:
            logger.error(f"Database error occurred: {str(pme)}")
            # write audit trail for fail
            self.audit_trail.log_audittrail(
                self.mongo,
                action="retrieve",
                target=self.collection_name,
                target_id=content_id,
                details={"_id": content_id},
                status="failure",
                error_message=str(pme)
            )
            raise ValueError("Database error occurred while find document.") from pme
        except S3Error as s3e:
            logger.error(
                "MinIO S3Error",
                host=config.minio_host,
                port=config.minio_port,
                bucket=config.minio_bucket,
                error=str(s3e.message),
                error_type="S3Error"
            )
            raise ValueError("Minio presigned object failed") from s3e
        except Exception as e:
            logger.exception(f"Unexpected error occurred while finding document: {str(e)}")
            raise

    def update_by_id(self, brand_id: str, data: Brand):
        """
        Update a brand data by ID.
        """
        collection = self.mongo.get_database()[self.collection_name]
        obj = data.model_dump()
        obj["mod_by"] = self.user_id
        obj["mod_date"] = datetime.now(timezone.utc)
        try:
            update_data = collection.find_one_and_update({"_id": brand_id}, {"$set": obj}, return_document=True)
            if not update_data:
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    self.mongo,
                    action="update",
                    target=self.collection_name,
                    target_id=brand_id,
                    details={"$set": obj},
                    status="failure",
                    error_message="Brand not found"
                )
                raise ValueError("Brand not found")
            # write audit trail for success
            self.audit_trail.log_audittrail(
                self.mongo,
                action="update",
                target=self.collection_name,
                target_id=brand_id,
                details={"$set": obj},
                status="success"
            )
            return BrandDetailResponse(**update_data)
        except PyMongoError as pme:
            logger.error(f"Database error occurred: {str(pme)}")
            # write audit trail for fail
            self.audit_trail.log_audittrail(
                self.mongo,
                action="update",
                target=self.collection_name,
                target_id=brand_id,
                details={"$set": obj},
                status="failure",
                error_message=str(pme)
            )
            raise ValueError("Database error occurred while update document.") from pme
        except Exception as e:
            logger.exception(f"Error updating role: {str(e)}")
            raise

    def update_status(self, brand_id: str, data: UpdateStatus):
        """
        Update a brand data [status] by ID.
        """
        collection = self.mongo.get_database()[self.collection_name]
        obj = data.model_dump()
        obj["mod_by"] = self.user_id
        obj["mod_date"] = datetime.now(timezone.utc)
        try:
            update_data = collection.find_one_and_update({"_id": brand_id}, {"$set": obj}, return_document=True)
            if not update_data:
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    self.mongo,
                    action="update",
                    target=self.collection_name,
                    target_id=brand_id,
                    details={"$set": obj},
                    status="failure",
                    error_message="Brand not found"
                )
                raise ValueError("Brand not found")
            logger.info(f"Brand {brand_id} status updated.")
            # write audit trail for success
            self.audit_trail.log_audittrail(
                self.mongo,
                action="update",
                target=self.collection_name,
                target_id=brand_id,
                details={"$set": obj},
                status="success"
            )
            return BrandDetailResponse(**update_data)
        except PyMongoError as pme:
            logger.error(f"Database error occurred: {str(pme)}")
            # write audit trail for fail
            self.audit_trail.log_audittrail(
                self.mongo,
                action="update",
                target=self.collection_name,
                target_id=brand_id,
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
        collection = self.mongo.get_database()[self.collection_name]
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
                "status": 1,
                "org_id": 1,
                "_id": 0
            }

            # Aggregation pipeline
            pipeline = [
                {"$match": query_filter},  # Filter stage
                {
                    "$lookup": {
                        "from": "_dmsfile",
                        "let": { "brand_id": { "$toString": "$_id" } },
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": { "$eq": ["$refkey_id", "$$brand_id"] },
                                    "doctype": "28f0634cdbea43f89010a147e365ae98" 
                                }
                            },
                            {
                                "$project": {
                                    "id": "$_id",
                                    "_id": 0,
                                    "filename": "$filename",
                                    "metadata": "$metadata",
                                    "path": "$folder_path",
                                    "info_file": "$filestat"
                                }
                            }
                        ],
                        "as": "brand_logo_data"
                    }
                },
                {"$sort": {sort_field: order}},  # Sorting stage
                {"$skip": skip},  # Pagination skip stage
                {"$limit": limit},  # Pagination limit stage
                {
                    "$addFields": {
                        "logo": "$brand_logo_data",
                    }
                },
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

            parsed_results = [BrandDetailResponse(**item) for item in results]

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
            logger.error(f"Error retrieving role with filters and pagination: {str(e)}")
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