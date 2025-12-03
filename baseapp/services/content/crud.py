import logging

from pymongo.errors import PyMongoError
from typing import Optional, Dict, Any
from pymongo import ASCENDING, DESCENDING
from datetime import datetime, timezone

from baseapp.config import setting, mongodb, minio
from baseapp.utils.utility import generate_uuid
from baseapp.services.content.model import Content
from baseapp.services.audit_trail_service import AuditTrailService

config = setting.get_settings()
logger = logging.getLogger(__name__)

class CRUD:
    def __init__(self, collection_name="content"):
        self.collection_name = collection_name
        self.audit_trail = None
        self.minio_conn = minio.MinioConn()

    def set_context(self, user_id: str, org_id: str, ip_address: Optional[str] = None, user_agent: Optional[str] = None):
        """
        Memperbarui konteks pengguna dan menginisialisasi AuditTrailService.
        """
        self.user_id = user_id
        self.org_id = org_id
        self.ip_address = ip_address
        self.user_agent = user_agent

        self.audit_trail = AuditTrailService(
            user_id=self.user_id,
            org_id=self.org_id,
            ip_address=self.ip_address,
            user_agent=self.user_agent
        )

    def create(self, data: Content):
        """
        Insert a new role into the collection.
        """
        with mongodb.MongoConn() as mongo:
            collection = mongo.get_database()[self.collection_name]

            obj = data.model_dump()
            obj["_id"] = generate_uuid()
            obj["rec_by"] = self.user_id
            obj["rec_date"] = datetime.now(timezone.utc)
            obj["org_id"] = self.org_id
            try:
                result = collection.insert_one(obj)
                return obj
            except PyMongoError as pme:
                logger.error(f"Database error occurred: {str(pme)}")
                raise ValueError("Database error occurred while creating document.") from pme
            except Exception as e:
                logger.exception(f"Unexpected error occurred while creating document: {str(e)}")
                raise

    def get_by_id(self, content_id: str):
        """
        Retrieve a content by ID.
        """
        with mongodb.MongoConn() as mongo:
            collection = mongo.get_database()[self.collection_name]
            with self.minio_conn as conn:
                try:
                    # Apply filters
                    query_filter = {"_id": content_id}

                    # Selected field
                    selected_fields = {
                        "id": "$_id",
                        "name": 1,
                        "description": 1,
                        "cast": 1,
                        "genre": 1,
                        "genre_details":1,
                        "duration": 1,
                        "type": 1,
                        "episodes": 1,
                        "language": 1,
                        "rating": 1,
                        "status": 1,
                        "thumbnail": 1,
                        "release_date": 1,
                        "_id": 0
                    }

                    # Aggregation pipeline
                    pipeline = [
                        {"$match": query_filter},  # Filter stage
                        {
                            "$lookup": {
                                "from": "_enum",  # The collection to join with
                                "localField": "genre",  # Array field in content collection
                                "foreignField": "_id",  # Field in genre_groups collection
                                "as": "genre_details"  # Output array field
                            }
                        },
                        {
                            "$addFields": {
                                "genre_details": {
                                    "$map": {
                                        "input": "$genre_details",
                                        "as": "genre",
                                        "in": {
                                            "id": "$$genre._id",
                                            "value": "$$genre.value",
                                            "sort": "$$genre.sort",
                                        }
                                    }
                                }
                            }
                        },
                        # Lookup for thumbnail data
                        {
                            "$lookup": {
                                "from": "_dmsfile",
                                "let": { "content_id": { "$toString": "$_id" } },
                                "pipeline": [
                                    {
                                        "$match": {
                                            "$expr": { "$eq": ["$refkey_id", "$$content_id"] },
                                            "doctype": "723aa7d2-9326-4666-b9df-902d48c0543c"
                                        }
                                    },
                                    {
                                        "$project": {
                                            "id": "$_id",
                                            "_id": 0,
                                            "filename": "$filename",
                                            "path": "$folder_path",
                                            "info_file": "$filestat"
                                        }
                                    }
                                ],
                                "as": "thumbnail_data"
                            }
                        },
                        {
                            "$addFields": {
                                "thumbnail": { "$arrayElemAt": ["$thumbnail_data", 0] }
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
                            mongo,
                            action="retrieve",
                            target=self.collection_name,
                            target_id=content_id,
                            details={"_id": content_id},
                            status="failure",
                            error_message="Content not found"
                        )
                        raise ValueError("Content not found")

                    # write audit trail for success
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="retrieve",
                        target=self.collection_name,
                        target_id=content_id,
                        details={"_id": content_id, "retrieved_user": content_data},
                        status="success"
                    )

                    # presigned url
                    if "thumbnail" in content_data:
                        content_data['thumbnail']['url'] = None
                        minio_client = conn.get_minio_client()
                        url = minio_client.presigned_get_object(config.minio_bucket, content_data['thumbnail']['filename'])
                        if url:
                            content_data['thumbnail']['url'] = url.replace(minio_client.get_minio_endpoint(),minio_client.get_domain_endpoint())

                    return content_data
                except PyMongoError as pme:
                    logger.error(f"Database error occurred: {str(pme)}")
                    # write audit trail for fail
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="retrieve",
                        target=self.collection_name,
                        target_id=content_id,
                        details={"_id": content_id},
                        status="failure",
                        error_message=str(pme)
                    )
                    raise ValueError("Database error occurred while find document.") from pme
                except Exception as e:
                    logger.exception(f"Unexpected error occurred while finding document: {str(e)}")
                    raise

    def update_by_id(self, content_id: str, data):
        """
        Update a role's data by ID.
        """
        with mongodb.MongoConn() as mongo:
            collection = mongo.get_database()[self.collection_name]
            obj = data.model_dump()
            obj["mod_by"] = self.user_id
            obj["mod_date"] = datetime.now(timezone.utc)
            try:
                update_content = collection.find_one_and_update({"_id": content_id}, {"$set": obj}, return_document=True)
                if not update_content:
                    # write audit trail for fail
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="update",
                        target=self.collection_name,
                        target_id=content_id,
                        details={"$set": obj},
                        status="failure",
                        error_message="Content not found"
                    )
                    raise ValueError("Content not found")
                # write audit trail for success
                self.audit_trail.log_audittrail(
                    mongo,
                    action="update",
                    target=self.collection_name,
                    target_id=content_id,
                    details={"$set": obj},
                    status="success"
                )
                return update_content
            except PyMongoError as pme:
                logger.error(f"Database error occurred: {str(pme)}")
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    mongo,
                    action="update",
                    target=self.collection_name,
                    target_id=content_id,
                    details={"$set": obj},
                    status="failure",
                    error_message=str(pme)
                )
                raise ValueError("Database error occurred while update document.") from pme
            except Exception as e:
                logger.exception(f"Error updating role: {str(e)}")
                raise

    def get_all(self, filters: Optional[Dict[str, Any]] = None, page: int = 1, per_page: int = 10, sort_field: str = "_id", sort_order: str = "asc"):
        """
        Retrieve all documents from the collection with optional filters, pagination, and sorting.
        """
        with mongodb.MongoConn() as mongo:
            collection = mongo.get_database()[self.collection_name]
            with self.minio_conn as conn:
                try:
                    # Apply filters
                    query_filter = filters or {}

                    # Handle role filter specifically
                    if 'genre' in query_filter:
                        # Jika roles adalah string, konversi ke format $in
                        if isinstance(query_filter['genre'], str):
                            query_filter['genre'] = {"$in": [query_filter['genre']]}
                        # Jika roles adalah list, gunakan $in
                        elif isinstance(query_filter['genre'], list):
                            query_filter['genre'] = {"$in": query_filter['genre']}

                    # Pagination
                    skip = (page - 1) * per_page
                    limit = per_page

                    # Sorting
                    order = ASCENDING if sort_order == "asc" else DESCENDING

                    # Selected fields
                    selected_fields = {
                        "id": "$_id",
                        "name": 1,
                        "description": 1,
                        "cast": 1,
                        "genre": 1,
                        "genre_details":1,
                        "duration": 1,
                        "type": 1,
                        "episodes": 1,
                        "language": 1,
                        "rating": 1,
                        "status": 1,
                        "thumbnail": 1,
                        "release_date": 1,
                        "_id": 0
                    }

                    # Aggregation pipeline
                    pipeline = [
                        {"$match": query_filter},  # Filter stage
                        {"$sort": {sort_field: order}},  # Sorting stage
                        # Lookup stage to join with role groups
                        {
                            "$lookup": {
                                "from": "_enum",  # The collection to join with
                                "localField": "genre",  # Array field in content collection
                                "foreignField": "_id",  # Field in genre_groups collection
                                "as": "genre_details"  # Output array field
                            }
                        },
                        {
                            "$addFields": {
                                "genre_details": {
                                    "$map": {
                                        "input": "$genre_details",
                                        "as": "genre",
                                        "in": {
                                            "id": "$$genre._id",
                                            "value": "$$genre.value",
                                            "sort": "$$genre.sort",
                                        }
                                    }
                                }
                            }
                        },
                        # Lookup for thumbnail data
                        {
                            "$lookup": {
                                "from": "_dmsfile",
                                "let": { "content_id": { "$toString": "$_id" } },
                                "pipeline": [
                                    {
                                        "$match": {
                                            "$expr": { "$eq": ["$refkey_id", "$$content_id"] },
                                            "doctype": "723aa7d2-9326-4666-b9df-902d48c0543c"
                                        }
                                    },
                                    {
                                        "$project": {
                                            "id": "$_id",
                                            "_id": 0,
                                            "filename": "$filename",
                                            "path": "$folder_path",
                                            "info_file": "$filestat"
                                        }
                                    }
                                ],
                                "as": "thumbnail_data"
                            }
                        },
                        {
                            "$addFields": {
                                "thumbnail": { "$arrayElemAt": ["$thumbnail_data", 0] }
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
                    if self.audit_trail:
                        self.audit_trail.log_audittrail(
                            mongo,
                            action="retrieve",
                            target=self.collection_name,
                            target_id="agregate",
                            details={"aggregate": pipeline},
                            status="success"
                        )

                    for i, data in enumerate(results):
                        # presigned url
                        if "thumbnail" in data:
                            data['thumbnail']['url'] = None
                            minio_client = conn.get_minio_client()
                            url = minio_client.presigned_get_object(config.minio_bucket, data['thumbnail']['filename'])
                            if url:
                                data['thumbnail']['url'] = url.replace(conn.get_minio_endpoint(),conn.get_domain_endpoint())

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
                    logger.error(f"Error retrieving role with filters and pagination: {str(e)}")
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
                    logger.exception(f"Unexpected error during deletion: {str(e)}")
                    raise
