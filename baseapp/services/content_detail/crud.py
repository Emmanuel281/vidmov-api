from pymongo.errors import PyMongoError
from minio.error import S3Error
from typing import Optional, Dict, Any
from pymongo import ASCENDING, DESCENDING
from datetime import datetime, timezone

from baseapp.config import setting, mongodb, minio
from baseapp.utils.logger import Logger
from baseapp.utils.utility import generate_uuid
from baseapp.services.content_detail.model import ContentDetail, ContentDetailResponse, ContentDetailListItem
from baseapp.services.audit_trail_service import AuditTrailService
from baseapp.services.streaming.crud import StreamingURLMixin

config = setting.get_settings()
logger = Logger("baseapp.services.content_detail.crud")

class CRUD(StreamingURLMixin):
    def __init__(self, collection_name="content_video"):
        super().__init__()
        self.collection_name = collection_name
        self.audit_trail = None

    def __enter__(self):
        self._mongo_context = mongodb.MongoConn()
        self.mongo = self._mongo_context.__enter__()

        self._minio_context = minio.MinioConn()
        self.minio = self._minio_context.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if hasattr(self, '_mongo_context'):
            return self._mongo_context.__exit__(exc_type, exc_value, traceback)
        if hasattr(self, '_minio_context'):
            return self._minio_context.__exit__(exc_type, exc_value, traceback)
        return False
    
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

    def create(self, data: ContentDetail):
        """
        Insert a new role into the collection.
        """
        collection = self.mongo.get_database()[self.collection_name]

        obj = data.model_dump()
        obj["_id"] = generate_uuid()
        obj["rec_by"] = self.user_id
        obj["rec_date"] = datetime.now(timezone.utc)
        obj["org_id"] = self.org_id
        try:
            result = collection.insert_one(obj)
            obj["id"] = obj.pop("_id")
            return ContentDetailResponse(**obj)
        except PyMongoError as pme:
            logger.error(f"Database error occurred: {str(pme)}")
            raise ValueError("Database error occurred while creating document.") from pme
        except Exception as e:
            logger.exception(f"Unexpected error occurred while creating document: {str(e)}")
            raise

    def get_by_id(self, episode_id: str):
        """
        Retrieve a episode by ID.
        """
        collection = self.mongo.get_database()[self.collection_name]
        try:
            # Apply filters
            query_filter = {"_id": episode_id}

            # Selected field
            selected_fields = {
                "id": "$_id",
                "content_id": 1,
                "episode": 1,
                "duration": 1,
                "rating": 1,
                "status": 1,
                "video": 1,
                "subtitle": 1,
                "dubbing": 1,
                "is_free": 1,
                "episode_price": 1,
                "episode_sponsor": 1,
                "_id": 0
            }

            # Aggregation pipeline
            pipeline = [
                {"$match": query_filter},  # Filter stage
                # Lookup stage untuk video
                {
                    "$lookup": {
                        "from": "_dmsfile",
                        "let": { "video_id": { "$toString": "$_id" } },
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": { "$eq": ["$refkey_id", "$$video_id"] },
                                    "doctype": "d67d38fe623b40ccb0ddb4671982c0d3"
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
                        "as": "video_data"
                    }
                },
                # Lookup stage untuk subtitle
                {
                    "$lookup": {
                        "from": "_dmsfile",
                        "let": { "video_id": { "$toString": "$_id" } },
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": { "$eq": ["$refkey_id", "$$video_id"] },
                                    "doctype": "ab176d7597704fe0b10f6521ca5b96bd"
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
                        "as": "subtitle_data"
                    }
                },
                # Lookup stage untuk dubber
                {
                    "$lookup": {
                        "from": "_dmsfile",
                        "let": { "video_id": { "$toString": "$_id" } },
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": { "$eq": ["$refkey_id", "$$video_id"] },
                                    "doctype": "4a626e3ebb8242a7b448a6203af4aefb"
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
                        "as": "dubbing_data"
                    }
                },
                # Lookup for brand data
                {
                    "$lookup": {
                        "from": "brand",
                        "localField": "episode_sponsor.brand_id",
                        "foreignField": "_id",
                        "as": "brand_info"
                    }
                },
                {
                    "$lookup": {
                        "from": "_dmsfile",
                        "let": { "brand_id": "$episode_sponsor.brand_id" },
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
                # Add fields for video, subtitle, dubbing and episode_sponsor
                {
                    "$addFields": {
                        "video": "$video_data",
                        "subtitle": "$subtitle_data",
                        "dubbing": "$dubbing_data",
                        "episode_sponsor": {
                            "$cond": {
                                "if": { "$and": [
                                    { "$gt": [{ "$size": "$brand_info" }, 0] },
                                    { "$ne": ["$episode_sponsor", None] }
                                ]},
                                "then": {
                                    "$mergeObjects": [
                                        "$episode_sponsor",
                                        { "$arrayElemAt": ["$brand_info", 0] },
                                        { "logo": { "$arrayElemAt": ["$brand_logo_data", 0] } }
                                    ]
                                },
                                "else": "$episode_sponsor"
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
                content_data = results[0]  # Get the first (and only) document
            else:
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    self.mongo,
                    action="retrieve",
                    target=self.collection_name,
                    target_id=episode_id,
                    details={"_id": episode_id},
                    status="failure",
                    error_message="Content not found"
                )
                raise ValueError("Content not found")

            # write audit trail for success
            self.audit_trail.log_audittrail(
                self.mongo,
                action="retrieve",
                target=self.collection_name,
                target_id=episode_id,
                details={"_id": episode_id, "retrieved_data": content_data},
                status="success"
            )
            
            # presigned url
            if "video" in content_data and isinstance(content_data['video'], list):
                content_data['video'] = self.process_episode_videos(
                    episode_id,
                    content_data['video']
                )

            if "subtitle" in content_data and isinstance(content_data['subtitle'], list):
                content_data['subtitle'] = self.process_subtitle_items(
                    episode_id,
                    content_data['subtitle']
                )

            if "dubbing" in content_data and isinstance(content_data['dubbing'], list):
                content_data['dubbing'] = self.process_dubbing_items(
                    episode_id,
                    content_data['dubbing']
                )

            if content_data.get("episode_sponsor") and content_data["episode_sponsor"].get("logo"):
                content_data["episode_sponsor"] = self.add_logo_url(
                    content_data["episode_sponsor"]
                )

            return ContentDetailResponse(**content_data)
        except PyMongoError as pme:
            logger.error(f"Database error occurred: {str(pme)}")
            # write audit trail for fail
            self.audit_trail.log_audittrail(
                self.mongo,
                action="retrieve",
                target=self.collection_name,
                target_id=episode_id,
                details={"_id": episode_id},
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

    def update_by_id(self, episode_id: str, data):
        """
        Update a episode's data by ID.
        """
        collection = self.mongo.get_database()[self.collection_name]
        obj = data.model_dump()
        obj["mod_by"] = self.user_id
        obj["mod_date"] = datetime.now(timezone.utc)
        try:
            update_content = collection.find_one_and_update({"_id": episode_id}, {"$set": obj}, return_document=True)
            if not update_content:
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    self.mongo,
                    action="update",
                    target=self.collection_name,
                    target_id=episode_id,
                    details={"$set": obj},
                    status="failure",
                    error_message="Content not found"
                )
                raise ValueError("Content not found")
            # write audit trail for success
            self.audit_trail.log_audittrail(
                self.mongo,
                action="update",
                target=self.collection_name,
                target_id=episode_id,
                details={"$set": obj},
                status="success"
            )
            update_content["id"] = update_content.pop("_id")
            return ContentDetailResponse(**update_content)
        except PyMongoError as pme:
            logger.error(f"Database error occurred: {str(pme)}")
            # write audit trail for fail
            self.audit_trail.log_audittrail(
                self.mongo,
                action="update",
                target=self.collection_name,
                target_id=episode_id,
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
                "content_id": 1,
                "episode": 1,
                "duration": 1,
                "rating": 1,
                "status": 1,
                "video": 1,
                "is_free": 1,
                "episode_price": 1,
                "episode_sponsor": 1,
                "_id": 0
            }

            # Aggregation pipeline
            pipeline = [
                {"$match": query_filter},  # Filter stage
                {"$sort": {sort_field: order}},  # Sorting stage
                # Lookup stage untuk video
                {
                    "$lookup": {
                        "from": "_dmsfile",
                        "let": { "video_id": { "$toString": "$_id" } },
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": { "$eq": ["$refkey_id", "$$video_id"] },
                                    "doctype": "d67d38fe623b40ccb0ddb4671982c0d3"
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
                        "as": "video_data"
                    }
                },
                # Lookup for brand data
                {
                    "$lookup": {
                        "from": "brand",
                        "localField": "episode_sponsor.brand_id",
                        "foreignField": "_id",
                        "as": "brand_info"
                    }
                },
                {
                    "$lookup": {
                        "from": "_dmsfile",
                        "let": { "brand_id": "$episode_sponsor.brand_id" },
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
                {
                    "$addFields": {
                        "video": "$video_data",
                        "episode_sponsor": {
                            "$cond": {
                                "if": { "$and": [
                                    { "$gt": [{ "$size": "$brand_info" }, 0] },
                                    { "$ne": ["$episode_sponsor", None] }
                                ]},
                                "then": {
                                    "$mergeObjects": [
                                        "$episode_sponsor",
                                        { "$arrayElemAt": ["$brand_info", 0] },
                                        { "logo": { "$arrayElemAt": ["$brand_logo_data", 0] } }
                                    ]
                                },
                                "else": "$episode_sponsor"
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
            parsed_results = [ContentDetailListItem(**item) for item in results]

            # Total count
            total_count = collection.count_documents(query_filter)

            # write audit trail for success
            if self.audit_trail:
                self.audit_trail.log_audittrail(
                    self.mongo,
                    action="retrieve",
                    target=self.collection_name,
                    target_id="agregate",
                    details={"aggregate": pipeline},
                    status="success"
                )

            for i, data in enumerate(results):
                episode_detail_id = data.get('id')

                # presigned url
                if "video" in data and isinstance(data['video'], list):
                    data['video'] = self.process_episode_videos(
                        episode_detail_id,
                        data['video']
                    )
                
                if data.get("episode_sponsor") and data["episode_sponsor"].get("logo"):
                    data["episode_sponsor"] = self.add_logo_url(
                        data["episode_sponsor"]
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
            logger.error(f"Error retrieving role with filters and pagination: {str(e)}")
            # write audit trail for success
            if self.audit_trail:
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