from pymongo.errors import PyMongoError
from minio.error import S3Error
from typing import Optional, Dict, Any
from pymongo import ASCENDING, DESCENDING
from datetime import datetime, timezone

from baseapp.config import setting, mongodb, minio
from baseapp.utils.logger import Logger
from baseapp.utils.utility import generate_uuid
from baseapp.services.content.model import Content, ContentResponse, ContentDetailResponse, ContentListItem
from baseapp.services.audit_trail_service import AuditTrailService

config = setting.get_settings()
logger = Logger("baseapp.services.content.crud")

class CRUD:
    def __init__(self, collection_name="content"):
        self.collection_name = collection_name
        self.audit_trail = None
        self.minio_conn = minio.MinioConn()

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

    def create(self, data: Content):
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
            return ContentResponse(**obj)
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
        collection = self.mongo.get_database()[self.collection_name]
        try:
            # Apply filters
            query_filter = {"_id": content_id}

            # Selected field
            selected_fields = {
                "id": "$_id",
                "title": 1,
                "synopsis": 1,
                "cast": 1,
                "tags": 1,
                "genre": 1,
                "genre_details":1,
                "origin": 1,
                "territory": 1,
                "rating": 1,
                "status": 1,
                "release_date": 1,
                "license_from": 1,
                "licence_date_start": 1,
                "licence_date_end": 1,
                "is_full_paid": 1,
                "full_price_coins": 1,
                "main_sponsor": 1,
                "poster": 1,
                "fyp": 1,
                "highlight": 1,
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
                # Lookup for poster data
                {
                    "$lookup": {
                        "from": "_dmsfile",
                        "let": { "content_id": { "$toString": "$_id" } },
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": { "$eq": ["$refkey_id", "$$content_id"] },
                                    "doctype": "64c1c7ba4a5246648bf224bfd19fe118"
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
                        "as": "poster_data"
                    }
                },
                # Lookup for FYP data
                {
                    "$lookup": {
                        "from": "_dmsfile",
                        "let": { "content_id": { "$toString": "$_id" } },
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": { "$eq": ["$refkey_id", "$$content_id"] },
                                    "doctype": "31c557f0f4574f7aae55c1b6860a2e19"
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
                        "as": "fyp_data"
                    }
                },
                # Lookup for Highlight data
                {
                    "$lookup": {
                        "from": "_dmsfile",
                        "let": { "content_id": { "$toString": "$_id" } },
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": { "$eq": ["$refkey_id", "$$content_id"] },
                                    "doctype": "8014149170ad41148f5ae01d9b0aac7b"
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
                        "as": "highlight_data"
                    }
                },
                {
                    "$addFields": {
                        "poster": "$poster_data",
                        "fyp": "$fyp_data", 
                        "highlight": "$highlight_data"
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
                    target_id=content_id,
                    details={"_id": content_id},
                    status="failure",
                    error_message="Content not found"
                )
                raise ValueError("Content not found")

            # write audit trail for success
            self.audit_trail.log_audittrail(
                self.mongo,
                action="retrieve",
                target=self.collection_name,
                target_id=content_id,
                details={"_id": content_id, "retrieved_user": content_data},
                status="success"
            )

            # presigned url
            if "poster" in content_data and isinstance(content_data['poster'], list):
                grouped_poster = {}
                for poster_item in content_data['poster']:
                    # Generate URL
                    poster_item['url'] = None
                    if 'filename' in poster_item:
                        url = self.minio.presigned_get_object(config.minio_bucket, poster_item['filename'])
                        if url:
                            poster_item['url'] = url
                    
                    # Grouping Logic
                    lang_key = "other"
                    if "metadata" in poster_item and poster_item["metadata"] and "Language" in poster_item["metadata"]:
                        # Mengambil bahasa dari metadata, contoh: "ID" -> "id"
                        lang_key = poster_item["metadata"]["Language"].lower()
                    
                    if lang_key not in grouped_poster:
                        grouped_poster[lang_key] = {}
                    
                    grouped_poster[lang_key] = poster_item
                    poster_item.pop("metadata")

                # Replace list with grouped dictionary
                content_data['poster'] = grouped_poster

            if "fyp" in content_data and isinstance(content_data['fyp'], list):
                grouped_fyp = {}
                for video_item in content_data['fyp']:
                    # Generate URL
                    video_item['url'] = None
                    if 'filename' in video_item:
                        url = self.minio.presigned_get_object(config.minio_bucket, video_item['filename'])
                        if url:
                            video_item['url'] = url
                    
                    # Determine Keys
                    lang_key = "other"
                    res_key = "original"
                    
                    if "metadata" in video_item and video_item["metadata"]:
                        if "Language" in video_item["metadata"]:
                            lang_key = video_item["metadata"]["Language"].lower()
                        if "Resolution" in video_item["metadata"]:
                            res_key = video_item["metadata"]["Resolution"].lower()

                    # Build Nested Dict
                    if lang_key not in grouped_fyp:
                        grouped_fyp[lang_key] = {}
                    
                    grouped_fyp[lang_key][res_key] = video_item
                    video_item.pop("metadata")

                content_data['fyp'] = grouped_fyp

            if "highlight" in content_data and isinstance(content_data['highlight'], list):
                grouped_highlight = {}
                for video_item in content_data['highlight']:
                    # Generate URL
                    video_item['url'] = None
                    if 'filename' in video_item:
                        url = self.minio.presigned_get_object(config.minio_bucket, video_item['filename'])
                        if url:
                            video_item['url'] = url
                    
                    # Determine Keys
                    lang_key = "other"
                    res_key = "original"
                    
                    if "metadata" in video_item and video_item["metadata"]:
                        if "Language" in video_item["metadata"]:
                            lang_key = video_item["metadata"]["Language"].lower()
                        if "Resolution" in video_item["metadata"]:
                            res_key = video_item["metadata"]["Resolution"].lower()

                    # Build Nested Dict
                    if lang_key not in grouped_highlight:
                        grouped_highlight[lang_key] = {}
                    
                    grouped_highlight[lang_key][res_key] = video_item
                    video_item.pop("metadata")
                    
                content_data['highlight'] = grouped_highlight

            return ContentDetailResponse(**content_data)
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

    def update_by_id(self, content_id: str, data):
        """
        Update a role's data by ID.
        """
        collection = self.mongo.get_database()[self.collection_name]
        obj = data.model_dump()
        obj["mod_by"] = self.user_id
        obj["mod_date"] = datetime.now(timezone.utc)
        try:
            update_content = collection.find_one_and_update({"_id": content_id}, {"$set": obj}, return_document=True)
            if not update_content:
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    self.mongo,
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
                self.mongo,
                action="update",
                target=self.collection_name,
                target_id=content_id,
                details={"$set": obj},
                status="success"
            )
            update_content["id"] = update_content.pop("_id")
            return ContentResponse(**update_content)
        except PyMongoError as pme:
            logger.error(f"Database error occurred: {str(pme)}")
            # write audit trail for fail
            self.audit_trail.log_audittrail(
                self.mongo,
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
        collection = self.mongo.get_database()[self.collection_name]
        try:
            # Apply filters
            query_filter = filters or {}

            # Handle sort field
            if sort_field == "title":
                sort_field = "title.id"

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
                "title": 1,
                "synopsis": 1,
                "cast": 1,
                "tags": 1,
                "genre": 1,
                "genre_details":1,
                "origin": 1,
                "territory": 1,
                "rating": 1,
                "status": 1,
                "poster": 1,
                "fyp": 1,
                "highlight": 1,
                "release_date": 1,
                "license_from": 1,
                "licence_date_start": 1,
                "licence_date_end": 1,
                "is_full_paid": 1,
                "full_price_coins": 1,
                "main_sponsor": 1,
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
                # Lookup for poster data
                {
                    "$lookup": {
                        "from": "_dmsfile",
                        "let": { "content_id": { "$toString": "$_id" } },
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": { "$eq": ["$refkey_id", "$$content_id"] },
                                    "doctype": "64c1c7ba4a5246648bf224bfd19fe118"
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
                        "as": "poster_data"
                    }
                },
                # Lookup for FYP data
                {
                    "$lookup": {
                        "from": "_dmsfile",
                        "let": { "content_id": { "$toString": "$_id" } },
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": { "$eq": ["$refkey_id", "$$content_id"] },
                                    "doctype": "31c557f0f4574f7aae55c1b6860a2e19"
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
                        "as": "fyp_data"
                    }
                },
                # Lookup for Highlight data
                {
                    "$lookup": {
                        "from": "_dmsfile",
                        "let": { "content_id": { "$toString": "$_id" } },
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": { "$eq": ["$refkey_id", "$$content_id"] },
                                    "doctype": "8014149170ad41148f5ae01d9b0aac7b"
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
                        "as": "highlight_data"
                    }
                },
                {
                    "$addFields": {
                        "poster": "$poster_data",
                        "fyp": "$fyp_data", 
                        "highlight": "$highlight_data"
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
                    self.mongo,
                    action="retrieve",
                    target=self.collection_name,
                    target_id="agregate",
                    details={"aggregate": pipeline},
                    status="success"
                )

            for i, data in enumerate(results):
                # presigned url
                if "poster" in data and isinstance(data['poster'], list):
                    grouped_poster = {}
                    for poster_item in data['poster']:
                        # Generate URL
                        poster_item['url'] = None
                        if 'filename' in poster_item:
                            url = self.minio.presigned_get_object(config.minio_bucket, poster_item['filename'])
                            if url:
                                poster_item['url'] = url
                        
                        # Grouping Logic
                        lang_key = "other"
                        if "metadata" in poster_item and poster_item["metadata"] and "Language" in poster_item["metadata"]:
                            lang_key = poster_item["metadata"]["Language"].lower()
                        
                        if lang_key not in grouped_poster:
                            grouped_poster[lang_key] = {}
                        
                        grouped_poster[lang_key] = poster_item                    
                        poster_item.pop("metadata")
                        
                    # Replace list with grouped dictionary
                    data['poster'] = grouped_poster

                if "fyp" in data and isinstance(data['fyp'], list):
                    grouped_fyp = {}
                    for video_item in data['fyp']:
                        # Generate URL
                        video_item['url'] = None
                        if 'filename' in video_item:
                            url = self.minio.presigned_get_object(config.minio_bucket, video_item['filename'])
                            if url:
                                video_item['url'] = url
                        
                        # Determine Keys
                        lang_key = "other"
                        res_key = "original"
                        
                        if "metadata" in video_item and video_item["metadata"]:
                            if "Language" in video_item["metadata"]:
                                lang_key = video_item["metadata"]["Language"].lower()
                            if "Resolution" in video_item["metadata"]:
                                res_key = video_item["metadata"]["Resolution"].lower()

                        # Build Nested Dict
                        if lang_key not in grouped_fyp:
                            grouped_fyp[lang_key] = {}
                        
                        grouped_fyp[lang_key][res_key] = video_item
                        video_item.pop("metadata")

                    data['fyp'] = grouped_fyp

                if "highlight" in data and isinstance(data['highlight'], list):
                    grouped_highlight = {}
                    for video_item in data['highlight']:
                        # Generate URL
                        video_item['url'] = None
                        if 'filename' in video_item:
                            url = self.minio.presigned_get_object(config.minio_bucket, video_item['filename'])
                            if url:
                                video_item['url'] = url
                        
                        # Determine Keys
                        lang_key = "other"
                        res_key = "original"
                        
                        if "metadata" in video_item and video_item["metadata"]:
                            if "Language" in video_item["metadata"]:
                                lang_key = video_item["metadata"]["Language"].lower()
                            if "Resolution" in video_item["metadata"]:
                                res_key = video_item["metadata"]["Resolution"].lower()

                        # Build Nested Dict
                        if lang_key not in grouped_highlight:
                            grouped_highlight[lang_key] = {}
                        
                        grouped_highlight[lang_key][res_key] = video_item
                        video_item.pop("metadata")
                        
                    data['highlight'] = grouped_highlight

            parsed_results = [ContentListItem(**item) for item in results]

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
        except S3Error as s3e:
            logger.error(
                "MinIO S3Error",
                host=config.minio_host,
                port=config.minio_port,
                bucket=config.minio_bucket,
                error=str(s3e.message),
                error_type="S3Error"
            )
            raise ValueError("Minio presigned object error") from s3e
        except Exception as e:
            logger.exception(f"Unexpected error during deletion: {str(e)}")
            raise