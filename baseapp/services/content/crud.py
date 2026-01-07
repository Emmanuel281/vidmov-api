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
from baseapp.services.content_search.hooks import content_search_hooks
from baseapp.services.streaming.crud import StreamingURLMixin

config = setting.get_settings()
logger = Logger("baseapp.services.content.crud")

class CRUD(StreamingURLMixin):
    def __init__(self, collection_name="content"):
        super().__init__()
        self.collection_name = collection_name
        self.colls_content_video = "content_video"
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
    
    def create(self, data: Content):
        """
        Insert a new content into the collection.
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
            
            # Enqueue sync task
            content_search_hooks.after_create(obj["id"], obj)

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
                "mature_content": 1,
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
                "fyp_1": 1,
                "fyp_2": 1,
                "_id": 0
            }

            # Aggregation pipeline
            pipeline = [
                {"$match": query_filter},  # Filter stage
                # lookup for genre details
                {
                    "$lookup": {
                        "from": "_enum",  # The collection to join with
                        "localField": "genre",  # Array field in content collection
                        "foreignField": "_id",  # Field in genre_groups collection
                        "as": "genre_details"  # Output array field
                    }
                },
                # Lookup for brand data
                {
                    "$lookup": {
                        "from": "brand",
                        "localField": "main_sponsor.brand_id",
                        "foreignField": "_id",
                        "as": "brand_info"
                    }
                },
                {
                    "$lookup": {
                        "from": "_dmsfile",
                        "let": { "brand_id": "$main_sponsor.brand_id" },
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
                        "as": "fyp_1_data"
                    }
                },
                # Lookup for FYP #2 data
                {
                    "$lookup": {
                        "from": "_dmsfile",
                        "let": { "content_id": { "$toString": "$_id" } },
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": { "$eq": ["$refkey_id", "$$content_id"] },
                                    "doctype": "3551a74699394f22b21ecf8277befa39"
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
                        "as": "fyp_2_data"
                    }
                },
                # Add fields for genre_details, poster, fyp, and main_sponsor with logo
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
                        },
                        "poster": "$poster_data",
                        "fyp_1": "$fyp_1_data",
                        "fyp_2": "$fyp_2_data",
                        "main_sponsor": {
                            "$cond": {
                                "if": { "$and": [
                                    { "$gt": [{ "$size": "$brand_info" }, 0] },
                                    { "$ne": ["$main_sponsor", None] }
                                ]},
                                "then": {
                                    "$mergeObjects": [
                                        "$main_sponsor",
                                        { "$arrayElemAt": ["$brand_info", 0] },
                                        { "logo": { "$arrayElemAt": ["$brand_logo_data", 0] } }
                                    ]
                                },
                                "else": "$main_sponsor"
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
                details={"_id": content_id, "retrieved_data": content_data},
                status="success"
            )

            # presigned url
            if "poster" in content_data and isinstance(content_data['poster'], list):
                content_data['poster'] = self.process_poster_items(
                    content_id, 
                    content_data['poster']
                )

            if "fyp_1" in content_data and isinstance(content_data['fyp_1'], list):
                content_data['fyp_1'] = self.process_video_items(
                    content_id,
                    content_data['fyp_1'],
                    'fyp_1'
                )

            if "fyp_2" in content_data and isinstance(content_data['fyp_2'], list):
                content_data['fyp_2'] = self.process_video_items(
                    content_id,
                    content_data['fyp_2'],
                    'fyp_2'
                )

            if content_data.get("main_sponsor") and content_data["main_sponsor"].get("logo"):
                content_data["main_sponsor"] = self.add_logo_url(
                    content_data["main_sponsor"]
                )

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
        Update a content's data by ID.
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

            # Enqueue sync task
            content_search_hooks.after_update(content_id, obj)
            
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
                "mature_content": 1,
                "rating": 1,
                "status": 1,
                "poster": 1,
                "fyp_1": 1,
                "fyp_2": 1,
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
                # Lookup for brand data
                {
                    "$lookup": {
                        "from": "brand",
                        "localField": "main_sponsor.brand_id",
                        "foreignField": "_id",
                        "as": "brand_info"
                    }
                },
                {
                    "$lookup": {
                        "from": "_dmsfile",
                        "let": { "brand_id": "$main_sponsor.brand_id" },
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
                        "as": "fyp_1_data"
                    }
                },
                # Lookup for FYP #2 data
                {
                    "$lookup": {
                        "from": "_dmsfile",
                        "let": { "content_id": { "$toString": "$_id" } },
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": { "$eq": ["$refkey_id", "$$content_id"] },
                                    "doctype": "3551a74699394f22b21ecf8277befa39"
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
                        "as": "fyp_2_data"
                    }
                },
                # Add fields for poster, fyp, and main_sponsor with logo
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
                        },
                        "poster": "$poster_data",
                        "fyp_1": "$fyp_1_data",
                        "fyp_2": "$fyp_2_data",
                        "main_sponsor": {
                            "$cond": {
                                "if": { "$and": [
                                    { "$gt": [{ "$size": "$brand_info" }, 0] },
                                    { "$ne": ["$main_sponsor", None] }
                                ]},
                                "then": {
                                    "$mergeObjects": [
                                        "$main_sponsor",
                                        { "$arrayElemAt": ["$brand_info", 0] },
                                        { "logo": { "$arrayElemAt": ["$brand_logo_data", 0] } }
                                    ]
                                },
                                "else": "$main_sponsor"
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
                content_id = data.get('id')

                # presigned url
                if "poster" in data and isinstance(data['poster'], list):
                    data['poster'] = self.process_poster_items(
                        content_id,
                        data['poster']
                    )

                if "fyp_1" in data and isinstance(data['fyp_1'], list):
                    data['fyp_1'] = self.process_video_items(
                        content_id,
                        data['fyp_1'],
                        'fyp_1'
                    )

                if "fyp_2" in data and isinstance(data['fyp_2'], list):
                    data['fyp_2'] = self.process_video_items(
                        content_id,
                        data['fyp_2'],
                        'fyp_2'
                    )

                if data.get("main_sponsor") and data["main_sponsor"].get("logo"):
                    data["main_sponsor"] = self.add_logo_url(
                        data["main_sponsor"]
                    )

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

    def get_content_by_brand(self, brand_id: str, title_contains: Optional[str] = None):
        """
        Mengambil konten berdasarkan brand_id (Sponsor Utama atau Sponsor Episode)
        dengan tambahan filter pencarian judul menggunakan regex.
        """
        collection = self.mongo.get_database()[self.collection_name]
        
        try:
            # Match dasar untuk Brand (Sponsor Utama atau di dalam array episodes)
            match_condition = {
                "$or": [
                    {"main_sponsor.brand_id": brand_id},
                    {"episodes.episode_sponsor.brand_id": brand_id}
                ]
            }

            # Tambahkan filter Regex untuk judul jika parameter title_contains diberikan
            if title_contains:
                # Menggunakan title.id karena field title adalah dictionary dan 'id' wajib ada
                match_condition["title.id"] = {
                    "$regex": f".*{title_contains}.*", 
                    "$options": "i"
                }

            pipeline = [
                # 1. Join dengan koleksi content_video
                {
                    "$lookup": {
                        "from": self.colls_content_video,
                        "localField": "_id",
                        "foreignField": "content_id",
                        "as": "episodes"
                    }
                },
                # 2. Filter gabungan (Brand + Title Regex)
                {"$match": match_condition},
                # 3. Project output
                {
                    "$project": {
                        "_id": 0,
                        "id": "$_id",
                        "title": 1,
                        "status": 1
                    }
                }
            ]

            cursor = collection.aggregate(pipeline)
            results = list(cursor)

            if self.audit_trail:
                self.audit_trail.log_audittrail(
                    self.mongo,
                    action="retrieve_by_brand_search",
                    target=self.collection_name,
                    target_id=brand_id,
                    details={"brand_id": brand_id, "search": title_contains},
                    status="success"
                )

            return {
                "data": results
            }

        except PyMongoError as pme:
            logger.error(f"Database error in get_content_by_brand: {str(pme)}")
            raise ValueError("Gagal mencari data.") from pme