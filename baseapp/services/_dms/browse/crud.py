import logging

from pymongo.errors import PyMongoError
from typing import Optional, Dict, Any
from pymongo import ASCENDING, DESCENDING
from datetime import datetime, timezone

from baseapp.config import setting, mongodb, minio
from baseapp.services.audit_trail_service import AuditTrailService

from baseapp.services._dms.upload.model import MoveToTrash

config = setting.get_settings()

class CRUD:
    def __init__(self):
        self.collection_file = "_dmsfile"
        self.collection_folder = "_dmsfolder"
        self.collection_organization = "_organization"
        self.minio_conn = minio.MinioConn()
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

    def browse_by_key(self, filters: Optional[Dict[str, Any]] = None):
        """
        Retrieve all documents from the collection with optional filters, pagination, and sorting.
        """
        client = mongodb.MongoConn()
        with client as mongo:
            collection = mongo._db[self.collection_file]
            with self.minio_conn as conn:
                try:
                    # Apply filters
                    query_filter = {}
                    if filters:
                        for key, value in filters.items():
                            if isinstance(value, str) and value.startswith("regex:"):
                                # Extract regex pattern from value
                                regex_pattern = value.split("regex:", 1)[1]
                                query_filter[key] = {"$regex": regex_pattern, "$options": "i"}  # Case-insensitive regex
                            else:
                                query_filter[key] = value

                    # Selected fields
                    selected_fields = {
                        "id": "$_id",
                        "filename": 1,
                        "filestat": 1,
                        "folder_id": 1,
                        "folder_path": 1,
                        "metadata": 1,
                        "doctype": 1,
                        "refkey_table": 1,
                        "refkey_name": 1,
                        "refkey_id": 1,
                        "_id": 0
                    }

                    # Aggregation pipeline
                    pipeline = [
                        {"$match": query_filter},  # Filter stage
                        {"$project": selected_fields}  # Project only selected fields
                    ]

                    # Execute aggregation pipeline
                    cursor = collection.aggregate(pipeline)
                    results = list(cursor)

                    # write audit trail for success
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="retrieve",
                        target=self.collection_file,
                        target_id="agregate",
                        details={"aggregate": pipeline},
                        status="success"
                    )
                    
                    # retData = {}
                    # for x in results:
                    #     retData[x['refkey_name']] = {
                    #         'id':x['id'],
                    #         'folder_id':x['folder_id'],
                    #         'folder_path':x['folder_path'],
                    #         'filename':x['filename'],
                    #         'filestat':x['filestat'],
                    #         'metadata':x['metadata'],
                    #     }
                    #     # presigned url
                    #     minio_client = conn.get_minio_client()
                    #     url = minio_client.presigned_get_object(config.minio_bucket, x['filename'])
                    #     retData[x['refkey_name']]['url'] = url

                    for i, data in enumerate(results):
                        # presigned url
                        minio_client = conn.get_minio_client()
                        url = minio_client.presigned_get_object(config.minio_bucket, data['filename'])
                        data['url'] = url

                    return {
                        "data": results
                    }
                except PyMongoError as pme:
                    self.logger.error(f"Error retrieving index with filters and pagination: {str(e)}")
                    # write audit trail for success
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="retrieve",
                        target=self.collection_file,
                        target_id="agregate",
                        details={"aggregate": pipeline},
                        status="failure"
                    )
                    raise ValueError("Database error while retrieve document") from pme
                except Exception as e:
                    self.logger.exception(f"Unexpected error during deletion: {str(e)}")
                    raise

    def list_folder(self, filters: Optional[Dict[str, Any]] = None):
        """
        Retrieve all documents from the collection with optional filters, pagination, and sorting.
        """
        client = mongodb.MongoConn()
        with client as mongo:
            collection = mongo._db[self.collection_folder]
            try:
                # Apply filters
                query_filter = {}
                if filters:
                    for key, value in filters.items():
                        if isinstance(value, str) and value.startswith("regex:"):
                            # Extract regex pattern from value
                            regex_pattern = value.split("regex:", 1)[1]
                            query_filter[key] = {"$regex": regex_pattern, "$options": "i"}  # Case-insensitive regex
                        else:
                            query_filter[key] = value

                # Selected fields
                selected_fields = {
                    "id": "$_id",
                    "folder_name": 1,
                    "level": 1,
                    "pid": 1,
                    "_id": 0
                }

                # Aggregation pipeline
                pipeline = [
                    {"$match": query_filter},  # Filter stage
                    {"$project": selected_fields}  # Project only selected fields
                ]

                self.logger.debug(f"Pipeline data: {pipeline}")

                # Execute aggregation pipeline
                cursor = collection.aggregate(pipeline)
                results = list(cursor)

                retData = []
                for x in results:
                    objFolder = {
                        'id':x['id'],
                        'value':x['folder_name']
                    }
                    retData.append(objFolder)

                if "pid" not in filters:
                    retData.append({
                        'id':'delete',
                        'value':'Trash'
                    })

                # write audit trail for success
                self.audit_trail.log_audittrail(
                    mongo,
                    action="retrieve",
                    target=self.collection_folder,
                    target_id="agregate",
                    details={"aggregate": pipeline},
                    status="success"
                )

                return {
                    "data": retData
                }
            except PyMongoError as pme:
                self.logger.error(f"Error retrieving index with filters and pagination: {str(e)}")
                # write audit trail for success
                self.audit_trail.log_audittrail(
                    mongo,
                    action="retrieve",
                    target=self.collection_folder,
                    target_id="agregate",
                    details={"aggregate": pipeline},
                    status="failure"
                )
                raise ValueError("Database error while retrieve document") from pme
            except Exception as e:
                self.logger.exception(f"Unexpected error during deletion: {str(e)}")
                raise

    def list_file(self, filters: Optional[Dict[str, Any]] = None, page: int = 1, per_page: int = 10, sort_field: str = "_id", sort_order: str = "asc"):
        """
        Retrieve all documents from the collection with optional filters, pagination, and sorting.
        """
        client = mongodb.MongoConn()
        with client as mongo:
            collection = mongo._db[self.collection_file]
            with self.minio_conn as conn:
                try:
                    # Apply filters
                    query_filter = {}
                    if filters:
                        for key, value in filters.items():
                            if key == "$or":
                                # Handle special $or operator
                                query_filter[key] = value
                            elif isinstance(value, str) and value.startswith("regex:"):
                                regex_pattern = value.split("regex:", 1)[1]
                                query_filter[key] = {"$regex": regex_pattern, "$options": "i"}
                            else:
                                query_filter[key] = value

                    # Tambahkan default filter untuk is_deleted jika tidak ada dalam filters
                    if "is_deleted" not in query_filter and "$or" not in query_filter:
                        query_filter["$or"] = [
                            {"is_deleted": 0},
                            {"is_deleted": {"$exists": False}}
                        ]

                    # Pagination
                    skip = (page - 1) * per_page
                    limit = per_page

                    # Sorting
                    order = ASCENDING if sort_order == "asc" else DESCENDING

                    # Selected fields
                    selected_fields = {
                        "id": "$_id",
                        "filename": 1,
                        "filestat": 1,
                        "folder_id": 1,
                        "folder_path": 1,
                        "metadata": 1,
                        "doctype": 1,
                        "refkey_table": 1,
                        "refkey_name": 1,
                        "refkey_id": 1,
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
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="retrieve",
                        target=self.collection_file,
                        target_id="agregate",
                        details={"aggregate": pipeline},
                        status="success"
                    )

                    for i, data in enumerate(results):
                        # presigned url
                        minio_client = conn.get_minio_client()
                        url = minio_client.presigned_get_object(config.minio_bucket, data['filename'])
                        data['url'] = url

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
                    self.logger.error(f"Error retrieving index with filters and pagination: {str(e)}")
                    # write audit trail for success
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="retrieve",
                        target=self.collection_file,
                        target_id="agregate",
                        details={"aggregate": pipeline},
                        status="failure"
                    )
                    raise ValueError("Database error while retrieve document") from pme
                except Exception as e:
                    self.logger.exception(f"Unexpected error during deletion: {str(e)}")
                    raise

    def check_storage(self):
        """
        Retrieve a free space by org id (session).
        """
        client = mongodb.MongoConn()
        with client as mongo:
            collection = mongo._db[self.collection_organization]
            try:
                obj = collection.find_one({"_id": self.org_id})
                if not obj:
                    # write audit trail for fail
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="retrieve",
                        target=self.collection_organization,
                        target_id=self.org_id,
                        details={"_id": self.org_id},
                        status="failure",
                        error_message="Organization not found"
                    )
                    raise ValueError("Organization not found")
                # write audit trail for success
                self.audit_trail.log_audittrail(
                    mongo,
                    action="retrieve",
                    target=self.collection_organization,
                    target_id=self.org_id,
                    details={"_id": self.org_id, "retrieved": obj},
                    status="success"
                )
                return {"storage":obj["storage"],"usedstorage":obj["usedstorage"]}
            except PyMongoError as pme:
                self.logger.error(f"Database error occurred: {str(pme)}")
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    mongo,
                    action="retrieve",
                    target=self.collection_organization,
                    target_id=self.org_id,
                    details={"_id": self.org_id},
                    status="failure",
                    error_message=str(pme)
                )
                raise ValueError("Database error occurred while find document.") from pme
            except Exception as e:
                self.logger.exception(f"Unexpected error occurred while finding document: {str(e)}")
                raise
    
    def move_to_trash_restore(self, file_id: str, data: MoveToTrash):
        """
        File move to trash by ID.
        """
        client = mongodb.MongoConn()
        with client as mongo:
            collection = mongo._db[self.collection_file]
            obj = data.model_dump()
            obj["mod_by"] = self.user_id
            obj["mod_date"] = datetime.now(timezone.utc)
            try:
                update_role = collection.find_one_and_update({"_id": file_id}, {"$set": obj}, return_document=True)
                if not update_role:
                    # write audit trail for fail
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="update",
                        target=self.collection_file,
                        target_id=file_id,
                        details={"$set": obj},
                        status="failure",
                        error_message="File not found"
                    )
                    raise ValueError("File not found")
                self.logger.info(f"File {file_id} status updated.")
                # write audit trail for success
                self.audit_trail.log_audittrail(
                    mongo,
                    action="update",
                    target=self.collection_file,
                    target_id=file_id,
                    details={"$set": obj},
                    status="success"
                )
                return update_role
            except PyMongoError as pme:
                self.logger.error(f"Database error occurred: {str(pme)}")
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    mongo,
                    action="update",
                    target=self.collection_file,
                    target_id=file_id,
                    details={"$set": obj},
                    status="failure",
                    error_message=str(pme)
                )
                raise ValueError("Database error occurred while update document.") from pme
            except Exception as e:
                self.logger.exception(f"Error updating status: {str(e)}")
                raise

    def delete_file_by_id(self, file_id: str):
        """
        Menghapus satu file dari Minio dan database MongoDB
        
        Args:
            file_id: ID file yang akan dihapus
            
        Returns:
            Dict berisi status dan pesan
        """
        client = mongodb.MongoConn()
        with client as mongo:
            collection = mongo._db[self.collection_file]
            collection_org = mongo._db[self.collection_organization]
            with self.minio_conn as conn:
                try:
                    minio_client = conn.get_minio_client()
                    obj = collection.find_one({"_id": file_id})
                    if not obj:
                        # write audit trail for fail
                        self.audit_trail.log_audittrail(
                            mongo,
                            action="retrieve",
                            target=self.collection_file,
                            target_id=file_id,
                            details={"_id": file_id},
                            status="failure",
                            error_message="File not found"
                        )
                        raise ValueError("File not found")
                    
                    # remove file in minio
                    minio_client.remove_object(config.minio_bucket, obj['filename'])

                    # update space storage after deleted file
                    deleted_size = obj['filestat']['size']
                    collection_org.update_one({"_id": self.org_id}, {"$inc": {"usedstorage": -deleted_size}}, upsert=True)
                    
                    # delete file in mongodb
                    result = collection.delete_one({"_id": file_id})

                    self.audit_trail.log_audittrail(
                        mongo,
                        action="delete",
                        target=self.collection_file,
                        target_id=file_id,
                        details=obj,
                        status="success"
                    )

                    return result.deleted_count
                except PyMongoError as pme:
                    self.logger.error(f"Database error while deleting document with ID {file_id}: {str(pme)}")
                    # write audit trail for success
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="delete",
                        target=self.collection_file,
                        target_id=file_id,
                        details={"_id": file_id},
                        status="failure"
                    )
                    raise ValueError("Database error while delete document") from pme
                except Exception as e:
                    self.logger.exception(f"Unexpected error during deletion: {str(e)}")
                    raise

    def delete_folder_by_id(self, folder_id: str) -> Dict[str, Any]:
        """
        Menghapus satu file dari Minio dan database MongoDB
        
        Args:
            file_id: ID file yang akan dihapus
            
        Returns:
            Dict berisi status dan pesan
        """

        def _recursive(collection_file,collection_folder,start,depth=-1):
            try:
                folder_ids = [start]
                file_ids = []
                root_folder = []

                # get root folder
                folders = collection_folder.find_one({"_id": start})
                root_folder.append(folders)

                # recursive function that collects all the ids in `acc`
                def recurse(current, depth):
                    getFiles = collection_file.find({"folder_id": current})
                    for file in getFiles:
                        file_ids.append({
                            'id':file['_id'],
                            'filename':file['filename'],
                            'size':file['filestat']['size'],
                            'folder_id':file['folder_id']
                        })
                    folders = collection_folder.find({"pid": current})
                    for folder in folders:
                        folder_ids.append(folder['_id'])
                        recurse(folder['_id'], depth-1)

                recurse(start, depth) # starts the recursion
                return {'folders':folder_ids,'files':file_ids,'root':root_folder}
            except Exception as e:
                self.logger.error(f"Database error while recursive folders with ID {start}: {str(e)}")
                return None
            
        client = mongodb.MongoConn()
        with client as mongo:
            collection_file = mongo._db[self.collection_file]
            collection_folder = mongo._db[self.collection_folder]
            collection_org = mongo._db[self.collection_organization]
            with self.minio_conn as conn:
                try:
                    minio_client = conn.get_minio_client()
                    
                    # Recursive folder and files
                    recursive_folder = _recursive(collection_file,collection_folder,folder_id)

                    if not recursive_folder:
                        # write audit trail for fail
                        self.audit_trail.log_audittrail(
                            mongo,
                            action="retrieve",
                            target=self.collection_folder,
                            target_id=folder_id,
                            details={"_id": folder_id},
                            status="failure",
                            error_message="Folder not found"
                        )
                        raise ValueError("Folder not found")

                    # Delete folder
                    del_folder = collection_folder.delete_many({"_id": {"$in": recursive_folder["folders"]}})

                    # DELETE FILE
                    fileID = []
                    deleted_size = 0
                    for i in recursive_folder['files']:
                        minio_client.remove_object(config.minio_bucket, i['filename'])
                        deleted_size += i["size"]
                        fileID.append(i['id'])
                    del_file = collection_file.delete_many({"_id": {"$in": fileID}})

                    # update space storage after deleted file
                    collection_org.update_one({"_id": self.org_id}, {"$inc": {"usedstorage": -deleted_size}}, upsert=True)

                    self.audit_trail.log_audittrail(
                        mongo,
                        action="delete",
                        target=self.collection_folder,
                        target_id=folder_id,
                        details=recursive_folder,
                        status="success"
                    )

                    return recursive_folder
                except PyMongoError as pme:
                    self.logger.error(f"Database error while deleting folder with ID {folder_id}: {str(pme)}")
                    # write audit trail for success
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="delete",
                        target=self.collection_folder,
                        target_id=folder_id,
                        details={"_id": folder_id},
                        status="failure"
                    )
                    raise ValueError("Database error while delete folder") from pme
                except Exception as e:
                    self.logger.exception(f"Unexpected error during deletion: {str(e)}")
                    raise