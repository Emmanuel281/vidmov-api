import io,re

from pymongo.errors import PyMongoError
from typing import Optional
from datetime import datetime, timezone
from minio.error import S3Error
from magic import from_buffer
from pathlib import Path
from fastapi import UploadFile

from baseapp.utils.utility import generate_uuid
from baseapp.config import setting, mongodb, minio
from baseapp.utils.logger import Logger
from baseapp.services._dms.upload.model import UploadFile, SetMetaData
from baseapp.services.audit_trail_service import AuditTrailService

config = setting.get_settings()
logger = Logger("baseapp.services._dms.upload.crud")

class CRUD:
    def __init__(self):
        self.collection_file = "_dmsfile"
        self.collection_folder = "_dmsfolder"
        self.collection_doctype = "_dmsdoctype"
        self.collection_organization = "_organization"
        self.minio_conn = minio.MinioConn()

    def __enter__(self):
        self._mongo_context = mongodb.MongoConn()
        self.mongo = self._mongo_context.__enter__()

        self._minio_context = minio.MinioConn()
        self.minio_conn = self._minio_context.__enter__()
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

        # Inisialisasi atau perbarui AuditTrailService dengan konteks terbaru
        self.audit_trail = AuditTrailService(
            user_id=self.user_id,
            org_id=self.org_id,
            ip_address=self.ip_address,
            user_agent=self.user_agent
        )

    def get_file_extension(self, file: UploadFile) -> str:
        """
        Get the extension of the uploaded file.
        """
        return Path(file.filename).suffix.lower()
    
    def validate_mime_type(self, file_content: bytes, allowed_mime_types: list):
        """
        Validate the MIME type of a file.
        """
        mime_type = from_buffer(file_content, mime=True)
        if mime_type not in allowed_mime_types:
            logger.warning("Invalid file type")
            raise ValueError("Invalid file type")
    
    def get_storage_org(self, collection) -> int:
        get_org = collection.find_one({'_id': self.org_id})

        if not get_org:
            raise ValueError('Organization not found')
        
        dmsStorage = get_org["usedstorage"] if "usedstorage" in get_org else 0

        return dmsStorage
        
    def create_folders(self, mongo, values, pidFolder=None):
        """
        Function to create folders based on the doctype structure.
        """
        # Retrieve doctype
        doctype_collection = mongo.get_database()[self.collection_doctype]
        doctypeList = doctype_collection.find_one({'_id': values['doctype']})

        if not doctypeList:
            raise ValueError('Doctype not found')

        # Get folder structure from doctype
        folderToArr = doctypeList['folder'].split('>>')
        metaData_ = values['metadata']
        folderString = []
        levelFolder = 0

        for folder in folderToArr:
            levelFolder += 1
            folderName = folder.strip()
            folderName = re.sub(r'[^a-zA-Z0-9_ \n\.]', '', folderName)

            # Determine the folder name from metadata or default value
            if folderName not in metaData_:
                folderName = folderName
            else:
                if metaData_[folderName] == "":
                    folderName = "_Empty_"
                else:
                    folderName = metaData_[folderName]

            folderString.append(folderName)

            # Search for the folder in the MongoDB collection
            query = {
                "folder_name": folderName,
                "level": levelFolder,
                "org_id": self.org_id
            }
            if levelFolder > 0:
                query["pid"] = pidFolder

            collection_folder = mongo.get_database()[self.collection_folder]
            getFolder = list(collection_folder.find(query))

            if len(getFolder) == 0:
                # Insert folder if it does not exist
                insertFolder = {
                    "_id":generate_uuid(),
                    "folder_name": folderName,
                    "level": levelFolder,
                    "org_id": self.org_id,
                    "pid": pidFolder
                }

                inserted_folder = collection_folder.insert_one(insertFolder)
                pidFolder = inserted_folder.inserted_id
            else:
                logger.debug(f"data folder: {getFolder}")
                pidFolder = getFolder[0]['_id']

        return pidFolder,folderString
    
    async def upload_file_to_minio(self, file: UploadFile, payload: SetMetaData):
        """
        Upload file.
        """
        # Read file content
        file_content = await file.read()

        # Validate file MIME type
        allowed_mime_types = ["application/pdf", "image/jpeg", "image/png", "text/plain", "video/mp4", "video/mpeg"]
        self.validate_mime_type(file_content, allowed_mime_types)

        # Validate file extension
        file_extension = self.get_file_extension(file)
        if file_extension not in [".pdf", ".jpg", ".png", ".txt", ".mp4", ".mpeg"]:
            raise ValueError("Unsupported file extension")
        
        UUID = generate_uuid()
        payload = payload.model_dump()

        collection_file = self.mongo.get_database()[self.collection_file]
        collection_org = self.mongo.get_database()[self.collection_organization]
        try:
            # check last storage
            storage_minio = self.get_storage_org(collection_org)

            # generate folder
            pid_folder, folderString = self.create_folders(self.mongo, payload)
            
            object_name = f"{UUID}{file_extension}"
            file_stream = io.BytesIO(file_content)
            file_size = len(file_content)

            # upload to minio
            self.minio_conn.put_object(
                bucket_name=config.minio_bucket,
                object_name=object_name,
                data=file_stream,
                length=file_size,
                content_type=file.content_type
            )
            
            # save metadata
            obj = UploadFile(
                filename=object_name,
                filestat={
                    "mime-type": file.content_type ,
                    "original-name": file.filename ,
                    "size": file_size
                },
                folder_id=pid_folder,
                folder_path=" >> ".join(folderString)
            )
            obj = obj.model_dump()
            obj["_id"] = UUID
            obj["rec_by"] = self.user_id
            obj["rec_date"] = datetime.now(timezone.utc)
            obj["org_id"] = self.org_id
            # metadata
            obj["metadata"] = payload["metadata"]
            obj["doctype"] = payload["doctype"]
            obj["refkey_id"] = payload["refkey_id"]
            obj["refkey_table"] = payload["refkey_table"]
            obj["refkey_name"] = payload["refkey_name"]
            insert_metadata = collection_file.insert_one(obj)

            # update storage
            collection_org.find_one_and_update({"_id": self.org_id}, {"$set": {"usedstorage":storage_minio+file_size}}, return_document=True)

            return {
                "filename":object_name,
                "id":insert_metadata.inserted_id,
                "folder_path":obj["folder_path"]
            }
        except S3Error as s3e:
            logger.error(
                "MinIO S3Error",
                host=config.minio_host,
                port=config.minio_port,
                bucket=config.minio_bucket,
                error=str(e),
                error_type="S3Error"
            )
            raise ValueError("Minio put object failed") from s3e
        except PyMongoError as pme:
            logger.error(f"Database error occurred: {str(pme)}")
            raise ValueError("Database error occurred while creating document.") from pme
        except Exception as e:
            logger.exception(f"Unexpected error occurred while creating document: {str(e)}")
            raise

    def set_metadata(self, file_id: str, data: SetMetaData):
        """
        Set metadata to file.
        """
        collection = self.mongo.get_database()[self.collection_file]
        obj = data.model_dump()
        obj["mod_by"] = self.user_id
        obj["mod_date"] = datetime.now(timezone.utc)
        try:
            obj["folder_id"] = ""
            obj["folder_path"] = ""
            update_metadata = collection.find_one_and_update({"_id": file_id}, {"$set": obj}, return_document=True)
            if not update_metadata:
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    self.mongo,
                    action="update_metadata",
                    target=self.collection_file,
                    target_id=file_id,
                    details={"$set": obj},
                    status="failure",
                    error_message="File not found"
                )
                raise ValueError("File not found")
            # write audit trail for success
            self.audit_trail.log_audittrail(
                self.mongo,
                action="update_metadata",
                target=self.collection_file,
                target_id=file_id,
                details={"$set": obj},
                status="success"
            )
            return update_metadata
        except PyMongoError as pme:
            logger.error(f"Database error occurred: {str(pme)}")
            # write audit trail for fail
            self.audit_trail.log_audittrail(
                self.mongo,
                action="update",
                target=self.collection_file,
                target_id=file_id,
                details={"$set": obj},
                status="failure",
                error_message=str(pme)
            )
            raise ValueError("Database error occurred while update document.") from pme
        except Exception as e:
            logger.exception(f"Error updating metadata: {str(e)}")
            raise