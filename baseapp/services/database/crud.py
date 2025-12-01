import logging,json
from pymongo.errors import PyMongoError

from baseapp.config import setting, mongodb, minio

config = setting.get_settings()
logger = logging.getLogger(__name__)

class CRUD:
    def __init__(self):
        pass

    def create_db(self):
        """
        Create database and tables with schema.
        """
        try:
            with open(f"{config.file_location}mongodb.json") as json_file:
                initData = json.load(json_file)            
                with mongodb.MongoConn() as mongo_conn:
                    is_exists = mongo_conn.check_database_exists()
                    logger.debug(f"Database exist is {is_exists}")
                    if not is_exists:
                        mongo_conn.create_database(initData)
                    return is_exists
        except PyMongoError as pme:
            logger.error(f"Database error occurred: {str(pme)}")
            raise ValueError("Database error occurred while create database and tables.") from pme
        except Exception as e:
            logger.exception(f"Unexpected error occurred while creating document: {str(e)}")
            raise

    def create_bucket(self):
        """
        Create bucket.
        """
        with minio.MinioConn(bucket=config.minio_bucket) as conn:
            conn.create_bucket()