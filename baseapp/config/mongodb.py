from pymongo import MongoClient,errors
import logging,uuid
from baseapp.config import setting

logger = logging.getLogger()

class MongoConn:
    def __init__(self, host=None, port=None, database=None, username=None, password=None):
        config = setting.get_settings()
        self.host = host or config.mongodb_host
        self.port = port or config.mongodb_port
        self.database = database or config.mongodb_db
        self.username = username or config.mongodb_user
        self.password = password or config.mongodb_pass
        self._conn = None
        self._db = None

    def __enter__(self):
        try:
            # Buat URL koneksi
            uri = f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}" if self.username and self.password else f"mongodb://{self.host}:{self.port}"
            
            # Membuka koneksi ke MongoDB
            self._conn = MongoClient(uri)
            logger.info("Connected to MongoDB")

            if self.database:
                self._db = self._conn[self.database]
                logger.info(f"Selected database: {self.database}")

            return self
        except errors.ServerSelectionTimeoutError as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise ConnectionError("Failed to connect to MongoDB")
        except errors.OperationFailure as e:
            logger.error(f"Authentication failed: {e}")
            raise ConnectionError("Authentication failed to connect to MongoDB")
        except errors.PyMongoError as e:
            logger.error(f"MongoDB error: {e}")
            raise 
        except Exception as e:
            logger.exception(f"Unexpected error: {e}")
            raise

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self._conn:
            self._conn.close()
            logger.info("MongoDB connection closed.")
        if exc_type:
            logger.exception(f"Error type: {exc_type}, value: {exc_value}")
            return False

    def get_database(self):
        if self._db is None:
            logger.warning("Database is not selected. Use __enter__ method with a database name.")
            raise ValueError("Database is not selected")
        return self._db

    def get_connection(self):
        if not self._conn:
            self.__enter__()
        return self._conn

    def close(self):
        if self._conn:
            logger.info("Closing connection to MongoDB")
            self._conn.close()
            self._conn = None
            self._db = None

    def create_database(self, config_json):
        """
        Create database, collections, and initialize data based on a JSON configuration.
        """
        if self._db is None:
            logger.error("Database is not selected.")
            raise ValueError("Database is not selected.")

        logger.info("Starting database creation...")

        for collection_name, collection_config in config_json.items():
            logger.info(f"Processing collection: {collection_name}")
            collection = self._db[collection_name]

            # 1. Membuat Indeks
            indexes = collection_config.get("index", [])
            for idx in indexes:
                try:
                    if isinstance(idx, str):  # Indeks sederhana
                        collection.create_index(idx)
                        logger.info(f"Created index on fields: {idx}")
                    elif isinstance(idx, dict):  # Indeks kompleks
                        for index_name, fields in idx.items():
                            index_fields = [(field, 1) for field in fields]
                            collection.create_index(index_fields, name=index_name)
                            logger.info(f"Created compound index: {index_name} on fields: {fields}")
                except errors.PyMongoError as e:
                    logger.error(f"Error creating index on collection '{collection_name}': {e}")
                    raise ValueError("Error creating index on collection")

            # 2. Menambahkan Data Awal
            initial_data = collection_config.get("data", [])
            if initial_data:
                logger.info(f"Inserting initial data into collection: {collection_name}")
                try:
                    # Memeriksa dan menambahkan _id jika belum ada
                    for data in initial_data:
                        if "id" not in data:
                            data["_id"] = str(uuid.uuid4())
                        else:
                            data["_id"] = data["id"]
                            del data["id"]

                    collection.insert_many(initial_data, ordered=False)
                    logger.info(f"Inserted {len(initial_data)} documents into {collection_name}")
                except errors.BulkWriteError as bwe:
                    logger.error(f"Bulk write error in collection '{collection_name}': {bwe.details}")
                    raise ValueError("Bulk write error in collection")
                except errors.PyMongoError as e:
                    logger.error(f"Error inserting data into collection '{collection_name}': {e}")
                    raise ValueError("Error inserting data into collection")

        logger.info("Database creation completed.")

    def check_database_exists(self):
        """
        Function to check if a database exists in MongoDB.
        """
        try:
            existing_databases = self._conn.list_database_names()
            if self._db.name in existing_databases:
                return True
            else:
                logger.info(f"Database {self._db.name} does not exist.")
                return False
        except errors.PyMongoError as e:
            logger.error(f"Error while checking database existence: {e}")
            raise ValueError(f"Database operation failed: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error occurred while checking database existence: {e}")
            raise
