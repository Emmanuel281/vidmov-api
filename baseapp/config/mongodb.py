from pymongo import MongoClient,errors
import logging,uuid
from baseapp.config import setting

config = setting.get_settings()
logger = logging.getLogger()

class MongoConn:
    _client = None

    def __init__(self, database=None):
        self.database = database or config.mongodb_db
        self._db = None

    @classmethod
    def initialize(cls):
        """
        Inisialisasi Global Connection Pool.
        Wajib dipanggil SEKALI saat aplikasi start (misal di main.py).
        """
        if cls._client is None:
            try:
                # Konstruksi URI dengan/tanpa autentikasi
                if config.mongodb_user and config.mongodb_pass:
                    uri = f"mongodb://{config.mongodb_user}:{config.mongodb_pass}@{config.mongodb_host}:{config.mongodb_port}"
                else:
                    uri = f"mongodb://{config.mongodb_host}:{config.mongodb_port}"

                # Membuat MongoClient (Otomatis mengatur pooling)
                # maxPoolSize=100 (default)
                cls._client = MongoClient(
                    uri,
                    minPoolSize=config.mongodb_min_pool_size, 
                    maxPoolSize=config.mongodb_max_pool_size,
                )
                
                # Test koneksi ringan (opsional)
                # cls._client.admin.command('ping')
                
                logger.info(f"MongoDB Pool initialized (Min: {config.mongodb_min_pool_size}, Max: {config.mongodb_max_pool_size})")
                
            except errors.ConnectionFailure as e:
                logger.error(f"Failed to connect to MongoDB: {e}")
                raise ConnectionError("Failed to connect to MongoDB")
            except Exception as e:
                logger.exception(f"Unexpected error initializing MongoDB: {e}")
                raise

    @classmethod
    def close_connection(cls):
        """
        Menutup seluruh koneksi di pool. Dipanggil saat aplikasi shutdown.
        """
        if cls._client:
            cls._client.close()
            cls._client = None
            logger.info("MongoDB Connection Pool closed.")

    def __enter__(self):
        try:
            # Lazy Init: Jaga-jaga jika lupa panggil initialize() di main.py
            if self.__class__._client is None:
                self.__class__.initialize()

            # Pilih Database dari client yang sudah ada (sangat cepat)
            self._db = self.__class__._client[self.database]
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
        self._db = None
        
        if exc_type:
            logger.error(f"Error in MongoDB Context: {exc_type.__name__}: {exc_value}")
            # Return False agar exception tetap naik (raise) ke pemanggil
            return False

    def __getattr__(self, name):
        """
        Memungkinkan akses collection langsung via attribute.
        Contoh: mongo.users.find() daripada mongo.get_database()['users'].find()
        """
        if self._db is not None:
            return self._db[name]
        raise AttributeError(f"Database context not active or attribute '{name}' not found.")
    
    def get_database(self):
        if self._db is None:
            logger.warning("Database is not selected. Use __enter__ method with a database name.")
            raise ValueError("Database is not selected")
        return self._db

    def get_connection(self):
        if not self.__class__._client:
            self.__class__.initialize()
        return self.__class__._client

    def create_database(self, config_json):
        """
        Create database, collections, and initialize data based on a JSON configuration.
        """
        db_target = self._db
        if db_target is None:
             # Fallback jika dipanggil di luar context manager
             if self.__class__._client is None:
                self.__class__.initialize()
             db_target = self.__class__._client[self.database]

        logger.info(f"Starting schema creation for database: {self.database}...")

        for collection_name, collection_config in config_json.items():
            logger.info(f"Processing collection: {collection_name}")
            collection = db_target[collection_name]

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
        if self.__class__._client is None:
            self.__class__.initialize()

        try:
            existing_databases = self.__class__._client.list_database_names()
            exists = self.database in existing_databases
            if exists:
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
