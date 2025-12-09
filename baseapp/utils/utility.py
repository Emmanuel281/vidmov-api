import bcrypt,string,secrets,uuid,hashlib
from pymongo.errors import PyMongoError

from baseapp.config.setting import get_settings
from baseapp.utils.logger import Logger

config = get_settings()
logger = Logger("baseapp.utils.utility")

def generate_uuid() -> str:
    return str(uuid.uuid4().hex)

def hash_password(password: str, salt=None) -> str:
    sha_digest = hashlib.sha256(password.encode("utf-8")).hexdigest()
    usalt = bcrypt.gensalt() if salt == None else salt
    hashed_password = bcrypt.hashpw(sha_digest.encode("utf-8"), usalt)
    return hashed_password.decode("utf-8")

def check_password(password: str, stored_hash: str) -> bool:
    """
    Memverifikasi password mentah dengan hash yang disimpan di DB.
    """
    if not password or not stored_hash:
        return False
    try:
        sha_digest_input = hashlib.sha256(password.encode("utf-8")).hexdigest()
        return bcrypt.checkpw(sha_digest_input.encode("utf-8"), stored_hash.encode("utf-8"))
    except Exception as e:
        logger.warning(f"Error saat mengecek password: {e}")
        return False

def generate_password(length: int = 8):
    characters = string.ascii_letters + string.digits + string.punctuation
    password = ''.join(secrets.choice(characters) for _ in range(length))
    return password
 
def get_enum(mongo, enum_id):
    collection = mongo.get_database()["_enum"]
    try:
        enum = collection.find_one({"_id": enum_id})
        return enum
    except PyMongoError as pme:
        raise ValueError("Database error occurred while find document.") from pme
    except Exception as e:
        raise

def is_none(variable, default_value):
    return default_value if variable is None else variable