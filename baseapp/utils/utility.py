import cbor2,bcrypt,string,secrets,logging
from fastapi.responses import Response, JSONResponse
from pymongo.errors import PyMongoError
from fastapi import Response, Request
from pydantic import BaseModel
from functools import wraps
from datetime import datetime, timezone

from baseapp.config.setting import get_settings
from baseapp.model.common import ApiResponse, TokenResponse

config = get_settings()
logger = logging.getLogger()

# Custom CBOR response class
class CBORResponse(Response):
    def __init__(self, content, status_code: int = 200, **kwargs):
        # Periksa apakah konten adalah salah satu dari model yang didukung
        if isinstance(content, (ApiResponse, TokenResponse)):
            content = process_mongodb_data(content.model_dump(mode="json"))
            cbor_content = cbor2.dumps(content)
        else:
            raise ValueError("Content must be an instance of ApiResponse or TokenResponse.")
        super().__init__(content=cbor_content, media_type="application/cbor", status_code=status_code, **kwargs)

def process_mongodb_data(data):
    if isinstance(data, dict):
        # Jika data adalah dictionary, proses semua nilai dalam dictionary
        if 'data' in data:
            # Jika ada key 'data', proses data dalam key tersebut
            data['data'] = process_mongodb_data(data['data'])
        return {k: process_mongodb_data(v) for k, v in data.items()}
    elif isinstance(data, list):
        # Jika data adalah list, proses semua item dalam list
        return [process_mongodb_data(item) for item in data]
    elif isinstance(data, datetime):
        # Jika data adalah datetime, konversi ke string ISO 8601
        if data.tzinfo is None:
            data = data.replace(tzinfo=timezone.utc)
        return data.isoformat()  # Mengubah ke string ISO 8601
    return data

def get_response_based_on_env(response_model: BaseModel, status_code: int = 200, request: Request = None):
    # Periksa flag force_json
    force_json = hasattr(request, 'state') and getattr(request.state, 'force_json', False)
    logger.debug(f"Force JSON status [utility]: {force_json}")
    
    if config.app_env == "production" and not force_json:
        return CBORResponse(content=response_model, status_code=status_code)
    else:
        return JSONResponse(content=response_model.model_dump(mode="json"), status_code=status_code)

def cbor_or_json(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        # Jalankan fungsi asli untuk mendapatkan hasil
        result = await func(*args, **kwargs)
        # Tentukan format respons
        return get_response_based_on_env(response_model=result)
    return wrapper

def json_only(func):
    @wraps(func)
    async def wrapper(request: Request = None, *args, **kwargs):
        if request:
            request.state.force_json = True  # Force JSON
        result = await func(request, *args, **kwargs)
        return JSONResponse(content=result.model_dump(mode="json"))
    return wrapper

async def parse_request_body(request: Request, model):
    """Parsing request body (CBOR/JSON) dan validasi dengan Pydantic."""
    content_type = request.headers.get("content-type", "")

    try:
        if content_type == "application/cbor":
            body = await request.body()
            if not body:
                raise ValueError("Empty request body")
            data = cbor2.loads(body)
        else:  # Default ke JSON
            data = await request.json()
        
        return model(**data)  # Validasi dengan Pydantic
    except Exception as e:
        raise ValueError(f"Invalid request format: {str(e)}")
        
def hash_password(password: str, salt=None) -> tuple:
    usalt = bcrypt.gensalt() if salt == None else salt
    hashed_password = bcrypt.hashpw(password.encode('utf-8'), usalt)
    return usalt, hashed_password

def generate_password(length: int = 8):
    characters = string.ascii_letters + string.digits + string.punctuation
    password = ''.join(secrets.choice(characters) for _ in range(length))
    return password
 
def get_enum(mongo, enum_id):
    collection = mongo._db["_enum"]
    try:
        enum = collection.find_one({"_id": enum_id})
        return enum
    except PyMongoError as pme:
        raise ValueError("Database error occurred while find document.") from pme
    except Exception as e:
        raise

def is_none(variable, default_value):
    return default_value if variable is None else variable