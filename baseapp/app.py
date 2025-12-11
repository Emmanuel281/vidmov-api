import os
import logging.config
from contextlib import asynccontextmanager

from baseapp.config import setting
config = setting.get_settings()

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from baseapp.config.logging import get_logging_config, request_id_ctx, user_id_ctx, ip_address_ctx
from baseapp.utils.logger import Logger
from baseapp.services.middleware import setup_middleware

# Setup logging
os.makedirs("log", exist_ok=True)
logging.config.dictConfig(get_logging_config())
logger = Logger("baseapp.app")

from baseapp.config.mongodb import MongoConn
from baseapp.config.postgresql import PostgreSQLConn

from baseapp.test_connection.api import router as testconn_router # test connection
from baseapp.services._enum.api import router as enum_router # enum
from baseapp.services._org.api import router as org_router # organization
from baseapp.services.auth.api import router as auth_router # auth
from baseapp.services.profile.api import router as profile_router # profile
from baseapp.services._role.api import router as role_router # role
from baseapp.services._user.api import router as user_router # user
from baseapp.services._dms.index_list.api import router as index_router # index dms
from baseapp.services._dms.doc_type.api import router as doctype_router # doctype dms
from baseapp.services._dms.upload.api import router as upload_router # upload dms
from baseapp.services._dms.browse.api import router as browse_router # browse dms
from baseapp.services._feature.api import router as feature_router # feature and role
from baseapp.services._forgot_password.api import router as forgot_password_router # forgot password
from baseapp.services.oauth_google.api import router as oauth_google_router # Oauth Google
from baseapp.services._api_credentials.api import router as api_credential_router # API Credentials

from baseapp.services.content.api import router as content_router # Video Content Management
from baseapp.services.content_detail.api import router as content_detail_router # Video Content Detail

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. BAGIAN STARTUP (Dijalankan sebelum aplikasi menerima request)
    req_token = request_id_ctx.set("system-startup")
    user_token = user_id_ctx.set("system")
    ip_token = ip_address_ctx.set("localhost")
    logger.info("Startup: Initializing resources...")
    
    try:
        # Init MongoDB
        MongoConn.initialize()
        logger.info("MongoDB Connection Pool initialized.")
        
        # Init PostgreSQL (Jika pakai)
        PostgreSQLConn.initialize_pool()
        logger.info("PostgreSQL Connection Pool initialized.")
        
    except Exception as e:
        logger.error(f"Startup Failed: {e}")
        # Opsional: raise e # Uncomment jika ingin app crash kalau DB mati
    
    yield # <--- Titik tunggu (Aplikasi berjalan di sini)

    # 2. BAGIAN SHUTDOWN (Dijalankan saat aplikasi mau mati)
    request_id_ctx.set("system-shutdown")
    logger.info("Shutdown: Cleaning up resources...")
    
    try:
        MongoConn.close_connection()
        PostgreSQLConn.close_pool()
        
    except Exception as e:
        logger.error(f"Shutdown error: {e}")
    
    request_id_ctx.reset(req_token)
    user_id_ctx.reset(user_token)
    ip_address_ctx.reset(ip_token)
    logger.info("Resources cleaned up.")

app = FastAPI(
    title="Arena API",
    description="Gateway for Arena implementation.",
    version="0.0.1",
)

allowed_origins = [
    "http://localhost:53464",
    "http://arena.localhost",
    "https://gai.co.id",
    "https://arena.gai.co.id"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

os.makedirs(config.file_location, exist_ok=True) # create folder data/files

setup_middleware(app)

app.include_router(testconn_router)
app.include_router(enum_router)
app.include_router(org_router)
app.include_router(auth_router)
app.include_router(profile_router)
app.include_router(role_router)
app.include_router(user_router)
app.include_router(index_router)
app.include_router(doctype_router)
app.include_router(upload_router)
app.include_router(browse_router)
app.include_router(feature_router)
app.include_router(forgot_password_router)
app.include_router(oauth_google_router)
app.include_router(api_credential_router)
# Video Content Management
app.include_router(content_router)
app.include_router(content_detail_router)

@app.get("/v1/test")
def read_root():
    return "ok"