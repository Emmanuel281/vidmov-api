import os

from baseapp.config import setting
config = setting.get_settings()

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from baseapp.model.common import REDIS_QUEUE_BASE_KEY
from baseapp.services.middleware import setup_middleware

os.makedirs("log", exist_ok=True) # create log folder

import logging.config
logging.config.fileConfig('logging.conf')
from logging import getLogger
logger = getLogger()

from baseapp.test_connection.api import router as testconn_router # test connection
from baseapp.services.database.api import router as db_router # init database
from baseapp.services._enum.api import router as enum_router # enum
from baseapp.services._org.api import router as org_router # organization
from baseapp.services.auth.api import router as auth_router # auth
from baseapp.services.profile.api import router as profile_router # profile
from baseapp.services._menu.api import router as menu_router # menu
from baseapp.services._role.api import router as role_router # role
from baseapp.services._user.api import router as user_router # user
from baseapp.services._dms.index_list.api import router as index_router # index dms
from baseapp.services._dms.doc_type.api import router as doctype_router # doctype dms
from baseapp.services._dms.upload.api import router as upload_router # upload dms
from baseapp.services._dms.browse.api import router as browse_router # browse dms
from baseapp.services._feature.api import router as feature_router # feature and role
from baseapp.services._forgot_password.api import router as forgot_password_router # forgot password
# from baseapp.services.gai_ai.api import router as gai_ai_router # GAI AI
from baseapp.services.oauth_google.api import router as oauth_google_router # Oauth Google
from baseapp.services.content.api import router as content_router # Video Content Management
from baseapp.services.content_detail.api import router as content_detail_router # Video Content Detail

from baseapp.services.redis_queue import RedisQueueManager
from baseapp.services.redis_worker import RedisWorker

# Redis connection and queue configuration
queue_manager = RedisQueueManager(queue_name=REDIS_QUEUE_BASE_KEY)

# Worker setup
worker = RedisWorker(queue_manager)
worker.start()

app = FastAPI(
    title="vidmov API",
    description="Gateway for vidmov implementation.",
    version="0.0.1",
)

allowed_origins = [
    "http://localhost:53464",
    "http://vidmov.localhost",
    "https://gai.co.id",
    "https://vidmov.gai.co.id"
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
app.include_router(db_router)
app.include_router(enum_router)
app.include_router(org_router)
app.include_router(auth_router)
app.include_router(profile_router)
app.include_router(menu_router)
app.include_router(role_router)
app.include_router(user_router)
app.include_router(index_router)
app.include_router(doctype_router)
app.include_router(upload_router)
app.include_router(browse_router)
app.include_router(feature_router)
app.include_router(forgot_password_router)
# app.include_router(gai_ai_router)
app.include_router(oauth_google_router)
app.include_router(content_router)
app.include_router(content_detail_router)

@app.get("/v1/test")
def read_root():
    return "ok"

@app.on_event("shutdown")
def shutdown_worker():
    """
    Gracefully stop the worker on server shutdown.
    """
    worker.stop()