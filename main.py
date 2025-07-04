import uvicorn
from baseapp.config import setting
from baseapp.app import app

config = setting.get_settings()
port = config.port if not config.port else 1899

uvicorn.run(app, host="0.0.0.0", port=port)
