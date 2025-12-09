import logging
import sys
import json
from datetime import datetime
from contextvars import ContextVar

# --- 1. CONTEXT VARIABLES (Untuk Tracing) ---
request_id_ctx = ContextVar("request_id", default="-")
user_id_ctx = ContextVar("user_id", default="guest")
ip_address_ctx = ContextVar("ip_address", default="-")

# --- 2. CUSTOM JSON FORMATTER ---
class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger_name": record.name,
            "module": record.module,
            "func_name": record.funcName,
            "line_no": record.lineno,
            "log_id": request_id_ctx.get(),
            "user": user_id_ctx.get(),
            "ip": ip_address_ctx.get(),
        }

        # Jika ada error exception, masukkan stack trace
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_record)

# --- 3. FILTER UNTUK MENYARING LOG LIBRARY EKSTERNAL ---
class AppOnlyFilter(logging.Filter):
    """
    Filter untuk hanya menampilkan log dari aplikasi kita sendiri.
    Menyaring log dari library eksternal (pymongo, fastapi, uvicorn, dll)
    """
    def __init__(self, app_prefix="baseapp"):
        super().__init__()
        self.app_prefix = app_prefix
        # Daftar library yang ingin di-filter (disembunyikan)
        self.blocked_loggers = [
            "pymongo",
            "uvicorn",
            "fastapi",
            # "aio_pika",
            # "aiormq",
            # "jose",
            # "passlib",
            # "multipart",
            # "httpx",
            # "httpcore",
        ]
    
    def filter(self, record):
        # Hanya tampilkan log dari aplikasi kita (baseapp)
        if record.name.startswith(self.app_prefix):
            return True
        
        # Blokir log dari library eksternal
        for blocked in self.blocked_loggers:
            if record.name.startswith(blocked):
                return False
        
        # Untuk logger lain yang tidak termasuk baseapp atau blocked,
        # hanya tampilkan WARNING ke atas
        return record.levelno >= logging.WARNING

def get_logging_config():
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "json": {
                "()": JSONFormatter,
            },
            "simple": {
                "format": "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S"
            }
        },
        "filters": {
            "app_only": {
                "()": AppOnlyFilter,
                "app_prefix": "baseapp"  # Sesuaikan dengan prefix aplikasi Anda
            }
        },
        "handlers": {
            "consoleHandler": {
                "class": "logging.StreamHandler",
                "level": "INFO",  # Ubah ke INFO agar DEBUG tidak tampil
                "formatter": "json",
                "stream": sys.stdout,
                "filters": ["app_only"]  # Tambahkan filter
            },
            "appHandler": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "INFO",  # INFO level untuk production
                "formatter": "json",
                "filename": "log/app.log",
                "maxBytes": 10*1024*1024,
                "backupCount": 5,
                "encoding": "utf-8",
                "filters": ["app_only"]
            },
            "debugHandler": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "DEBUG",  # Handler khusus untuk DEBUG
                "formatter": "json",
                "filename": "log/debug.log",
                "maxBytes": 10*1024*1024,
                "backupCount": 3,
                "encoding": "utf-8",
                "filters": ["app_only"]
            },
            "errorHandler": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "ERROR",  # Handler khusus untuk ERROR
                "formatter": "json",
                "filename": "log/error.log",
                "maxBytes": 10*1024*1024,
                "backupCount": 5,
                "encoding": "utf-8",
                "filters": ["app_only"]
            }
        },
        "loggers": {
            # Root logger - untuk semua log aplikasi
            "": {
                "level": "DEBUG",  # Capture semua level
                "handlers": ["consoleHandler", "appHandler", "debugHandler", "errorHandler"],
            },
            # Logger spesifik untuk aplikasi kita
            "baseapp": {
                "level": "DEBUG",
                "handlers": ["consoleHandler", "appHandler", "debugHandler", "errorHandler"],
                "propagate": False
            },
            # Matikan log DEBUG dari library eksternal
            "pymongo": {
                "level": "WARNING",
                "handlers": [],
                "propagate": False
            },
            "uvicorn": {
                "level": "WARNING",
                "handlers": [],
                "propagate": False
            },
            "fastapi": {
                "level": "WARNING",
                "handlers": [],
                "propagate": False
            }
        }
    }