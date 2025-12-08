import logging
import sys
import json
from datetime import datetime
from contextvars import ContextVar

# --- 1. CONTEXT VARIABLES (Untuk Tracing) ---
# Variable ini "hidup" selama satu request berlangsung
request_id_ctx = ContextVar("request_id", default="-")
user_id_ctx = ContextVar("user_id", default="guest")
ip_address_ctx = ContextVar("ip_address", default="-")

# --- 2. CUSTOM JSON FORMATTER ---
class JSONFormatter(logging.Formatter):
    def format(self, record):
        # Ambil context data
        log_record = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger_name": record.name,
            "module": record.module,
            "func_name": record.funcName,
            "line_no": record.lineno,
            # Context tracing (otomatis terisi)
            "log_id": request_id_ctx.get(),
            "user": user_id_ctx.get(),
            "ip": ip_address_ctx.get(),
        }

        # Jika ada error exception, masukkan stack trace
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_record)

def get_logging_config():
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "json": {
                "()": JSONFormatter, # Menggunakan class custom kita
            },
            "simple": { # Backup jika ingin text biasa
                "format": "%(asctime)s - %(levelname)s - %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S"
            }
        },
        "handlers": {
            "consoleHandler": {
                "class": "logging.StreamHandler",
                "level": "DEBUG",
                "formatter": "json", # Ubah ke 'simple' jika ingin baca di terminal biasa
                "stream": sys.stdout,
            },
            "appHandler": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "DEBUG",
                "formatter": "json",
                "filename": "log/app.log",
                "maxBytes": 10*1024*1024, # 10MB
                "backupCount": 5,
                "encoding": "utf-8"
            },
            "rabbitHandler": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "DEBUG",
                "formatter": "json",
                "filename": "log/rabbit.log",
                "maxBytes": 10*1024*1024,
                "backupCount": 5,
                "encoding": "utf-8"
            },
            "cronHandler": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "DEBUG",
                "formatter": "json",
                "filename": "log/cron.log",
                "maxBytes": 10*1024*1024,
                "backupCount": 5,
                "encoding": "utf-8"
            }
        },
        "loggers": {
            "root": {
                "level": "DEBUG",
                "handlers": ["consoleHandler", "appHandler"],
            },
            "rabbit": {
                "level": "DEBUG",
                "handlers": ["consoleHandler", "rabbitHandler"],
                "propagate": False
            },
            "cronjob": {
                "level": "DEBUG",
                "handlers": ["consoleHandler", "cronHandler"],
                "propagate": False
            }
        }
    }