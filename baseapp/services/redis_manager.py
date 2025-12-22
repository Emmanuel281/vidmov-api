import argparse
import time
import sys
import signal
import logging.config
from baseapp.config.logging import get_logging_config
from baseapp.config.redis import RedisConn
from baseapp.services.redis_queue import RedisQueueManager

logging.config.dictConfig(get_logging_config())
logger = logging.getLogger("baseapp.services.redis_manager")

# Importing the worker classes
from baseapp.services._redis_worker.email_worker import EmailWorker
from baseapp.services._redis_worker.delete_file_worker import DeleteFileWorker

WORKER_MAP = {
    "otp_tasks": EmailWorker,
    "minio_delete_file_tasks": DeleteFileWorker
}

worker = None

def signal_handler(signum, frame):
    """Handle termination signals"""
    logger.info(f"Received signal {signum}. Shutting down gracefully...")
    if worker:
        worker.stop()
    sys.exit(0)

if __name__ == "__main__":
    # Setup signal handlers untuk graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Buat parser untuk argumen command-line
    parser = argparse.ArgumentParser(description="Redis Worker Manager")
    parser.add_argument(
        '--queue', 
        type=str, 
        required=True, 
        choices=WORKER_MAP.keys(),
        help="Nama antrian yang akan di-consume."
    )
    parser.add_argument(
        '--max-retries',
        type=int,
        default=3,
        help="Maximum consecutive errors before worker exits (default: 3)"
    )
    parser.add_argument(
        '--health-check-interval',
        type=int,
        default=5,
        help="Interval in seconds to check worker health (default: 5)"
    )
    args = parser.parse_args()
    queue_name = args.queue

    # Dapatkan class Worker yang sesuai dari map
    WorkerClass = WORKER_MAP.get(queue_name)
    if not WorkerClass:
        logger.error(f"No worker class found for queue: '{queue_name}'")
        sys.exit(1)

    logger.info(f"Starting {WorkerClass.__name__} for queue: '{queue_name}'...")

    try:
        redis_conn = RedisConn()
        queue_manager = RedisQueueManager(redis_conn=redis_conn, queue_name=queue_name)
        worker = WorkerClass(queue_manager, max_retries=args.max_retries)
        worker.start()
        
        logger.info(f"Worker started. Health check interval: {args.health_check_interval}s")
        
        # Main loop dengan health check
        while True:
            time.sleep(args.health_check_interval)
            
            # Periksa apakah worker thread masih hidup
            if not worker.is_alive():
                logger.critical("Worker thread has died unexpectedly. Exiting container.")
                sys.exit(1)
                
    except KeyboardInterrupt:
        logger.info(f"Stopping Redis worker for queue: '{queue_name}'...")
        if worker:
            worker.stop()
        logger.info("Worker stopped gracefully.")
        sys.exit(0)
        
    except Exception as e:
        logger.critical(f"Fatal error initializing worker: {e}")
        if worker:
            worker.stop()
        sys.exit(1)