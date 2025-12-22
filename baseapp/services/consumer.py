import argparse
import json
import sys
import signal
import logging.config
from baseapp.config.logging import get_logging_config
from baseapp.config.rabbitmq import RabbitMqConn
from baseapp.utils.logger import Logger

logging.config.dictConfig(get_logging_config())
logger = Logger("baseapp.services.consumer")

# Importing the worker class
from baseapp.services._rabbitmq_worker.webhook_worker import WebhookWorker

WORKER_MAP = {
    "webhook_tasks": WebhookWorker,
    # Tambahkan worker lain di sini
}

# Global variables untuk cleanup
channel = None
worker_instance = None

def signal_handler(signum, frame):
    """Handle termination signals"""
    logger.info(f"Received signal {signum}. Shutting down gracefully...")
    if channel:
        try:
            channel.stop_consuming()
        except Exception as e:
            logger.error(f"Error stopping consumer: {e}")
    sys.exit(0)

def start_consuming(queue_name: str, worker_instance, requeue_on_error: bool = True):
    """
    Memulai worker untuk mendengarkan pesan dari antrian secara terus-menerus.
    
    Args:
        queue_name: Nama antrian RabbitMQ
        worker_instance: Instance dari worker class
        requeue_on_error: Jika True, message akan di-requeue saat infrastructure error
    """
    try:
        # Setup signal handlers
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

        # Gunakan context manager untuk koneksi yang andal
        with RabbitMqConn() as ch:
            channel = ch

            # Deklarasi antrian yang andal, harus cocok dengan publisher
            channel.queue_declare(queue=queue_name, durable=True, auto_delete=False)
            
            # Ambil 1 pesan per worker pada satu waktu untuk distribusi beban yang adil
            channel.basic_qos(prefetch_count=1)

            # Definisikan fungsi callback di dalam scope ini agar bisa mengakses 'channel'
            def callback(ch, method, properties, body):
                try:
                    task_data = json.loads(body)
                    logger.info(
                        f"New task received",
                        worker=worker_instance.__class__.__name__,
                        queue=queue_name
                    )
                    # Delegasikan ke worker.process() yang sudah ada error handling
                    success = worker_instance.process(task_data)
                    if success:
                        # Task berhasil - acknowledge
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        logger.info("Task successfully processed and acknowledged")
                    else:
                        # Task gagal validasi - reject tanpa requeue
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                        logger.warning("Task rejected due to validation error (no requeue)")

                except ValueError as ve:
                    # Validation error - reject tanpa requeue
                    logger.warning(f"Task validation failed: {ve}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

                except ConnectionError as ce:
                    # Infrastructure error - nack dengan requeue
                    logger.error(f"Connection error processing task: {ce}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=requeue_on_error)
                    # Worker akan terus jalan, error counter sudah di-track di BaseWorker
                except Exception as e:
                    logger.error(f"Unexpected error processing task: {e}")
                    # Tolak pesan (nack) dan jangan masukkan kembali ke antrian (requeue=False)
                    # Ini akan mengirim pesan ke Dead Letter Exchange jika dikonfigurasi
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            # Mulai proses consuming dengan manual acknowledgement
            channel.basic_consume(
                queue=queue_name,
                on_message_callback=callback,
                auto_ack=False # Sangat penting untuk keandalan
            )

            logger.info(
                f"Worker ready and waiting for messages",
                worker=worker_instance.__class__.__name__,
                queue=queue_name,
                max_consecutive_errors=worker_instance.max_consecutive_errors
            )
            logger.info("To exit press CTRL+C")
            
            channel.start_consuming()

    except KeyboardInterrupt:
        logger.info("Worker stopped by user.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"RabbitMQ connection failed: {e}")
        sys.exit(0)


if __name__ == "__main__":
    # Buat parser untuk argumen command-line
    parser = argparse.ArgumentParser(description="RabbitMQ Consumer Worker")
    parser.add_argument(
        '--queue', 
        type=str, 
        required=True,
        choices=WORKER_MAP.keys(),
        help="Nama antrian yang akan di-consume."
    )
    parser.add_argument(
        '--max-errors',
        type=int,
        default=3,
        help="Maximum consecutive errors before worker exits (default: 3)"
    )
    parser.add_argument(
        '--no-requeue',
        action='store_true',
        help="Do not requeue messages on infrastructure errors"
    )
    args = parser.parse_args()

    queue_name = args.queue
    WorkerClass = WORKER_MAP.get(queue_name)

    if not WorkerClass:
        logger.error(f"No worker configured for queue: '{queue_name}'")
        sys.exit(1)

    worker_instance = WorkerClass(max_consecutive_errors=args.max_errors)

    logger.info(
        f"Starting RabbitMQ consumer",
        queue=queue_name,
        worker=WorkerClass.__name__,
        max_consecutive_errors=args.max_errors,
        requeue_on_error=not args.no_requeue
    )

    # Jalankan consumer dengan instance worker tersebut
    start_consuming(
        queue_name=args.queue, 
        worker_instance=worker_instance,
        requeue_on_error=not args.no_requeue
    )