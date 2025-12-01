import argparse, json
from baseapp.config.rabbitmq import RabbitMqConn

# Importing the worker class
from baseapp.services._rabbitmq_worker._webhook_worker import WebhookWorker

import logging.config
logging.config.fileConfig('logging.conf')
from logging import getLogger
logger = getLogger("rabbit")

WORKER_MAP = {
    "webhook_tasks": WebhookWorker,
    # Tambahkan worker lain di sini
}

def start_consuming(queue_name: str, worker_instance):
    """
    Memulai worker untuk mendengarkan pesan dari antrian secara terus-menerus.
    """
    try:
        # Gunakan context manager untuk koneksi yang andal
        with RabbitMqConn() as channel:
            # Deklarasi antrian yang andal, harus cocok dengan publisher
            channel.queue_declare(queue=queue_name, durable=True, auto_delete=False)
            
            # Ambil 1 pesan per worker pada satu waktu untuk distribusi beban yang adil
            channel.basic_qos(prefetch_count=1)

            # Definisikan fungsi callback di dalam scope ini agar bisa mengakses 'channel'
            def callback(ch, method, properties, body):
                try:
                    task_data = json.loads(body)
                    logger.info(f"New task received for worker {worker_instance.__class__.__name__}")
                    # Delegasikan tugas ke method process() dari worker yang sesuai
                    worker_instance.process(task_data)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    logger.info("Task successfully processed and acknowledged.")

                except Exception as e:
                    logger.error(f"Terjadi error saat memproses tugas: {e}")
                    # Tolak pesan (nack) dan jangan masukkan kembali ke antrian (requeue=False)
                    # Ini akan mengirim pesan ke Dead Letter Exchange jika dikonfigurasi
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            # Mulai proses consuming dengan manual acknowledgement
            channel.basic_consume(
                queue=queue_name,
                on_message_callback=callback,
                auto_ack=False # Sangat penting untuk keandalan
            )

            logger.info(f"[*] Worker '{worker_instance.__class__.__name__}' ready for queue '{queue_name}'. To exit press CTRL+C")
            channel.start_consuming()

    except KeyboardInterrupt:
        logger.info("Worker stopped by user.")
    except Exception as e:
        logger.error(f"RabbitMQ connection failed: {e}")


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
    args = parser.parse_args()
    queue_name = args.queue

    WorkerClass = WORKER_MAP.get(queue_name)
    if not WorkerClass:
        logger.error(f"No worker configured for queue: '{queue_name}'")
        exit(1)

    worker_instance = WorkerClass()

    # Jalankan consumer dengan instance worker tersebut
    start_consuming(queue_name=args.queue, worker_instance=worker_instance)