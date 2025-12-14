import pika, json
from baseapp.config.rabbitmq import RabbitMqConn
from baseapp.utils.logger import Logger

logger = Logger("baseapp.services.publisher")

def publish_message(queue_name: str, task_data: dict):
    """
    Membuka koneksi, mengirim satu pesan ke antrian, lalu menutup koneksi.
    Fungsi ini sekarang mandiri dan tidak memerlukan class.

    Args:
        queue_name (str): Nama antrian tujuan.
        task_data (dict): Data tugas yang akan dikirim.
    """
    try:
        # Gunakan context manager untuk koneksi yang aman
        with RabbitMqConn() as channel:
            # Deklarasi antrian yang andal (durable dan tidak auto-delete)
            channel.queue_declare(queue=queue_name, durable=True, auto_delete=False)

            message_body = json.dumps(task_data)

            # Mengaktifkan konfirmasi pengiriman (Publisher Confirms)
            channel.confirm_delivery()

            # Kirim pesan dengan mode persistent
            channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=message_body,
                properties=pika.BasicProperties(
                    content_type='application/json',
                    delivery_mode=pika.DeliveryMode.Persistent # Pesan tidak akan hilang jika RabbitMQ restart
                ),
                mandatory=True
            )
            logger.info(f"Pesan berhasil dikirim ke antrian '{queue_name}'")

    except pika.exceptions.UnroutableError:
        logger.error("Pesan tidak dapat dirutekan. Antrian mungkin tidak ada.")
    except Exception as e:
        logger.error(f"Gagal mengirim pesan ke RabbitMQ: {e}")


if __name__ == "__main__":
    # Contoh penggunaan fungsi publisher
    
    # Data yang akan dikirim
    objData = {
        "event_type": "payment.succeeded",
        "event_data": {"payment_id": "pay_12345", "amount": 50000},
        "org_id": "org_abcde"
    }

    # Mengirim pesan ke antrian 'webhook_tasks'
    publish_message(queue_name="webhook_tasks", task_data=objData)