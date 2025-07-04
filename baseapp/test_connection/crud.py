import logging
import traceback
from baseapp.config import setting, redis, mongodb, minio, rabbitmq, clickhouse

config = setting.get_settings()
logger = logging.getLogger()

def test_connection_to_redis():
    logger.info("Redis test connection")
    client = redis.RedisConn()
    with client as redis_conn:
        redis_conn.set("test_connection", "test", 60)
        res = redis_conn.get("test_connection")
        redis_conn.delete("test_connection", "test")
        result = f"redis: {res}"
        logger.info(result)
        return result
    
def test_connection_to_mongodb():
    logger.info("Mongodb test connection")
    client = mongodb.MongoConn()
    with client as mongo_conn:
        server_info = mongo_conn.get_connection().server_info()
        result = f"mongodb: {server_info.get('version', 'Unknown')}"
        logger.info(result)
        return result
    
def test_connection_to_minio():
    logger.info("Minio test connection")
    client = minio.MinioConn()
    with client as minio_conn:
        try:
            if minio_conn.bucket_exists():
                return "Bucket exist"
            else:
                return "Bucket not exist"
        except Exception as err:
            logger.exception(f"Unexpected error occurred while testing minio connection. {str(err)}")
            raise
    
def test_connection_to_rabbit():
    try:
        logger.info("RabbitMQ test connection")
        client = rabbitmq.RabbitMqConn()
        with client as rabbit_conn:
            rabbit_conn.queue_declare(queue='test_queue', durable=False, auto_delete=True)
            logger.info("RabbitMQ: Connection successful. Queue 'test_queue' declared.")
            return "RabbitMQ: Connection successful. Queue 'test_queue' declared."
    except Exception as e:
        logger.error(f"RabbitMQ operation failed: {e}")
        raise
    
def test_connection_to_clickhouse():
    logger.info("Clickhouse test connection")
    client = clickhouse.ClickHouseConn()
    with client as clickhouse_conn:
        result = clickhouse_conn.query("SELECT 1")
        if result:
            logging.info("ClickHouse: Connection successful")
            return "ClickHouse: Connection successful"
        else:
            logging.error("ClickHouse: Connection test failed")
            raise ValueError("ClickHouse: Connection test failed")