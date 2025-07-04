import json
from baseapp.config.redis import RedisConn

import logging
logger = logging.getLogger()

class RedisQueueManager:
    def __init__(self, queue_name: str):
        self.redis_conn = RedisConn()
        self.queue_name = queue_name

    def enqueue_task(self, data: dict):
        """
        Push a task to the Redis queue.
        """
        with self.redis_conn as conn:
            conn.lpush(self.queue_name, json.dumps(data))
            logger.info(f"Task added to queue: {data}")

    def dequeue_task(self):
        """
        Pop a task from the Redis queue.
        """
        with self.redis_conn as conn:
            task = conn.rpop(self.queue_name)
            return json.loads(task) if task else None
