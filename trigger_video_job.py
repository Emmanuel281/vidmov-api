import json
import redis
import sys
import os
import argparse

# Ensure we can import from baseapp
sys.path.append(os.getcwd())

# Try importing config, handle failure gracefully
try:
    from baseapp.config import redis as redis_config
except ImportError:
    redis_config = None

def get_conf(obj, attr, env_name, default):
    """Helper to get config from module attribute or environment variable"""
    if obj and hasattr(obj, attr):
        return getattr(obj, attr)
    return os.getenv(env_name, default)

def get_redis_connection():
    host = get_conf(redis_config, 'REDIS_HOST', 'REDIS_HOST', 'redis')
    port = int(get_conf(redis_config, 'REDIS_PORT', 'REDIS_PORT', 6379))
    db = int(get_conf(redis_config, 'REDIS_DB', 'REDIS_DB', 0))
    password = get_conf(redis_config, 'REDIS_PASSWORD', 'REDIS_PASSWORD', None)

    return redis.Redis(
        host=host,
        port=port,
        db=db,
        password=password,
        decode_responses=True
    )

def push_video_task(bucket, object_name, output_prefix, queue_name="video_conversion_queue"):
    r = get_redis_connection()
    
    payload = {
        "bucket": bucket,
        "object_name": object_name,
        "output_bucket": bucket, # Defaulting output to same bucket
        "output_prefix": output_prefix
    }
    
    # We use LPUSH to add to the list. 
    # The worker uses BRPOP, which takes from the right (FIFO behavior effectively if we push left)
    r.lpush(queue_name, json.dumps(payload))
    
    print(f"Successfully pushed job to '{queue_name}'")
    print(f"Payload: {json.dumps(payload, indent=2)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trigger a video conversion task in Redis")
    parser.add_argument("--bucket", required=True, help="MinIO bucket name")
    parser.add_argument("--file", required=True, help="Path to file in MinIO (e.g., raw/video.mp4)")
    parser.add_argument("--output", required=True, help="Output folder prefix (e.g., hls/video/)")
    parser.add_argument("--queue", default="video_conversion_queue", help="Redis queue name")

    args = parser.parse_args()

    push_video_task(args.bucket, args.file, args.output, args.queue)
    #python3 trigger_video_job.py   --bucket arena   --file 4845cbb7e2384723abeb4ff09bcbf2a1.mp4   --output processed/4845cbb7e2384723abeb4ff09bcbf2a1/