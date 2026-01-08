import os
import time
import logging
import subprocess
import json
import tempfile
import argparse
import redis
from minio import Minio
from minio.error import S3Error

# Try importing config, handle failure gracefully
try:
    from baseapp.config import redis as redis_config
except ImportError:
    redis_config = None

try:
    from baseapp.config import minio as minio_config
except ImportError:
    minio_config = None

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("video_worker")

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

def get_minio_client():
    endpoint = get_conf(minio_config, 'MINIO_ENDPOINT', 'MINIO_ENDPOINT', 'minio:9000')
    access_key = get_conf(minio_config, 'MINIO_ACCESS_KEY', 'MINIO_ACCESS_KEY', 'minioadmin')
    secret_key = get_conf(minio_config, 'MINIO_SECRET_KEY', 'MINIO_SECRET_KEY', 'minioadmin')
    
    secure_val = get_conf(minio_config, 'MINIO_USE_SSL', 'MINIO_USE_SSL', 'False')
    if isinstance(secure_val, str):
        secure = secure_val.lower() == 'true'
    else:
        secure = bool(secure_val)

    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure
    )

def convert_and_upload(client, bucket, object_name, output_bucket, output_prefix):
    """
    Downloads file, converts to HLS using ffmpeg, uploads back to MinIO, and benchmarks.
    """
    start_time = time.time()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        input_file = os.path.join(temp_dir, "input_video")
        output_hls_dir = os.path.join(temp_dir, "hls")
        os.makedirs(output_hls_dir, exist_ok=True)
        
        # 1. DOWNLOAD
        logger.info(f"Downloading {bucket}/{object_name}...")
        try:
            client.fget_object(bucket, object_name, input_file)
        except S3Error as e:
            logger.error(f"MinIO Download Error: {e}")
            return {"status": "error", "stage": "download", "error": str(e)}

        download_time = time.time()
        
        # 2. CONVERT
        playlist_file = os.path.join(output_hls_dir, "playlist.m3u8")
        segment_filename = os.path.join(output_hls_dir, "segment_%03d.ts")
        
        cmd = [
            "ffmpeg",
            "-i", input_file,
            "-c:v", "libx264",
            "-c:a", "aac",
            "-hls_time", "10",
            "-hls_list_size", "0",
            "-hls_segment_filename", segment_filename,
            "-f", "hls",
            playlist_file
        ]
        
        logger.info("Starting FFmpeg conversion...")
        try:
            subprocess.run(
                cmd, 
                check=True, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE
            )
        except subprocess.CalledProcessError as e:
            err_msg = e.stderr.decode() if e.stderr else str(e)
            logger.error(f"FFmpeg Error: {err_msg}")
            return {"status": "error", "stage": "conversion", "error": err_msg}
            
        conversion_time = time.time()

        # 3. UPLOAD
        logger.info(f"Uploading results to {output_bucket}/{output_prefix}...")
        uploaded_files = 0
        try:
            for root, _, files in os.walk(output_hls_dir):
                for filename in files:
                    local_path = os.path.join(root, filename)
                    remote_path = os.path.join(output_prefix, filename) 
                    
                    content_type = "application/octet-stream"
                    if filename.endswith(".m3u8"):
                        content_type = "application/x-mpegURL"
                    elif filename.endswith(".ts"):
                        content_type = "video/MP2T"

                    client.fput_object(
                        output_bucket, 
                        remote_path, 
                        local_path, 
                        content_type=content_type
                    )
                    uploaded_files += 1
        except S3Error as e:
             logger.error(f"MinIO Upload Error: {e}")
             return {"status": "error", "stage": "upload", "error": str(e)}

        end_time = time.time()
        
        metrics = {
            "status": "success",
            "total_duration": end_time - start_time,
            "download_duration": download_time - start_time,
            "conversion_duration": conversion_time - download_time,
            "upload_duration": end_time - conversion_time,
            "files_uploaded": uploaded_files
        }
        
        logger.info(f"Task Completed. Metrics: {metrics}")
        return metrics

def run_worker(queue_name):
    r = get_redis_connection()
    try:
        minio_client = get_minio_client()
    except Exception as e:
        logger.error(f"Failed to initialize MinIO client: {e}")
        return

    logger.info(f"MinIO Video Worker started. Listening on '{queue_name}'...")
    
    while True:
        try:
            task = r.brpop(queue_name, timeout=5)
            
            if task:
                _, data_json = task
                logger.info(f"Received task: {data_json}")
                
                try:
                    data = json.loads(data_json)
                    bucket = data.get("bucket")
                    object_name = data.get("object_name")
                    output_bucket = data.get("output_bucket", bucket)
                    output_prefix = data.get("output_prefix", "hls_output/")
                    
                    if bucket and object_name:
                        convert_and_upload(minio_client, bucket, object_name, output_bucket, output_prefix)
                    else:
                        logger.warning("Invalid task: Missing bucket or object_name")
                        
                except json.JSONDecodeError:
                    logger.error("Failed to decode task JSON")
                except Exception as e:
                    logger.error(f"Error processing task: {e}")
                    
        except redis.ConnectionError:
            logger.error("Redis connection lost. Retrying in 5s...")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Unexpected worker error: {e}")
            time.sleep(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Video Conversion Worker")
    parser.add_argument("--queue", default="video_conversion_queue", help="Redis queue name to listen on")
    
    args = parser.parse_args()
    
    run_worker(args.queue)