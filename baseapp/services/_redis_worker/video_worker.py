import os
import time
import logging
import subprocess
import json
import tempfile
import redis
from minio import Minio
from minio.error import S3Error
from baseapp.config import redis as redis_config
from baseapp.config import minio as minio_config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("video_worker")

def get_redis_connection():
    return redis.Redis(
        host=redis_config.REDIS_HOST,
        port=redis_config.REDIS_PORT,
        db=redis_config.REDIS_DB,
        password=redis_config.REDIS_PASSWORD,
        decode_responses=True
    )

def get_minio_client():
    """Initialize MinIO client using project config"""
    # Mengambil konfigurasi MinIO, dengan fallback ke env var jika perlu
    endpoint = getattr(minio_config, 'MINIO_ENDPOINT', os.getenv('MINIO_ENDPOINT', 'minio:9000'))
    access_key = getattr(minio_config, 'MINIO_ACCESS_KEY', os.getenv('MINIO_ACCESS_KEY', 'minioadmin'))
    secret_key = getattr(minio_config, 'MINIO_SECRET_KEY', os.getenv('MINIO_SECRET_KEY', 'minioadmin'))
    
    # Perbaikan: pastikan secure adalah boolean
    secure_val = getattr(minio_config, 'MINIO_USE_SSL', os.getenv('MINIO_USE_SSL', 'False'))
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
    
    # Gunakan temporary directory agar file otomatis terhapus setelah selesai
    with tempfile.TemporaryDirectory() as temp_dir:
        input_file = os.path.join(temp_dir, "input_video")
        output_hls_dir = os.path.join(temp_dir, "hls")
        os.makedirs(output_hls_dir, exist_ok=True)
        
        # 1. DOWNLOAD dari MinIO
        logger.info(f"Downloading {bucket}/{object_name}...")
        try:
            client.fget_object(bucket, object_name, input_file)
        except S3Error as e:
            logger.error(f"MinIO Download Error: {e}")
            return {"status": "error", "stage": "download", "error": str(e)}

        download_time = time.time()
        
        # 2. CONVERT dengan FFmpeg
        playlist_file = os.path.join(output_hls_dir, "playlist.m3u8")
        segment_filename = os.path.join(output_hls_dir, "segment_%03d.ts")
        
        # Command FFmpeg untuk HLS
        cmd = [
            "ffmpeg",
            "-i", input_file,
            "-c:v", "libx264",     # Video codec H.264
            "-c:a", "aac",         # Audio codec AAC
            "-hls_time", "10",     # Durasi per segmen (detik)
            "-hls_list_size", "0", # Masukkan semua segmen ke playlist
            "-hls_segment_filename", segment_filename,
            "-f", "hls",           # Format HLS
            playlist_file
        ]
        
        # Opsional: Uncomment baris di bawah untuk scaling (misal ke 720p)
        # cmd.insert(2, "-vf")
        # cmd.insert(3, "scale=-2:720")

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

        # 3. UPLOAD ke MinIO
        logger.info(f"Uploading results to {output_bucket}/{output_prefix}...")
        uploaded_files = 0
        try:
            for root, _, files in os.walk(output_hls_dir):
                for filename in files:
                    local_path = os.path.join(root, filename)
                    # Buat path remote, misal: movies/matrix/playlist.m3u8
                    remote_path = os.path.join(output_prefix, filename) 
                    
                    # Tentukan Content-Type
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

def run_worker():
    r = get_redis_connection()
    try:
        minio_client = get_minio_client()
    except Exception as e:
        logger.error(f"Failed to initialize MinIO client: {e}")
        return

    queue_name = "video_conversion_queue"
    
    logger.info(f"MinIO Video Worker started. Listening on '{queue_name}'...")
    
    while True:
        try:
            # Blocking pop - tunggu sampai ada tugas
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
    run_worker()