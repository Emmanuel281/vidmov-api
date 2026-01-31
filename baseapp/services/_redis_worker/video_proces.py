import os
import subprocess
import tempfile
import shutil
import time
import json
from pathlib import Path
from datetime import datetime
from contextlib import contextmanager
from typing import Dict, List, Optional
from enum import Enum

from baseapp.services._redis_worker.base_worker import BaseWorker
from pymongo.errors import PyMongoError
from minio.error import S3Error
from baseapp.config import setting, minio, mongodb, redis
from baseapp.utils.logger import Logger

config = setting.get_settings()
logger = Logger("baseapp.services._redis_worker.video_proces")


class DocumentTypeHLS(str, Enum):
    """Document types for HLS conversion"""
    FYP_1 = "31c557f0f4574f7aae55c1b6860a2e19"
    FYP_2 = "3551a74699394f22b21ecf8277befa39"
    VIDEO = "d67d38fe623b40ccb0ddb4671982c0d3"


class VideoWorker(BaseWorker):
    def __init__(self, queue_manager, max_retries: int = 3):
        super().__init__(queue_manager, max_retries)
        self.collection_file = "_dmsfile"
        self.collection_organization = "_organization"
        self.collection_hls_conversion = "_hls_conversion"
        
        # Predefined bitrate map (fallback if auto-detection fails)
        self.bitrate_map = {
            "4K": {"bitrate": 8000000, "width": 3840, "height": 2160},
            "HD": {"bitrate": 2500000, "width": 1280, "height": 720},
            "SD": {"bitrate": 1000000, "width": 854, "height": 480}
        }
    
    def process_task(self, data: dict):
        """
        Process video conversion to HLS format with adaptive bitrate support.
        Expected data format:
        {
            "content_id": "8a66f138350e401d919d9125511fc7ab",
            "file": "76f13ae2f1de431ab7cbf678b028dc8e.mp4",
            "resolution": "HD",
            "language": "EN",
            "doctype": "31c557f0f4574f7aae55c1b6860a2e19",
            "refkey_name": "fyp",
            "episode_id": "optional_for_video_type"  # Only for VIDEO doctype
        }
        """
        start_time = time.time()
        conversion_id = None
        
        logger.info(f"[BENCHMARK] Starting ABR HLS processing: {data}, Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")
        
        # Validate input
        required_fields = ["content_id", "file", "resolution", "doctype"]
        for field in required_fields:
            if not data.get(field):
                logger.error(f"Invalid task data: missing '{field}' field. Data: {data}")
                raise ValueError(f"Missing required field: '{field}'")
        
        content_id = data.get("content_id")
        source_file = data.get("file")
        resolution = data.get("resolution", "SD").upper()
        language = data.get("language", "EN").upper()
        doctype = data.get("doctype")
        refkey_name = data.get("refkey_name", "fyp")
        episode_id = data.get("episode_id")
        
        file_name_without_ext = Path(source_file).stem
        
        # Determine video type and base path
        video_type = self._get_video_type(doctype, episode_id)
        base_hls_path = self._get_base_hls_path(content_id, doctype, episode_id)
        
        logger.info(f"[PROGRESS] Video type: {video_type}, Base path: {base_hls_path}")
        
        # Create temporary directory for processing
        temp_dir = tempfile.mkdtemp(prefix="video_hls_abr_")
        logger.info(f"[PROGRESS] Created temporary directory: {temp_dir}")
        
        # Acquire Redis lock for this content/video_type combination
        lock_key = f"hls:master:{content_id}:{video_type}"
        
        try:
            with self._redis_lock(lock_key, timeout=300):  # 5 minute lock
                logger.info(f"[PROGRESS] Acquired lock: {lock_key}")
                
                # Create MongoDB tracking record
                conversion_id = self._create_conversion_record(
                    content_id=content_id,
                    video_type=video_type,
                    resolution=resolution,
                    language=language,
                    source_file=source_file,
                    source_file_id=file_name_without_ext,
                    hls_path=f"{base_hls_path}/{resolution.lower()}/"
                )
                
                with minio.MinioConn() as minio_client:
                    # Step 1: Download video from MinIO (with retry)
                    download_start = time.time()
                    logger.info(f"[PROGRESS] Step 1/5: Downloading video from MinIO: {source_file}")
                    
                    local_video_path = os.path.join(temp_dir, source_file)
                    file_size_mb = self._download_with_retry(minio_client, source_file, local_video_path)
                    
                    download_time = time.time() - download_start
                    logger.info(
                        f"[BENCHMARK] Download completed in {download_time:.2f}s "
                        f"({file_size_mb:.2f} MB at {file_size_mb/download_time:.2f} MB/s)"
                    )
                    
                    # Step 2: Get video metadata
                    logger.info("[PROGRESS] Step 2/5: Analyzing video metadata")
                    video_info = self._get_video_info(local_video_path, resolution)
                    logger.info(f"[BENCHMARK] Video info: {video_info}")
                    
                    # Step 3: Convert to HLS
                    hls_output_dir = os.path.join(temp_dir, "hls_output")
                    os.makedirs(hls_output_dir, exist_ok=True)
                    
                    conversion_start = time.time()
                    logger.info("[PROGRESS] Step 3/5: Converting video to HLS format")
                    
                    self._convert_to_hls(
                        local_video_path,
                        hls_output_dir,
                        resolution.lower()  # Use resolution as base name
                    )
                    
                    conversion_time = time.time() - conversion_start
                    logger.info(
                        f"[BENCHMARK] HLS conversion completed in {conversion_time:.2f}s "
                        f"({conversion_time/60:.2f} minutes)"
                    )
                    
                    # Step 4: Upload HLS files to resolution-specific folder
                    upload_start = time.time()
                    minio_dest_path = f"{base_hls_path}/{resolution.lower()}"
                    logger.info(f"[PROGRESS] Step 4/5: Uploading HLS files to MinIO: {minio_dest_path}")
                    
                    uploaded_count = self._upload_hls_to_minio(
                        minio_client,
                        hls_output_dir,
                        minio_dest_path
                    )
                    
                    upload_time = time.time() - upload_start
                    logger.info(
                        f"[BENCHMARK] Upload completed in {upload_time:.2f}s "
                        f"({uploaded_count} files)"
                    )
                    
                    # Step 5: Generate/Update master playlist
                    master_start = time.time()
                    logger.info("[PROGRESS] Step 5/5: Generating adaptive bitrate master playlist")
                    
                    master_playlist_path = self._generate_master_playlist(
                        minio_client,
                        content_id,
                        video_type,
                        base_hls_path,
                        doctype,
                        episode_id
                    )
                    
                    master_time = time.time() - master_start
                    logger.info(f"[BENCHMARK] Master playlist generated in {master_time:.2f}s")
                    
                    # Calculate total processing time
                    total_time = time.time() - start_time
                    
                    logger.info(
                        f"[BENCHMARK] ===== ABR HLS PROCESSING SUMMARY =====\n"
                        f"  Content ID: {content_id}\n"
                        f"  Video Type: {video_type}\n"
                        f"  Resolution: {resolution}\n"
                        f"  Source File: {source_file}\n"
                        f"  Total Time: {total_time:.2f}s ({total_time/60:.2f} minutes)\n"
                        f"  - Download: {download_time:.2f}s ({(download_time/total_time)*100:.1f}%)\n"
                        f"  - Analysis: <1s\n"
                        f"  - Conversion: {conversion_time:.2f}s ({(conversion_time/total_time)*100:.1f}%)\n"
                        f"  - Upload: {upload_time:.2f}s ({(upload_time/total_time)*100:.1f}%)\n"
                        f"  - Master: {master_time:.2f}s ({(master_time/total_time)*100:.1f}%)\n"
                        f"  Input File Size: {file_size_mb:.2f} MB\n"
                        f"  Output Files: {uploaded_count}\n"
                        f"  HLS Path: {minio_dest_path}\n"
                        f"  Master Playlist: {master_playlist_path}\n"
                        f"  Bitrate: {video_info.get('bitrate', 0)} bps\n"
                        f"  Resolution: {video_info.get('width', 0)}x{video_info.get('height', 0)}\n"
                        f"==============================================="
                    )
                    
                    # Update MongoDB tracking record
                    self._update_conversion_record(
                        conversion_id,
                        status="completed",
                        completed_at=datetime.utcnow(),
                        duration_seconds=total_time,
                        segment_count=uploaded_count,
                        video_info=video_info,
                        master_playlist_path=master_playlist_path
                    )
                    
                    # Update _dmsfile (COMMENTED OUT - uncomment in future if needed)
                    # self._update_dmsfile_with_hls_info(
                    #     file_id=file_name_without_ext,
                    #     conversion_id=conversion_id,
                    #     hls_path=minio_dest_path,
                    #     master_playlist=master_playlist_path
                    # )
                    
                    logger.info(f"[PROGRESS] Processing completed successfully!")
                    return uploaded_count
                    
        except Exception as e:
            error_msg = str(e)
            logger.exception(f"[ERROR] Processing failed: {error_msg}")
            
            if conversion_id:
                self._update_conversion_record(
                    conversion_id,
                    status="failed",
                    error=error_msg
                )
            
            raise
        
        finally:
            # Clean up temporary directory
            cleanup_start = time.time()
            if os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir)
                    cleanup_time = time.time() - cleanup_start
                    logger.info(f"[BENCHMARK] Cleaned up temp directory in {cleanup_time:.2f}s")
                except Exception as cleanup_err:
                    logger.warning(f"Failed to clean up temp directory: {cleanup_err}")
    
    def _get_video_type(self, doctype: str, episode_id: Optional[str]) -> str:
        """Determine video type folder name"""
        if doctype == DocumentTypeHLS.FYP_1.value:
            return "fyp_1"
        elif doctype == DocumentTypeHLS.FYP_2.value:
            return "fyp_2"
        elif doctype == DocumentTypeHLS.VIDEO.value:
            return episode_id if episode_id else "video"
        else:
            raise ValueError(f"Unknown doctype: {doctype}")
    
    def _get_base_hls_path(self, content_id: str, doctype: str, episode_id: Optional[str]) -> str:
        """Get base HLS path in MinIO"""
        video_type = self._get_video_type(doctype, episode_id)
        return f"{content_id}/hls/{video_type}"
    
    @contextmanager
    def _redis_lock(self, lock_key: str, timeout: int = 300):
        """
        Redis distributed lock context manager.
        Ensures only one worker can update master playlist at a time.
        """
        lock = None
        try:
            with redis.RedisConn() as redis_client:
                # Acquire lock with timeout
                lock = redis_client.lock(lock_key, timeout=timeout, blocking_timeout=30)
                acquired = lock.acquire()
                
                if not acquired:
                    raise Exception(f"Could not acquire lock: {lock_key}")
                
                logger.info(f"[LOCK] Acquired: {lock_key}")
                yield lock
                
        finally:
            if lock:
                try:
                    lock.release()
                    logger.info(f"[LOCK] Released: {lock_key}")
                except Exception as e:
                    logger.warning(f"[LOCK] Failed to release: {e}")
    
    def _download_with_retry(self, minio_client, source_file: str, local_path: str) -> float:
        """Download file with retry logic. Returns file size in MB."""
        max_retries = 5
        
        for attempt in range(max_retries):
            try:
                logger.info(f"[PROGRESS] Download attempt {attempt + 1}/{max_retries}")
                
                minio_client.fget_object(
                    config.minio_bucket,
                    source_file,
                    local_path
                )
                
                file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
                logger.info(f"[PROGRESS] Download successful! File size: {file_size_mb:.2f} MB")
                return file_size_mb
                
            except S3Error as e:
                if e.code in ['AccessDenied', 'NoSuchKey'] and attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(
                        f"[PROGRESS] Download attempt {attempt + 1} failed: {e.code}. "
                        f"Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    raise
        
        raise ValueError(f"Download failed after {max_retries} attempts")
    
    def _get_video_info(self, video_path: str, resolution_hint: str) -> Dict:
        """
        Get video metadata using ffprobe.
        Falls back to predefined values if detection fails.
        """
        try:
            cmd = [
                "ffprobe",
                "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=width,height,bit_rate,r_frame_rate,codec_name:format=duration",
                "-of", "json",
                video_path
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            data = json.loads(result.stdout)
            
            stream = data.get('streams', [{}])[0]
            format_data = data.get('format', {})
            
            # Parse frame rate
            fps_str = stream.get('r_frame_rate', '30/1')
            if '/' in fps_str:
                num, den = fps_str.split('/')
                fps = int(num) / int(den) if int(den) != 0 else 30
            else:
                fps = float(fps_str)
            
            video_info = {
                "width": int(stream.get('width', 0)),
                "height": int(stream.get('height', 0)),
                "bitrate": int(stream.get('bit_rate', 0)),
                "duration": float(format_data.get('duration', 0)),
                "codec": stream.get('codec_name', 'h264'),
                "fps": round(fps, 2)
            }
            
            # If bitrate is 0 or missing, estimate from file size
            if video_info['bitrate'] == 0:
                file_size_bits = os.path.getsize(video_path) * 8
                if video_info['duration'] > 0:
                    video_info['bitrate'] = int(file_size_bits / video_info['duration'])
            
            logger.info(f"[VIDEO INFO] Detected: {video_info}")
            return video_info
            
        except Exception as e:
            logger.warning(f"[VIDEO INFO] Auto-detection failed: {e}, using fallback")
            
            # Fallback to predefined values
            fallback = self.bitrate_map.get(resolution_hint, self.bitrate_map["SD"])
            return {
                "width": fallback["width"],
                "height": fallback["height"],
                "bitrate": fallback["bitrate"],
                "duration": self._get_video_duration(video_path),
                "codec": "h264",
                "fps": 30.0
            }
    
    def _convert_to_hls(self, input_path: str, output_dir: str, base_name: str):
        """Convert video to HLS format"""
        playlist_path = os.path.join(output_dir, f"{base_name}.m3u8")
        segment_pattern = os.path.join(output_dir, f"{base_name}_%03d.ts")
        
        duration = self._get_video_duration(input_path)
        if duration > 0:
            logger.info(f"[BENCHMARK] Video duration: {duration:.2f}s ({duration/60:.2f} minutes)")
        
        ffmpeg_cmd = [
            "ffmpeg",
            "-i", input_path,
            "-c:v", "libx264",
            "-c:a", "aac",
            "-hls_time", "10",
            "-hls_playlist_type", "vod",
            "-hls_segment_type", "mpegts",
            "-hls_list_size", "0",
            "-hls_segment_filename", segment_pattern,
            "-progress", "pipe:1",
            "-f", "hls",
            playlist_path
        ]
        
        logger.debug(f"[BENCHMARK] Running ffmpeg: {' '.join(ffmpeg_cmd)}")
        
        process = subprocess.Popen(
            ffmpeg_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            bufsize=1
        )
        
        last_progress_log = time.time()
        last_progress_percent = 0
        
        for line in process.stdout:
            if "out_time_ms=" in line and duration > 0:
                try:
                    time_us = int(line.split("=")[1].strip())
                    current_time = time_us / 1_000_000
                    conversion_progress = min((current_time / duration) * 100, 100)
                    
                    if (conversion_progress - last_progress_percent >= 5) or \
                       (time.time() - last_progress_log > 5):
                        logger.info(
                            f"[PROGRESS] Conversion: {conversion_progress:.1f}% "
                            f"({current_time:.1f}s / {duration:.1f}s)"
                        )
                        last_progress_log = time.time()
                        last_progress_percent = conversion_progress
                except (ValueError, IndexError):
                    pass
        
        process.wait()
        
        if process.returncode != 0:
            stderr_output = process.stderr.read()
            logger.error(f"[ERROR] FFmpeg stderr: {stderr_output}")
            raise subprocess.CalledProcessError(process.returncode, ffmpeg_cmd, stderr=stderr_output)
        
        logger.info("[BENCHMARK] FFmpeg conversion completed")
    
    def _get_video_duration(self, video_path: str) -> float:
        """Get video duration in seconds"""
        try:
            cmd = [
                "ffprobe",
                "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                video_path
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return float(result.stdout.strip())
        except Exception as e:
            logger.warning(f"Could not get video duration: {e}")
            return 0.0
    
    def _upload_hls_to_minio(self, minio_client, hls_dir: str, dest_path: str) -> int:
        """Upload HLS files to MinIO"""
        uploaded_count = 0
        hls_files = [f for f in os.listdir(hls_dir) if os.path.isfile(os.path.join(hls_dir, f))]
        
        if not hls_files:
            logger.warning(f"No HLS files found in {hls_dir}")
            return 0
        
        total_files = len(hls_files)
        logger.info(f"[PROGRESS] Uploading {total_files} HLS files")
        
        for idx, file_name in enumerate(hls_files, 1):
            local_file_path = os.path.join(hls_dir, file_name)
            minio_object_name = f"{dest_path}/{file_name}"
            content_type = self._get_content_type(file_name)
            
            try:
                minio_client.fput_object(
                    config.minio_bucket,
                    minio_object_name,
                    local_file_path,
                    content_type=content_type
                )
                uploaded_count += 1
                logger.info(f"[PROGRESS] Uploaded [{idx}/{total_files}] {file_name}")
            except S3Error as s3e:
                logger.error(f"[ERROR] Failed to upload {file_name}: {str(s3e)}")
                raise
        
        return uploaded_count
    
    def _get_content_type(self, filename: str) -> str:
        """Determine content type"""
        extension = Path(filename).suffix.lower()
        content_types = {
            ".m3u8": "application/vnd.apple.mpegurl",
            ".ts": "video/mp2t",
            ".mp4": "video/mp4"
        }
        return content_types.get(extension, "application/octet-stream")
    
    def _generate_master_playlist(
        self,
        minio_client,
        content_id: str,
        video_type: str,
        base_hls_path: str,
        doctype: str,
        episode_id: Optional[str]
    ) -> str:
        """
        Generate or update master playlist with all available resolutions.
        Always creates master even if only one resolution exists.
        """
        logger.info(f"[MASTER] Generating master playlist for {video_type}")
        
        # Query MongoDB for all completed conversions of this video_type
        with mongodb.MongoConn() as mongo:
            collection = mongo.get_database()[self.collection_hls_conversion]
            
            query = {
                "content_id": content_id,
                "video_type": video_type,
                "status": "completed"
            }
            
            conversions = list(collection.find(query).sort("resolution", 1))
            logger.info(f"[MASTER] Found {len(conversions)} completed conversions")
        
        if not conversions:
            logger.warning(f"[MASTER] No completed conversions found for {video_type}")
            return None
        
        # Build master playlist
        master_lines = [
            "#EXTM3U",
            "#EXT-X-VERSION:3",
            ""
        ]
        
        # Sort by bitrate (highest to lowest)
        conversions_sorted = sorted(
            conversions,
            key=lambda x: x.get('video_info', {}).get('bitrate', 0),
            reverse=True
        )
        
        for conv in conversions_sorted:
            resolution = conv['resolution']
            video_info = conv.get('video_info', {})
            
            bitrate = video_info.get('bitrate', self.bitrate_map.get(resolution, {}).get('bitrate', 1000000))
            width = video_info.get('width', self.bitrate_map.get(resolution, {}).get('width', 854))
            height = video_info.get('height', self.bitrate_map.get(resolution, {}).get('height', 480))
            
            # Add variant to master (using RELATIVE path)
            master_lines.append(f"# {resolution} Variant")
            master_lines.append(
                f"#EXT-X-STREAM-INF:BANDWIDTH={bitrate},"
                f"RESOLUTION={width}x{height},"
                f"NAME=\"{resolution}\""
            )
            # Use relative path since master.m3u8 is in the same base folder
            master_lines.append(f"{resolution.lower()}/{resolution.lower()}.m3u8")
            master_lines.append("")
        
        master_content = "\n".join(master_lines)
        logger.info(f"[MASTER] Generated playlist with {len(conversions_sorted)} variants")
        
        # Upload master playlist
        master_path = f"{base_hls_path}/master.m3u8"
        
        try:
            from io import BytesIO
            minio_client.put_object(
                config.minio_bucket,
                master_path,
                BytesIO(master_content.encode('utf-8')),
                length=len(master_content.encode('utf-8')),
                content_type="application/vnd.apple.mpegurl"
            )
            logger.info(f"[MASTER] Uploaded master playlist to: {master_path}")
            return master_path
        except Exception as e:
            logger.error(f"[MASTER] Failed to upload: {e}")
            raise
    
    def _create_conversion_record(
        self,
        content_id: str,
        video_type: str,
        resolution: str,
        language: str,
        source_file: str,
        source_file_id: str,
        hls_path: str
    ) -> str:
        """Create MongoDB tracking record for HLS conversion"""
        try:
            with mongodb.MongoConn() as mongo:
                collection = mongo.get_database()[self.collection_hls_conversion]
                
                record = {
                    "content_id": content_id,
                    "video_type": video_type,
                    "resolution": resolution,
                    "language": language,
                    "source_file": source_file,
                    "source_file_id": source_file_id,
                    "status": "processing",
                    "started_at": datetime.utcnow(),
                    "hls_path": hls_path,
                    "in_master_playlist": False,
                    "error": None,
                    "retry_count": 0,
                    "created_by": "system",
                    "updated_at": datetime.utcnow()
                }
                
                result = collection.insert_one(record)
                conversion_id = str(result.inserted_id)
                logger.info(f"[DB] Created conversion record: {conversion_id}")
                return conversion_id
        except Exception as e:
            logger.error(f"[DB] Failed to create conversion record: {e}")
            raise
    
    def _update_conversion_record(
        self,
        conversion_id: str,
        status: Optional[str] = None,
        completed_at: Optional[datetime] = None,
        duration_seconds: Optional[float] = None,
        segment_count: Optional[int] = None,
        video_info: Optional[Dict] = None,
        master_playlist_path: Optional[str] = None,
        error: Optional[str] = None
    ):
        """Update MongoDB conversion record"""
        try:
            with mongodb.MongoConn() as mongo:
                collection = mongo.get_database()[self.collection_hls_conversion]
                
                from bson import ObjectId
                update_data = {"updated_at": datetime.utcnow()}
                
                if status:
                    update_data["status"] = status
                if completed_at:
                    update_data["completed_at"] = completed_at
                if duration_seconds is not None:
                    update_data["duration_seconds"] = duration_seconds
                if segment_count is not None:
                    update_data["segment_count"] = segment_count
                if video_info:
                    update_data["video_info"] = video_info
                if master_playlist_path:
                    update_data["master_playlist_path"] = master_playlist_path
                    update_data["in_master_playlist"] = True
                if error:
                    update_data["error"] = error
                
                collection.update_one(
                    {"_id": ObjectId(conversion_id)},
                    {"$set": update_data}
                )
                logger.info(f"[DB] Updated conversion record: {conversion_id}")
        except Exception as e:
            logger.warning(f"[DB] Failed to update conversion record: {e}")
    
    def _update_dmsfile_with_hls_info(
        self,
        file_id: str,
        conversion_id: str,
        hls_path: str,
        master_playlist: str
    ):
        """
        Update _dmsfile document with HLS conversion info.
        CURRENTLY COMMENTED OUT - Uncomment in future if needed.
        """
        try:
            with mongodb.MongoConn() as mongo:
                collection = mongo.get_database()[self.collection_file]
                
                hls_info = {
                    "status": "completed",
                    "conversion_id": conversion_id,
                    "hls_path": hls_path,
                    "master_playlist": master_playlist,
                    "converted_at": datetime.utcnow()
                }
                
                collection.update_one(
                    {"_id": file_id},
                    {"$set": {"hls_conversion": hls_info}}
                )
                logger.info(f"[DB] Updated _dmsfile: {file_id}")
        except Exception as e:
            logger.warning(f"[DB] Failed to update _dmsfile: {e}")