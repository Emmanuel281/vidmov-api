import os
import subprocess
import tempfile
import shutil
import time
from pathlib import Path
from baseapp.services._redis_worker.base_worker import BaseWorker
from pymongo.errors import PyMongoError
from minio.error import S3Error
from baseapp.config import setting, minio, mongodb
from baseapp.utils.logger import Logger

config = setting.get_settings()
logger = Logger("baseapp.services._redis_worker.video_proces")


class VideoWorker(BaseWorker):
    def __init__(self, queue_manager, max_retries: int = 3):
        super().__init__(queue_manager, max_retries)
        self.collection_file = "_dmsfile"
        self.collection_organization = "_organization"
    
    def process_task(self, data: dict):
        """
        Process video conversion to HLS format with progress tracking.
        Expected data format:
        {
            "content_id": "4845cbb7e2384723abeb4ff09bcbf2a",
            "file": "4845cbb7e2384723abeb4ff09bcbf2a.mp4"
        }
        """
        start_time = time.time()
        
        logger.info(f"[BENCHMARK] Starting video processing task: {data}, Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")
        
        # Validate input
        if not data.get("content_id"):
            logger.error(f"Invalid task data: missing 'content_id' field. Data: {data}")
            raise ValueError("Missing required field: 'content_id'")
        
        if not data.get("file"):
            logger.error(f"Invalid task data: missing 'file' field. Data: {data}")
            raise ValueError("Missing required field: 'file'")
        
        content_id = data.get("content_id")
        source_file = data.get("file")
        file_name_without_ext = Path(source_file).stem
        
        # Create temporary directory for processing
        temp_dir = tempfile.mkdtemp(prefix="video_hls_")
        logger.info(f"[PROGRESS] Created temporary directory: {temp_dir}")
        
        try:
            with minio.MinioConn() as minio_client:
                # Step 1: Check if file exists in MinIO
                logger.info(f"[PROGRESS] Checking if file exists: {source_file} in bucket: {config.minio_bucket}")
                try:
                    stat = minio_client.stat_object(config.minio_bucket, source_file)
                    logger.info(f"[PROGRESS] File found - Size: {stat.size / (1024*1024):.2f} MB, Last Modified: {stat.last_modified}")
                except S3Error as e:
                    if e.code == 'NoSuchKey':
                        logger.error(f"[ERROR] File not found in MinIO: {source_file}")
                        logger.error(f"[ERROR] Bucket: {config.minio_bucket}, Object: {source_file}")
                        logger.info("[INFO] Listing files in bucket to help debug:")
                        try:
                            objects = minio_client.list_objects(config.minio_bucket, prefix="", recursive=True)
                            file_list = [obj.object_name for obj in list(objects)[:10]]  # Show first 10 files
                            logger.info(f"[INFO] Sample files in bucket: {file_list}")
                        except Exception as list_err:
                            logger.error(f"[ERROR] Could not list bucket contents: {list_err}")
                    raise
                
                # Step 2: Download video from MinIO
                download_start = time.time()
                logger.info(f"[PROGRESS] Step 1/4: Downloading video from MinIO: {source_file}")
                
                local_video_path = os.path.join(temp_dir, source_file)
                
                minio_client.fget_object(
                    config.minio_bucket,
                    source_file,
                    local_video_path
                )
                
                download_time = time.time() - download_start
                file_size_mb = os.path.getsize(local_video_path) / (1024 * 1024)
                logger.info(
                    f"[BENCHMARK] Download completed in {download_time:.2f}s "
                    f"({file_size_mb:.2f} MB at {file_size_mb/download_time:.2f} MB/s)"
                )
                
                # Step 2: Create HLS output directory
                hls_output_dir = os.path.join(temp_dir, "hls_output")
                os.makedirs(hls_output_dir, exist_ok=True)
                logger.info(f"[PROGRESS] Created HLS output directory: {hls_output_dir}")
                
                # Step 3: Convert video to HLS using ffmpeg
                conversion_start = time.time()
                logger.info("[PROGRESS] Step 2/4: Converting video to HLS format")
                
                self._convert_to_hls(
                    local_video_path,
                    hls_output_dir,
                    file_name_without_ext
                )
                
                conversion_time = time.time() - conversion_start
                logger.info(
                    f"[BENCHMARK] HLS conversion completed in {conversion_time:.2f}s "
                    f"({conversion_time/60:.2f} minutes)"
                )
                
                # Step 4: Upload HLS files back to MinIO
                upload_start = time.time()
                minio_dest_path = f"{content_id}/{file_name_without_ext}"
                logger.info(f"[PROGRESS] Step 3/4: Uploading HLS files to MinIO: {minio_dest_path}")
                
                # Rewrite m3u8 to use full paths before uploading
                self._rewrite_m3u8_with_full_paths(hls_output_dir, file_name_without_ext, minio_dest_path)
                
                uploaded_count = self._upload_hls_to_minio(
                    minio_client,
                    hls_output_dir,
                    minio_dest_path
                )
                
                upload_time = time.time() - upload_start
                logger.info(
                    f"[BENCHMARK] Upload completed in {upload_time:.2f}s "
                    f"({uploaded_count} files at {uploaded_count/upload_time:.2f} files/s)"
                )
                
                # Calculate total processing time
                total_time = time.time() - start_time
                
                logger.info(
                    f"[BENCHMARK] ===== PROCESSING SUMMARY =====\n"
                    f"  Content ID: {content_id}\n"
                    f"  Source File: {source_file}\n"
                    f"  Total Time: {total_time:.2f}s ({total_time/60:.2f} minutes)\n"
                    f"  - Download: {download_time:.2f}s ({(download_time/total_time)*100:.1f}%)\n"
                    f"  - Conversion: {conversion_time:.2f}s ({(conversion_time/total_time)*100:.1f}%)\n"
                    f"  - Upload: {upload_time:.2f}s ({(upload_time/total_time)*100:.1f}%)\n"
                    f"  Input File Size: {file_size_mb:.2f} MB\n"
                    f"  Output Files: {uploaded_count}\n"
                    f"  Output Path: {minio_dest_path}\n"
                    f"  HLS Folder: {file_name_without_ext}\n"
                    f"  Download Speed: {file_size_mb/download_time:.2f} MB/s\n"
                    f"  Conversion Speed: {file_size_mb/conversion_time:.2f} MB/s\n"
                    f"  Upload Speed: {uploaded_count/upload_time:.2f} files/s\n"
                    f"=========================================="
                )
                
                logger.info(f"[PROGRESS] Step 4/4: Processing completed successfully!")
                
                # Update database with HLS info (optional but recommended)
                self._update_content_with_hls_info(
                    content_id=content_id,
                    hls_folder=file_name_without_ext,
                    hls_path=minio_dest_path,
                    total_files=uploaded_count,
                    total_size_mb=file_size_mb
                )
                
                return uploaded_count
                
        except S3Error as s3e:
            error_msg = f"MinIO error: {str(s3e)}"
            logger.error(f"[ERROR] {error_msg}")
            raise ValueError(error_msg) from s3e
        
        except subprocess.CalledProcessError as ffmpeg_err:
            error_msg = f"Video conversion failed: {str(ffmpeg_err)}"
            logger.error(f"[ERROR] {error_msg}")
            raise ValueError(error_msg) from ffmpeg_err
        
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.exception(f"[ERROR] {error_msg}")
            raise
        
        finally:
            # Clean up temporary directory
            cleanup_start = time.time()
            if os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir)
                    cleanup_time = time.time() - cleanup_start
                    logger.info(f"[BENCHMARK] Cleaned up temp directory in {cleanup_time:.2f}s: {temp_dir}")
                except Exception as cleanup_err:
                    logger.warning(f"Failed to clean up temp directory: {cleanup_err}")
    
    def _convert_to_hls(
        self,
        input_path: str,
        output_dir: str,
        base_name: str
    ):
        """
        Convert video to HLS format using ffmpeg with progress tracking.
        
        Args:
            input_path: Path to input video file
            output_dir: Directory to store HLS output
            base_name: Base name for HLS files
        """
        playlist_path = os.path.join(output_dir, f"{base_name}.m3u8")
        segment_pattern = os.path.join(output_dir, f"{base_name}_%03d.ts")
        
        # Get video duration for progress calculation
        duration = self._get_video_duration(input_path)
        if duration > 0:
            logger.info(f"[BENCHMARK] Video duration: {duration:.2f}s ({duration/60:.2f} minutes)")
        
        # FFmpeg command for HLS conversion
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
            "-progress", "pipe:1",  # Enable progress output
            "-f", "hls",
            playlist_path
        ]
        
        logger.debug(f"[BENCHMARK] Running ffmpeg command: {' '.join(ffmpeg_cmd)}")
        
        # Run ffmpeg with real-time progress tracking
        process = subprocess.Popen(
            ffmpeg_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            bufsize=1
        )
        
        last_progress_log = time.time()
        last_progress_percent = 0
        
        # Parse ffmpeg progress output
        for line in process.stdout:
            if "out_time_ms=" in line and duration > 0:
                try:
                    # Extract current time in microseconds
                    time_us = int(line.split("=")[1].strip())
                    current_time = time_us / 1_000_000  # Convert to seconds
                    
                    conversion_progress = min((current_time / duration) * 100, 100)
                    
                    # Log progress every 5% or every 5 seconds
                    if (conversion_progress - last_progress_percent >= 5) or \
                       (time.time() - last_progress_log > 5):
                        logger.info(
                            f"[PROGRESS] Conversion progress: {conversion_progress:.1f}% "
                            f"({current_time:.1f}s / {duration:.1f}s)"
                        )
                        last_progress_log = time.time()
                        last_progress_percent = conversion_progress
                            
                except (ValueError, IndexError):
                    pass
        
        # Wait for process to complete
        process.wait()
        
        if process.returncode != 0:
            stderr_output = process.stderr.read()
            logger.error(f"[ERROR] FFmpeg stderr: {stderr_output}")
            raise subprocess.CalledProcessError(
                process.returncode,
                ffmpeg_cmd,
                stderr=stderr_output
            )
        
        logger.info("[BENCHMARK] FFmpeg conversion completed successfully")
    
    def _get_video_duration(self, video_path: str) -> float:
        """
        Get video duration in seconds using ffprobe.
        
        Args:
            video_path: Path to video file
        
        Returns:
            Duration in seconds
        """
        try:
            cmd = [
                "ffprobe",
                "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                video_path
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            duration = float(result.stdout.strip())
            return duration
        except Exception as e:
            logger.warning(f"Could not get video duration: {e}")
            return 0.0
    
    def _rewrite_m3u8_with_full_paths(
        self,
        hls_dir: str,
        base_name: str,
        minio_path: str
    ):
        """
        Rewrite m3u8 playlist to use full paths instead of relative paths.
        This ensures segments are accessible from MinIO.
        
        Args:
            hls_dir: Local HLS directory
            base_name: Base name for files
            minio_path: Destination path in MinIO
        """
        try:
            m3u8_file = os.path.join(hls_dir, f"{base_name}.m3u8")
            
            if not os.path.exists(m3u8_file):
                logger.warning(f"M3U8 file not found: {m3u8_file}")
                return
            
            # Read original m3u8
            with open(m3u8_file, 'r') as f:
                content = f.read()
            
            # Replace relative paths with full paths
            # Original: base_name_000.ts
            # New: {minio_path}/base_name_000.ts
            lines = content.split('\n')
            new_lines = []
            
            for line in lines:
                # Check if line is a segment file (ends with .ts)
                if line.strip().endswith('.ts'):
                    # Convert to full path
                    new_line = f"{minio_path}/{line.strip()}"
                    new_lines.append(new_line)
                    logger.debug(f"[M3U8] Rewrite: {line.strip()} -> {new_line}")
                else:
                    new_lines.append(line)
            
            # Write updated m3u8
            with open(m3u8_file, 'w') as f:
                f.write('\n'.join(new_lines))
            
            logger.info(f"[PROGRESS] Rewrote m3u8 with full paths for {len([l for l in new_lines if l.endswith('.ts')])} segments")
            
        except Exception as e:
            logger.warning(f"Failed to rewrite m3u8: {e}")
    
    def _upload_hls_to_minio(
        self,
        minio_client,
        hls_dir: str,
        dest_path: str
    ) -> int:
        """
        Upload all HLS files to MinIO with progress tracking.
        
        Args:
            minio_client: MinIO client instance
            hls_dir: Local directory containing HLS files
            dest_path: Destination path in MinIO (without bucket)
        
        Returns:
            Number of files uploaded
        """
        uploaded_count = 0
        
        # Get all files in HLS directory
        hls_files = [f for f in os.listdir(hls_dir) if os.path.isfile(os.path.join(hls_dir, f))]
        
        if not hls_files:
            logger.warning(f"No HLS files found in {hls_dir}")
            return 0
        
        total_files = len(hls_files)
        total_size_mb = sum(
            os.path.getsize(os.path.join(hls_dir, f)) for f in hls_files
        ) / (1024 * 1024)
        
        logger.info(
            f"[PROGRESS] Found {total_files} HLS files to upload "
            f"(Total size: {total_size_mb:.2f} MB)"
        )
        
        for idx, file_name in enumerate(hls_files, 1):
            upload_file_start = time.time()
            local_file_path = os.path.join(hls_dir, file_name)
            minio_object_name = f"{dest_path}/{file_name}"
            
            # Determine content type
            content_type = self._get_content_type(file_name)
            file_size_mb = os.path.getsize(local_file_path) / (1024 * 1024)
            
            try:
                minio_client.fput_object(
                    config.minio_bucket,
                    minio_object_name,
                    local_file_path,
                    content_type=content_type
                )
                
                upload_file_time = time.time() - upload_file_start
                upload_speed = file_size_mb / upload_file_time if upload_file_time > 0 else 0
                
                logger.info(
                    f"[PROGRESS] Uploaded [{idx}/{total_files}] {file_name} "
                    f"({file_size_mb:.2f} MB in {upload_file_time:.2f}s at {upload_speed:.2f} MB/s)"
                )
                
                uploaded_count += 1
            except S3Error as s3e:
                logger.error(f"[ERROR] Failed to upload {file_name}: {str(s3e)}")
                raise
        
        return uploaded_count
    
    def _get_content_type(self, filename: str) -> str:
        """
        Determine content type based on file extension.
        
        Args:
            filename: Name of the file
        
        Returns:
            Content type string
        """
        extension = Path(filename).suffix.lower()
        
        content_types = {
            ".m3u8": "application/vnd.apple.mpegurl",
            ".ts": "video/mp2t",
            ".mp4": "video/mp4"
        }
        
        return content_types.get(extension, "application/octet-stream")
    
    def _update_content_with_hls_info(
        self,
        content_id: str,
        hls_folder: str,
        hls_path: str,
        total_files: int,
        total_size_mb: float
    ):
        """
        Update content document with HLS conversion information.
        This helps track which videos have HLS available.
        
        Args:
            content_id: Content ID
            hls_folder: HLS folder name
            hls_path: Full path in MinIO
            total_files: Number of HLS files
            total_size_mb: Total size in MB
        """
        try:
            with mongodb.MongoConn() as mongo:
                # Update your content collection (adjust collection name as needed)
                collection = mongo.get_database()["_content"]  # Change to your collection name
                
                from datetime import datetime
                
                update_data = {
                    "hls_status": "ready",
                    "hls_folder": hls_folder,
                    "hls_path": hls_path,
                    "hls_files_count": total_files,
                    "hls_size_mb": total_size_mb,
                    "hls_converted_at": datetime.utcnow()
                }
                
                result = collection.update_one(
                    {"_id": content_id},
                    {"$set": update_data}
                )
                
                if result.modified_count > 0:
                    logger.info(f"[PROGRESS] Updated content {content_id} with HLS info")
                else:
                    logger.warning(f"[PROGRESS] Content {content_id} not found in database, HLS info not saved")
                    
        except Exception as e:
            # Don't fail the whole process if DB update fails
            logger.warning(f"Failed to update database with HLS info: {e}")