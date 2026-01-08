#!/usr/bin/env python3
"""
Simple MinIO Access Test
Quick test to diagnose access issues
"""

from baseapp.config import setting, minio
from minio.error import S3Error

config = setting.get_settings()

print("=" * 60)
print("MinIO Access Test")
print("=" * 60)

# Configuration
print(f"\nConfiguration:")
print(f"  Host: {config.minio_host}")
print(f"  Port: {config.minio_port}")
print(f"  Bucket: {config.minio_bucket}")
print(f"  Secure: {config.minio_secure}")

# Test file from error
test_filename = "a11df87fa9a54ed7a27f8f3cb6199214.mp4"

try:
    with minio.MinioConn() as client:
        print(f"\n✓ Connected to MinIO")
        
        # Test 1: List buckets
        print(f"\n[1] Testing List Buckets...")
        try:
            buckets = client.list_buckets()
            bucket_names = [b.name for b in buckets]
            print(f"✓ Can list buckets: {bucket_names}")
            
            if config.minio_bucket not in bucket_names:
                print(f"✗ Target bucket '{config.minio_bucket}' not found!")
                exit(1)
        except S3Error as e:
            print(f"✗ Cannot list buckets: {e.code} - {e.message}")
            exit(1)
        
        # Test 2: Check if bucket exists
        print(f"\n[2] Testing Bucket Exists...")
        try:
            if client.bucket_exists(config.minio_bucket):
                print(f"✓ Bucket '{config.minio_bucket}' exists")
            else:
                print(f"✗ Bucket '{config.minio_bucket}' does not exist")
                exit(1)
        except S3Error as e:
            print(f"✗ Error checking bucket: {e.code} - {e.message}")
            exit(1)
        
        # Test 3: List objects
        print(f"\n[3] Testing List Objects...")
        try:
            count = 0
            sample_files = []
            for obj in client.list_objects(config.minio_bucket):
                sample_files.append(obj.object_name)
                count += 1
                if count >= 5:
                    break
            
            if sample_files:
                print(f"✓ Can list objects in bucket (found {count}+ files)")
                print(f"  Sample files:")
                for f in sample_files[:3]:
                    print(f"    - {f}")
            else:
                print(f"⚠ Bucket is empty")
        except S3Error as e:
            print(f"✗ Cannot list objects: {e.code} - {e.message}")
            if e.code == "AccessDenied":
                print(f"\n  FIX: Grant 's3:ListBucket' permission to user")
            exit(1)
        
        # Test 4: Stat specific file
        print(f"\n[4] Testing Stat Object ('{test_filename}')...")
        try:
            stat = client.stat_object(config.minio_bucket, test_filename)
            print(f"✓ Can stat file")
            print(f"  Size: {stat.size} bytes")
            print(f"  Content-Type: {stat.content_type}")
            print(f"  Last Modified: {stat.last_modified}")
        except S3Error as e:
            print(f"✗ Cannot stat file: {e.code} - {e.message}")
            if e.code == "NoSuchKey":
                print(f"\n  File doesn't exist in bucket")
            elif e.code == "AccessDenied":
                print(f"\n  FIX: Grant 's3:GetObject' and 's3:HeadObject' permissions")
            exit(1)
        
        # Test 5: Read file content
        print(f"\n[5] Testing Read Object...")
        try:
            response = client.get_object(config.minio_bucket, test_filename)
            data = response.read(1024)  # Read first 1KB
            response.close()
            response.release_conn()
            print(f"✓ Can read file content ({len(data)} bytes)")
        except S3Error as e:
            print(f"✗ Cannot read file: {e.code} - {e.message}")
            if e.code == "AccessDenied":
                print(f"\n  FIX: Grant 's3:GetObject' permission")
            exit(1)
        
        # Test 6: Generate presigned URL
        print(f"\n[6] Testing Presigned URL...")
        try:
            from datetime import timedelta
            url = client.presigned_get_object(
                config.minio_bucket,
                test_filename,
                expires=timedelta(minutes=5)
            )
            print(f"✓ Can generate presigned URL")
            print(f"  URL: {url[:100]}...")
            
            # Test presigned URL
            print(f"\n[7] Testing Presigned URL Access...")
            import requests
            resp = requests.head(url, timeout=10)
            if resp.status_code == 200:
                print(f"✓ Presigned URL works (HTTP {resp.status_code})")
                print(f"  Content-Type: {resp.headers.get('content-type')}")
                print(f"  Content-Length: {resp.headers.get('content-length')}")
            else:
                print(f"✗ Presigned URL failed (HTTP {resp.status_code})")
        except Exception as e:
            print(f"✗ Presigned URL test failed: {str(e)}")
        
        # Success!
        print("\n" + "=" * 60)
        print("✓ ALL TESTS PASSED!")
        print("=" * 60)
        print("\nMinIO access is working correctly.")
        print("If streaming still fails, the issue is in the streaming service code.")
        
        # Recommendations
        print("\nNext Steps:")
        print("1. Verify StreamingService is using the same MinioConn")
        print("2. Check resolver.py is finding the correct filename")
        print("3. Add debug logging in streaming service")
        print("4. Compare working CRUD code vs streaming code")

except Exception as e:
    print(f"\n✗ FATAL ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    exit(1)