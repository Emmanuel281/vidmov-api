#!/usr/bin/env python3
"""
MinIO Access Diagnostic Script
Run this to diagnose MinIO access issues
"""

import sys
from baseapp.config import setting, minio
from minio.error import S3Error

config = setting.get_settings()

def print_header(text):
    print(f"\n{'='*60}")
    print(f"  {text}")
    print(f"{'='*60}")

def print_result(test_name, success, message=""):
    status = "✓" if success else "✗"
    color = "\033[92m" if success else "\033[91m"
    reset = "\033[0m"
    print(f"{color}{status}{reset} {test_name}")
    if message:
        print(f"  → {message}")

def main():
    print_header("MinIO Access Diagnostic Script")
    
    # Test 1: Configuration
    print("\n[1] Configuration Check")
    print(f"  Host: {config.minio_host}")
    print(f"  Port: {config.minio_port}")
    print(f"  Bucket: {config.minio_bucket}")
    print(f"  Secure: {config.minio_secure}")
    print(f"  Access Key: {config.minio_access_key[:4]}***")
    
    # Test 2: Connection
    print_header("Connection Tests")
    
    try:
        with minio.MinioConn() as client:
            print_result("Create MinIO client", True, "Connected successfully")
            
            # Test 3: List Buckets
            try:
                buckets = client.list_buckets()
                bucket_names = [b.name for b in buckets]
                print_result("List buckets", True, f"Found: {bucket_names}")
                
                # Check if target bucket exists
                if config.minio_bucket in bucket_names:
                    print_result(f"Bucket '{config.minio_bucket}' exists", True)
                else:
                    print_result(f"Bucket '{config.minio_bucket}' exists", False, 
                               f"Not found. Available: {bucket_names}")
                    return
                
            except S3Error as e:
                print_result("List buckets", False, f"Error: {e.code} - {e.message}")
                return
            
            # Test 4: List Objects
            print_header("Bucket Access Tests")
            try:
                objects = list(client.list_objects(config.minio_bucket, max_keys=5))
                if objects:
                    print_result("List objects in bucket", True, 
                               f"Found {len(objects)} objects")
                    print("\n  Sample objects:")
                    for obj in objects[:3]:
                        print(f"    - {obj.object_name} ({obj.size} bytes)")
                else:
                    print_result("List objects in bucket", True, "Bucket is empty")
                
            except S3Error as e:
                print_result("List objects in bucket", False, 
                           f"Error: {e.code} - {e.message}")
                if e.code == "AccessDenied":
                    print("\n  💡 Suggestion: User doesn't have ListBucket permission")
                    print("     Fix: Grant 's3:ListBucket' permission to user")
                return
            
            # Test 5: Stat Object (if we have objects)
            if objects:
                print_header("Object Access Tests")
                test_file = objects[0].object_name
                
                try:
                    stat = client.stat_object(config.minio_bucket, test_file)
                    print_result(f"Stat object '{test_file}'", True, 
                               f"Size: {stat.size} bytes, Type: {stat.content_type}")
                except S3Error as e:
                    print_result(f"Stat object '{test_file}'", False, 
                               f"Error: {e.code} - {e.message}")
                    if e.code == "AccessDenied":
                        print("\n  💡 Suggestion: User doesn't have GetObject permission")
                        print("     Fix: Grant 's3:GetObject' and 's3:HeadObject' permissions")
                    return
                
                # Test 6: Get Object
                try:
                    response = client.get_object(config.minio_bucket, test_file)
                    data = response.read(1024)  # Read first 1KB
                    response.close()
                    response.release_conn()
                    print_result(f"Read object '{test_file}'", True, 
                               f"Successfully read {len(data)} bytes")
                except S3Error as e:
                    print_result(f"Read object '{test_file}'", False, 
                               f"Error: {e.code} - {e.message}")
                    return
                
                # Test 7: Presigned URL
                from datetime import timedelta
                try:
                    presigned_url = client.presigned_get_object(
                        config.minio_bucket,
                        test_file,
                        expires=timedelta(minutes=5)
                    )
                    print_result("Generate presigned URL", True)
                    print(f"\n  Sample URL (expires in 5 min):")
                    print(f"  {presigned_url[:80]}...")
                    
                except Exception as e:
                    print_result("Generate presigned URL", False, str(e))
            
            # Test 8: Test specific file if provided
            print_header("Specific File Test")
            
            # The file from error log
            test_filename = "a11df87fa9a54ed7a27f8f3cb6199214.mp4"
            print(f"\nTesting file: {test_filename}")
            
            try:
                stat = client.stat_object(config.minio_bucket, test_filename)
                print_result(f"Stat '{test_filename}'", True, 
                           f"Size: {stat.size} bytes")
                
                # Try to read
                response = client.get_object(config.minio_bucket, test_filename)
                data = response.read(1024)
                response.close()
                response.release_conn()
                print_result(f"Read '{test_filename}'", True, 
                           f"Read {len(data)} bytes")
                
            except S3Error as e:
                print_result(f"Access '{test_filename}'", False, 
                           f"Error: {e.code} - {e.message}")
                
                if e.code == "NoSuchKey":
                    print("\n  💡 File doesn't exist in bucket")
                elif e.code == "AccessDenied":
                    print("\n  💡 Permission denied for this file")
                    print("     Possible causes:")
                    print("     1. Bucket policy doesn't allow GetObject")
                    print("     2. User policy doesn't allow GetObject")
                    print("     3. Object ACL is restrictive")
    
    except Exception as e:
        print_result("MinIO Connection", False, str(e))
        print("\n  Possible issues:")
        print("  1. Wrong host/port configuration")
        print("  2. Invalid credentials")
        print("  3. Network connectivity issues")
        print("  4. MinIO server not running")
        return
    
    # Final Summary
    print_header("Diagnostic Summary")
    print("""
All tests passed! ✓

MinIO access is working correctly. If streaming still fails, check:

1. Streaming service is using the same MinioConn class
2. Bucket name is correctly configured in streaming service
3. Check for any middleware/proxy blocking requests
4. Verify Cloudflare settings (if applicable)

Next steps:
- Run streaming endpoint test
- Check application logs for other errors
- Verify resolver is finding correct filenames
""")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nDiagnostic interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n✗ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)