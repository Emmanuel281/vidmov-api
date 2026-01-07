#!/usr/bin/env python3
"""
Test Streaming Service Directly
Bypass FastAPI to test streaming service code
"""

from baseapp.services.streaming.service import StreamingService
from baseapp.services.streaming.resolver import MediaResolver
from baseapp.config import setting

config = setting.get_settings()

print("=" * 60)
print("Streaming Service Test")
print("=" * 60)

# Test parameters from error log
content_id = "8a66f138350e401d919d9125511fc7ab"  # Replace with actual content ID
video_type = "fyp_1"
language = "en"
resolution = "sd"
test_filename = "a11df87fa9a54ed7a27f8f3cb6199214.mp4"

print(f"\nTest Parameters:")
print(f"  Content ID: {content_id}")
print(f"  Video Type: {video_type}")
print(f"  Language: {language}")
print(f"  Resolution: {resolution}")
print(f"  Test Filename: {test_filename}")

# Test 1: MediaResolver
print(f"\n[1] Testing MediaResolver...")
try:
    with MediaResolver() as resolver:
        print(f"✓ MediaResolver initialized")
        
        # Try to resolve filename
        filename = resolver.resolve_video_filename(
            content_id, video_type, language, resolution
        )
        
        if filename:
            print(f"✓ Resolved filename: {filename}")
        else:
            print(f"⚠ No filename found for given parameters")
            print(f"  This means no file in _dmsfile collection matches:")
            print(f"    refkey_id={content_id}")
            print(f"    doctype={resolver.DOCTYPE_FYP_1}")
            print(f"    metadata.Language={language.upper()}")
            print(f"    metadata.Resolution={resolution.upper()}")
            
            print(f"\n  Trying direct filename test...")
            filename = test_filename
            
except Exception as e:
    print(f"✗ MediaResolver failed: {str(e)}")
    import traceback
    traceback.print_exc()
    exit(1)

# Test 2: StreamingService
print(f"\n[2] Testing StreamingService...")
try:
    with StreamingService() as streaming:
        print(f"✓ StreamingService initialized")
        print(f"  Bucket: {streaming.bucket_name}")
        print(f"  MinIO client: {streaming.minio is not None}")
        
        # Test get_file_info
        print(f"\n[3] Testing get_file_info...")
        try:
            file_size, content_type = streaming.get_file_info(filename)
            print(f"✓ File info retrieved")
            print(f"  Size: {file_size} bytes")
            print(f"  Content-Type: {content_type}")
        except Exception as e:
            print(f"✗ get_file_info failed: {str(e)}")
            
            # Debug: Check MinIO client directly
            print(f"\n[DEBUG] Testing MinIO client directly...")
            try:
                stat = streaming.minio.stat_object(streaming.bucket_name, filename)
                print(f"✓ Direct MinIO stat works!")
                print(f"  Size: {stat.size}")
                print(f"  This means the issue is in get_file_info error handling")
            except Exception as minio_e:
                print(f"✗ Direct MinIO stat also fails: {str(minio_e)}")
                print(f"\n  This is the root cause!")
                
                # Check error type
                from minio.error import S3Error
                if isinstance(minio_e, S3Error):
                    print(f"\n  S3Error Details:")
                    print(f"    Code: {minio_e.code}")
                    print(f"    Message: {minio_e.message}")
                    print(f"    Resource: {minio_e.resource}")
                    
                    if minio_e.code == "AccessDenied":
                        print(f"\n  ⚠ ACCESS DENIED - This is a permissions issue!")
                        print(f"\n  Possible fixes:")
                        print(f"  1. Set bucket policy to allow GetObject:")
                        print(f"     mc anonymous set download myminio/{streaming.bucket_name}")
                        print(f"  2. Grant user permissions:")
                        print(f"     Add policy with s3:GetObject, s3:HeadObject actions")
                        print(f"  3. Check if streaming service uses same credentials as CRUD")
                    
                    elif minio_e.code == "NoSuchKey":
                        print(f"\n  ⚠ FILE NOT FOUND")
                        print(f"  Check if filename is correct: {filename}")
                        print(f"  Check in MinIO console if file exists")
            
            import traceback
            traceback.print_exc()
            exit(1)
        
        # Test 4: Stream file (without range)
        print(f"\n[4] Testing stream_file (full file)...")
        try:
            response = streaming.stream_file(filename, range_header=None)
            print(f"✓ stream_file works")
            print(f"  Response type: {type(response)}")
            print(f"  Status code: {getattr(response, 'status_code', 'N/A')}")
            print(f"  Headers: {getattr(response, 'headers', {})}")
        except Exception as e:
            print(f"✗ stream_file failed: {str(e)}")
            import traceback
            traceback.print_exc()
            exit(1)
        
        # Test 5: Stream file (with range)
        print(f"\n[5] Testing stream_file (range request)...")
        try:
            response = streaming.stream_file(filename, range_header="bytes=0-1023")
            print(f"✓ stream_file with range works")
            print(f"  Response type: {type(response)}")
            print(f"  Status code: {getattr(response, 'status_code', 'N/A')}")
        except Exception as e:
            print(f"✗ stream_file with range failed: {str(e)}")
            import traceback
            traceback.print_exc()
        
        print("\n" + "=" * 60)
        print("✓ STREAMING SERVICE TESTS PASSED!")
        print("=" * 60)
        print("\nThe streaming service is working correctly.")
        print("If API endpoint still fails, check:")
        print("1. FastAPI route configuration")
        print("2. Middleware intercepting requests")
        print("3. Request headers being passed correctly")

except Exception as e:
    print(f"\n✗ StreamingService initialization failed: {str(e)}")
    import traceback
    traceback.print_exc()
    exit(1)