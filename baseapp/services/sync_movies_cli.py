#!/usr/bin/env python3
"""
CLI tool untuk manage Content Search OpenSearch.

Usage:
    python content_search_cli.py setup              # Setup index
    python content_search_cli.py sync-all           # Sync all contents
    python content_search_cli.py sync-one <id>      # Sync single content
    python content_search_cli.py delete <id>        # Delete from OpenSearch
    python content_search_cli.py stats              # Show statistics
    python content_search_cli.py verify             # Verify data integrity
    python content_search_cli.py reindex            # Full reindex
"""

import sys
import argparse
from datetime import datetime
from typing import List, Dict
from tabulate import tabulate

from baseapp.config.mongodb import MongoConn
from baseapp.config.opensearch import OpenSearchConn
from baseapp.services.content_search.crud import ContentSearchCRUD

# Colors for terminal output
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

def print_success(msg):
    print(f"{Colors.GREEN}✓{Colors.RESET} {msg}")

def print_error(msg):
    print(f"{Colors.RED}✗{Colors.RESET} {msg}")

def print_warning(msg):
    print(f"{Colors.YELLOW}⚠{Colors.RESET} {msg}")

def print_info(msg):
    print(f"{Colors.BLUE}ℹ{Colors.RESET} {msg}")

def print_header(msg):
    print(f"\n{Colors.BOLD}{msg}{Colors.RESET}")
    print("=" * len(msg))

def setup_index():
    """Setup OpenSearch index dengan mapping"""
    print_header("Setting up OpenSearch Index")
    
    try:
        with ContentSearchCRUD() as crud:
            crud.setup_index()
            print_success("Index 'content_search' created successfully")
            return True
    except Exception as e:
        print_error(f"Failed to create index: {e}")
        return False

def sync_all_contents(batch_size=1000, dry_run=False):
    """Sync semua contents dari MongoDB ke OpenSearch"""
    print_header("Bulk Sync Contents")
    
    if dry_run:
        print_warning("DRY RUN MODE - No changes will be made")
    
    try:
        # Initialize connections
        MongoConn.initialize()
        OpenSearchConn.initialize()
        
        with ContentSearchCRUD() as crud:
            # Count total
            total = crud.mongo.get_database()[crud.mongodb_collection].count_documents({})
            published = crud.mongo.get_database()[crud.mongodb_collection].count_documents(
                {"status": "published"}
            )
            
            print_info(f"Total contents in MongoDB: {total}")
            print_info(f"Published contents: {published}")
            print_info(f"Batch size: {batch_size}")
            
            if dry_run:
                print_info("Would sync all contents")
                return True
            
            # Confirm
            response = input("\nProceed with sync? (y/N): ")
            if response.lower() != 'y':
                print_warning("Sync cancelled")
                return False
            
            # Perform sync
            print_info("\nStarting sync... This may take a while...")
            start_time = datetime.now()
            
            stats = crud.bulk_sync_contents(batch_size=batch_size)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            # Display results
            print_header("Sync Completed")
            
            table_data = [
                ["Success", stats['success']],
                ["Failed", stats['failed']],
                ["Duration", f"{duration:.2f} seconds"],
                ["Speed", f"{stats['success']/duration:.2f} docs/sec"]
            ]
            
            print(tabulate(table_data, headers=["Metric", "Value"], tablefmt="grid"))
            
            if stats['failed'] == 0:
                print_success("All contents synced successfully!")
                return True
            else:
                print_warning(f"{stats['failed']} contents failed to sync")
                return False
                
    except Exception as e:
        print_error(f"Sync failed: {e}")
        return False
    finally:
        MongoConn.close_connection()
        OpenSearchConn.close_connection()

def sync_single_content(content_id):
    """Sync single content"""
    print_header(f"Syncing Content: {content_id}")
    
    try:
        MongoConn.initialize()
        OpenSearchConn.initialize()
        
        with ContentSearchCRUD() as crud:
            # Check if exists in MongoDB
            content = crud.mongo.get_database()[crud.mongodb_collection].find_one(
                {"_id": content_id}
            )
            
            if not content:
                print_error(f"Content {content_id} not found in MongoDB")
                return False
            
            print_info(f"Title: {content.get('title', {}).get('id', 'N/A')}")
            print_info(f"Status: {content.get('status', 'N/A')}")
            
            # Sync
            success = crud.sync_single_content(content_id)
            
            if success:
                print_success(f"Content {content_id} synced successfully")
                return True
            else:
                print_error(f"Failed to sync content {content_id}")
                return False
                
    except Exception as e:
        print_error(f"Sync failed: {e}")
        return False
    finally:
        MongoConn.close_connection()
        OpenSearchConn.close_connection()

def delete_content(content_id):
    """Delete content dari OpenSearch"""
    print_header(f"Deleting Content: {content_id}")
    
    try:
        OpenSearchConn.initialize()
        
        with ContentSearchCRUD() as crud:
            # Confirm
            response = input(f"\nAre you sure you want to delete {content_id}? (y/N): ")
            if response.lower() != 'y':
                print_warning("Delete cancelled")
                return False
            
            success = crud.delete_content_from_opensearch(content_id)
            
            if success:
                print_success(f"Content {content_id} deleted successfully")
                return True
            else:
                print_error(f"Content {content_id} not found in OpenSearch")
                return False
                
    except Exception as e:
        print_error(f"Delete failed: {e}")
        return False
    finally:
        OpenSearchConn.close_connection()

def show_stats():
    """Show comprehensive statistics"""
    print_header("Content Search Statistics")
    
    try:
        MongoConn.initialize()
        OpenSearchConn.initialize()
        
        with ContentSearchCRUD() as crud:
            # MongoDB stats
            mongo_total = crud.mongo.get_database()[crud.mongodb_collection].count_documents({})
            
            status_counts = {}
            for status in ["published", "draft", "archived"]:
                count = crud.mongo.get_database()[crud.mongodb_collection].count_documents(
                    {"status": status}
                )
                status_counts[status] = count
            
            # OpenSearch stats
            client = crud.opensearch.get_client()
            
            if not client.indices.exists(index=crud.opensearch_index):
                print_error("OpenSearch index 'content_search' does not exist")
                print_info("Run 'python content_search_cli.py setup' first")
                return False
            
            # Document count
            count_response = crud.opensearch.search(
                body={"query": {"match_all": {}}, "size": 0}
            )
            os_total = count_response['hits']['total']['value']
            
            # Index stats
            index_stats = client.indices.stats(index=crud.opensearch_index)
            index_data = index_stats['indices'][crud.opensearch_index]['total']
            
            index_size_bytes = index_data['store']['size_in_bytes']
            index_size_mb = index_size_bytes / (1024 * 1024)
            doc_count = index_data['docs']['count']
            
            # Display MongoDB stats
            print_header("MongoDB Statistics")
            mongo_table = [
                ["Total Contents", mongo_total],
                ["Published", status_counts.get('published', 0)],
                ["Draft", status_counts.get('draft', 0)],
                ["Archived", status_counts.get('archived', 0)]
            ]
            print(tabulate(mongo_table, headers=["Metric", "Count"], tablefmt="grid"))
            
            # Display OpenSearch stats
            print_header("OpenSearch Statistics")
            os_table = [
                ["Indexed Documents", os_total],
                ["Index Size", f"{index_size_mb:.2f} MB"],
                ["Disk Usage", f"{index_size_bytes:,} bytes"]
            ]
            print(tabulate(os_table, headers=["Metric", "Value"], tablefmt="grid"))
            
            # Sync status
            print_header("Sync Status")
            expected = status_counts.get('published', 0)
            difference = expected - os_total
            
            if difference == 0:
                print_success("✓ Fully synced")
                sync_status = "✓ Synced"
                sync_color = Colors.GREEN
            else:
                if difference > 0:
                    print_warning(f"⚠ {difference} documents missing in OpenSearch")
                else:
                    print_warning(f"⚠ {abs(difference)} extra documents in OpenSearch")
                sync_status = "⚠ Out of sync"
                sync_color = Colors.YELLOW
            
            sync_table = [
                ["Expected (Published)", expected],
                ["Actual (OpenSearch)", os_total],
                ["Difference", abs(difference)],
                ["Status", f"{sync_color}{sync_status}{Colors.RESET}"]
            ]
            print(tabulate(sync_table, headers=["Metric", "Value"], tablefmt="grid"))
            
            return difference == 0
            
    except Exception as e:
        print_error(f"Failed to get statistics: {e}")
        return False
    finally:
        MongoConn.close_connection()
        OpenSearchConn.close_connection()

def verify_integrity():
    """Verify data integrity between MongoDB and OpenSearch"""
    print_header("Verifying Data Integrity")
    
    try:
        MongoConn.initialize()
        OpenSearchConn.initialize()
        
        with ContentSearchCRUD() as crud:
            # Get all published content IDs from MongoDB
            mongo_ids = set()
            cursor = crud.mongo.get_database()[crud.mongodb_collection].find(
                {"status": "published"},
                {"_id": 1}
            )
            for doc in cursor:
                mongo_ids.add(str(doc['_id']))
            
            print_info(f"Found {len(mongo_ids)} published contents in MongoDB")
            
            # Get all content IDs from OpenSearch
            os_ids = set()
            response = crud.opensearch.search(
                body={
                    "query": {"match_all": {}},
                    "size": 10000,
                    "_source": ["content_id"]
                }
            )
            
            for hit in response['hits']['hits']:
                os_ids.add(hit['_source']['content_id'])
            
            print_info(f"Found {len(os_ids)} contents in OpenSearch")
            
            # Find differences
            missing_in_os = mongo_ids - os_ids
            extra_in_os = os_ids - mongo_ids
            
            print_header("Verification Results")
            
            if not missing_in_os and not extra_in_os:
                print_success("✓ Perfect sync - no discrepancies found!")
                return True
            
            if missing_in_os:
                print_warning(f"\n{len(missing_in_os)} contents missing in OpenSearch:")
                for i, content_id in enumerate(list(missing_in_os)[:10], 1):
                    print(f"  {i}. {content_id}")
                if len(missing_in_os) > 10:
                    print(f"  ... and {len(missing_in_os) - 10} more")
            
            if extra_in_os:
                print_warning(f"\n{len(extra_in_os)} extra contents in OpenSearch:")
                for i, content_id in enumerate(list(extra_in_os)[:10], 1):
                    print(f"  {i}. {content_id}")
                if len(extra_in_os) > 10:
                    print(f"  ... and {len(extra_in_os) - 10} more")
            
            # Suggestions
            print_header("Recommendations")
            if missing_in_os:
                print_info("Run 'python content_search_cli.py sync-all' to sync missing contents")
            if extra_in_os:
                print_info("Extra contents may be deleted/unpublished in MongoDB")
                print_info("Consider running full reindex to clean up")
            
            return False
            
    except Exception as e:
        print_error(f"Verification failed: {e}")
        return False
    finally:
        MongoConn.close_connection()
        OpenSearchConn.close_connection()

def reindex(batch_size=1000):
    """Full reindex - delete index and recreate"""
    print_header("Full Reindex")
    print_warning("This will DELETE the existing index and recreate it!")
    print_warning("All data will be re-synced from MongoDB")
    
    response = input("\nAre you absolutely sure? Type 'yes' to confirm: ")
    if response.lower() != 'yes':
        print_warning("Reindex cancelled")
        return False
    
    try:
        OpenSearchConn.initialize()
        
        with ContentSearchCRUD() as crud:
            client = crud.opensearch.get_client()
            
            # Delete existing index
            if client.indices.exists(index=crud.opensearch_index):
                print_info("Deleting existing index...")
                client.indices.delete(index=crud.opensearch_index)
                print_success("Existing index deleted")
            
            # Create new index
            print_info("Creating new index...")
            crud.setup_index()
            print_success("New index created")
        
        OpenSearchConn.close_connection()
        
        # Sync all data
        print_info("\nStarting data sync...")
        return sync_all_contents(batch_size=batch_size)
        
    except Exception as e:
        print_error(f"Reindex failed: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(
        description="CLI tool untuk manage Content Search OpenSearch",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python content_search_cli.py setup
  python content_search_cli.py sync-all --batch-size 500
  python content_search_cli.py sync-one fc3d27cf64274413ab74b3f72122707b
  python content_search_cli.py stats
  python content_search_cli.py verify
  python content_search_cli.py reindex
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Setup command
    subparsers.add_parser('setup', help='Setup OpenSearch index')
    
    # Sync all command
    sync_all_parser = subparsers.add_parser('sync-all', help='Sync all contents')
    sync_all_parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Batch size for bulk operations (default: 1000)'
    )
    sync_all_parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Dry run mode (no changes)'
    )
    
    # Sync one command
    sync_one_parser = subparsers.add_parser('sync-one', help='Sync single content')
    sync_one_parser.add_argument('content_id', help='Content ID to sync')
    
    # Delete command
    delete_parser = subparsers.add_parser('delete', help='Delete content from OpenSearch')
    delete_parser.add_argument('content_id', help='Content ID to delete')
    
    # Stats command
    subparsers.add_parser('stats', help='Show sync statistics')
    
    # Verify command
    subparsers.add_parser('verify', help='Verify data integrity')
    
    # Reindex command
    reindex_parser = subparsers.add_parser('reindex', help='Full reindex (delete and recreate)')
    reindex_parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Batch size for bulk operations (default: 1000)'
    )
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Execute command
    success = False
    
    try:
        if args.command == 'setup':
            success = setup_index()
        elif args.command == 'sync-all':
            success = sync_all_contents(
                batch_size=args.batch_size,
                dry_run=args.dry_run
            )
        elif args.command == 'sync-one':
            success = sync_single_content(args.content_id)
        elif args.command == 'delete':
            success = delete_content(args.content_id)
        elif args.command == 'stats':
            success = show_stats()
        elif args.command == 'verify':
            success = verify_integrity()
        elif args.command == 'reindex':
            success = reindex(batch_size=args.batch_size)
        
        return 0 if success else 1
        
    except KeyboardInterrupt:
        print_warning("\n\nOperation cancelled by user")
        return 1
    except Exception as e:
        print_error(f"\nUnexpected error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())