"""
MongoDB Migration CLI - Alembic-like interface
File: manage.py (place in project root)

Usage (exactly like Alembic):
    python manage.py revision -m "create users table"
    python manage.py upgrade head
    python manage.py downgrade -1
    python manage.py current
    python manage.py history
"""
import sys
import argparse
from baseapp.config import mongodb
from baseapp.services.database.migration import MigrationManager


def main():
    parser = argparse.ArgumentParser(
        description='MongoDB Migration Manager (Alembic-like)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create new migration
  python manage.py revision -m "create users collection"
  
  # Upgrade to latest
  python manage.py upgrade head
  
  # Upgrade to specific revision
  python manage.py upgrade 20241210120000
  
  # Downgrade one step
  python manage.py downgrade -1
  
  # Downgrade to specific revision
  python manage.py downgrade 20241210110000
  
  # Downgrade all (to base)
  python manage.py downgrade base
  
  # Show current revision
  python manage.py current
  
  # Show migration history
  python manage.py history
  python manage.py history -v  # verbose
  
  # Show head(s)
  python manage.py heads
  
  # Show specific revision
  python manage.py show 20241210120000
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # revision command
    revision_parser = subparsers.add_parser(
        'revision',
        help='Create a new revision file'
    )
    revision_parser.add_argument(
        '-m', '--message',
        required=True,
        help='Revision message'
    )
    revision_parser.add_argument(
        '--autogenerate',
        action='store_true',
        help='Automatically detect schema changes from models'
    )
    
    # upgrade command
    upgrade_parser = subparsers.add_parser(
        'upgrade',
        help='Upgrade to a later version'
    )
    upgrade_parser.add_argument(
        'revision',
        nargs='?',
        default='head',
        help='Revision target (default: head)'
    )
    
    # downgrade command
    downgrade_parser = subparsers.add_parser(
        'downgrade',
        help='Revert to a previous version'
    )
    downgrade_parser.add_argument(
        'revision',
        nargs='?',
        default='-1',
        help='Revision target (default: -1 for one step back, use "base" for complete rollback)'
    )
    
    # current command
    subparsers.add_parser(
        'current',
        help='Display the current revision'
    )
    
    # history command
    history_parser = subparsers.add_parser(
        'history',
        help='List migration history'
    )
    history_parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Show detailed information'
    )
    
    # heads command
    subparsers.add_parser(
        'heads',
        help='Show current head(s)'
    )
    
    # show command
    show_parser = subparsers.add_parser(
        'show',
        help='Show the revision denoted by the given symbol'
    )
    show_parser.add_argument(
        'revision',
        help='Revision identifier'
    )
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        manager = MigrationManager()
        
        if args.command == 'revision':
            print(f"🔨 Creating new revision: {args.message}")
            if args.autogenerate:
                print("   Running autogenerate...")
                # Initialize MongoDB for autogenerate
                mongodb.MongoConn.initialize()
                try:
                    filename = manager.revision(args.message, autogenerate=True)
                    if filename is None:
                        print("\n💡 Tip: Define your schema in baseapp/models/mongodb_schema.py")
                        print("   then run with --autogenerate again")
                finally:
                    mongodb.MongoConn.close_connection()
            else:
                filename = manager.revision(args.message, autogenerate=False)
                print("\n💡 Tip: Use --autogenerate to detect schema changes automatically")
        
        elif args.command == 'upgrade':
            # Initialize MongoDB connection for operations
            mongodb.MongoConn.initialize()
            try:
                print(f"⬆️  Upgrading to: {args.revision}")
                manager.upgrade(args.revision)
            finally:
                mongodb.MongoConn.close_connection()
        
        elif args.command == 'downgrade':
            mongodb.MongoConn.initialize()
            try:
                print(f"⬇️  Downgrading to: {args.revision}")
                manager.downgrade(args.revision)
            finally:
                mongodb.MongoConn.close_connection()
        
        elif args.command == 'current':
            mongodb.MongoConn.initialize()
            try:
                manager.current()
            finally:
                mongodb.MongoConn.close_connection()
        
        elif args.command == 'history':
            mongodb.MongoConn.initialize()
            try:
                manager.history(verbose=args.verbose)
            finally:
                mongodb.MongoConn.close_connection()
        
        elif args.command == 'heads':
            manager.heads()
        
        elif args.command == 'show':
            manager.show(args.revision)
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()