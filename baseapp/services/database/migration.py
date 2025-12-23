"""
MongoDB Migration System - Alembic-like approach
File: baseapp/services/database/migration.py

Pure Python migrations, no JSON dependency
"""
import os
import importlib.util
from datetime import datetime
from typing import List, Optional
from pathlib import Path

from baseapp.config import mongodb, setting
from baseapp.utils.logger import Logger

config = setting.get_settings()
logger = Logger("baseapp.services.database.migration")


class MigrationEnv:
    """
    Migration environment - Similar to Alembic's env.py
    Provides context for running migrations
    """
    
    def __init__(self, mongo_conn):
        self._db = mongo_conn.get_database()
        self.client = mongo_conn.get_connection()
        self.collections = self._db.list_collection_names()
    
    @property
    def db(self):
        """
        Return a wrapper that supports both dot and bracket notation
        for collection access
        """
        return CollectionAccessor(self._db)
    
    def create_collection(self, name: str):
        """Create a collection if it doesn't exist"""
        if name not in self.collections:
            self._db.create_collection(name)
            self.collections.append(name)
            logger.info(f"Created collection: {name}")
    
    def drop_collection(self, name: str):
        """Drop a collection if it exists"""
        if name in self.collections:
            self._db[name].drop()
            self.collections.remove(name)
            logger.info(f"Dropped collection: {name}")


class CollectionAccessor:
    """
    Wrapper for database that supports both dot and bracket notation
    This fixes the issue with collections starting with underscore
    """
    
    def __init__(self, db):
        self._db = db
    
    def __getattr__(self, name):
        """Support dot notation: env.db.users"""
        return self._db[name]
    
    def __getitem__(self, name):
        """Support bracket notation: env.db['_users']"""
        return self._db[name]


class Revision:
    """
    Represents a single migration revision
    Similar to Alembic's revision
    """
    
    def __init__(self, revision: str, down_revision: Optional[str], 
                 branch_labels: Optional[List[str]] = None,
                 depends_on: Optional[List[str]] = None):
        self.revision = revision
        self.down_revision = down_revision
        self.branch_labels = branch_labels or []
        self.depends_on = depends_on or []
        self.created_at = datetime.utcnow()
    
    def upgrade(self, env: MigrationEnv):
        """Override this in migration files"""
        raise NotImplementedError("upgrade() must be implemented")
    
    def downgrade(self, env: MigrationEnv):
        """Override this in migration files"""
        raise NotImplementedError("downgrade() must be implemented")


class MigrationManager:
    """
    Migration manager - Similar to Alembic command interface
    """
    
    MIGRATION_COLLECTION = "_db_version"
    MIGRATION_DIR = "migrations/versions"
    
    def __init__(self, migration_dir: str = None):
        self.migration_dir = migration_dir or self.MIGRATION_DIR
        self._ensure_migration_structure()
    
    def _ensure_migration_structure(self):
        """Create migration directory structure"""
        # Create versions directory
        Path(self.migration_dir).mkdir(parents=True, exist_ok=True)
        
        # Create __init__.py files
        for path in [Path("migrations"), Path(self.migration_dir)]:
            init_file = path / "__init__.py"
            if not init_file.exists():
                init_file.touch()
        
        # Create env.py if not exists
        env_file = Path("migrations") / "env.py"
        if not env_file.exists():
            self._create_env_file(env_file)
    
    def _create_env_file(self, filepath: Path):
        """Create env.py file - similar to Alembic"""
        content = '''"""
Migration Environment Configuration
Similar to Alembic's env.py
"""
from baseapp.config import mongodb
from baseapp.services.database.migration import MigrationEnv

def run_migrations_online():
    """Run migrations in 'online' mode."""
    with mongodb.MongoConn() as mongo_conn:
        env = MigrationEnv(mongo_conn)
        yield env

def run_migrations_offline():
    """Run migrations in 'offline' mode (not implemented for MongoDB)."""
    raise NotImplementedError("Offline mode not supported for MongoDB")
'''
        filepath.write_text(content)
        logger.info(f"Created env.py: {filepath}")
    
    def _ensure_migration_collection(self, mongo_conn):
        """Ensure migration tracking collection exists"""
        db = mongo_conn.get_database()
        collections = db.list_collection_names()
        
        if self.MIGRATION_COLLECTION not in collections:
            logger.info(f"Creating migration tracking collection: {self.MIGRATION_COLLECTION}")
            db.create_collection(self.MIGRATION_COLLECTION)
            
            # Create index on version_num and applied_at
            collection = getattr(mongo_conn, self.MIGRATION_COLLECTION)
            collection.create_index("version_num", unique=True)
            collection.create_index("applied_at")
            
            logger.info(f"✓ Migration collection created: {self.MIGRATION_COLLECTION}")

    def _get_current_revision(self, mongo_conn) -> Optional[str]:
        """Get current database revision"""
        try:
            # Ensure migration collection exists
            self._ensure_migration_collection(mongo_conn)

            # Use getattr to access collection from mongo_conn
            collection = getattr(mongo_conn, self.MIGRATION_COLLECTION)
            result = collection.find_one(
                {}, 
                sort=[("applied_at", -1)]
            )
            return result["version_num"] if result else None
        except Exception:
            return None
    
    def _get_all_revisions(self, mongo_conn) -> List[str]:
        """Get all applied revisions in order"""
        try:
            # Ensure migration collection exists
            self._ensure_migration_collection(mongo_conn)

            collection = getattr(mongo_conn, self.MIGRATION_COLLECTION)
            results = collection.find(
                {},
                {"version_num": 1}
            ).sort("applied_at", 1)
            return [r["version_num"] for r in results]
        except Exception:
            return []
    
    def _set_revision(self, mongo_conn, revision: str):
        """Mark revision as applied"""
        # Ensure migration collection exists
        self._ensure_migration_collection(mongo_conn)
        
        collection = getattr(mongo_conn, self.MIGRATION_COLLECTION)
        collection.insert_one({
            "version_num": revision,
            "applied_at": datetime.utcnow()
        })
        logger.info(f"Marked revision as applied: {revision}")
    
    def _remove_revision(self, mongo_conn, revision: str):
        """Remove revision mark"""
        collection = getattr(mongo_conn, self.MIGRATION_COLLECTION)
        collection.delete_one({
            "version_num": revision
        })
        logger.info(f"Removed revision mark: {revision}")
    
    def _load_migration(self, filename: str):
        """Load migration module and extract revision info"""
        filepath = os.path.join(self.migration_dir, filename)
        spec = importlib.util.spec_from_file_location(
            filename.replace('.py', ''), 
            filepath
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # Extract revision info
        revision = getattr(module, 'revision', None)
        down_revision = getattr(module, 'down_revision', None)
        upgrade_func = getattr(module, 'upgrade', None)
        downgrade_func = getattr(module, 'downgrade', None)
        
        if not revision or not upgrade_func or not downgrade_func:
            raise ValueError(f"Invalid migration file: {filename}")
        
        return {
            'module': module,
            'revision': revision,
            'down_revision': down_revision,
            'upgrade': upgrade_func,
            'downgrade': downgrade_func
        }
    
    def _get_migration_files(self) -> List[str]:
        """Get all migration files sorted by name"""
        if not os.path.exists(self.migration_dir):
            return []
        
        files = [
            f for f in os.listdir(self.migration_dir)
            if f.endswith('.py') and f != '__init__.py'
        ]
        return sorted(files)
    
    def _build_revision_map(self) -> dict:
        """Build map of all revisions and their relationships"""
        revision_map = {}
        
        for filename in self._get_migration_files():
            try:
                migration = self._load_migration(filename)
                revision_map[migration['revision']] = {
                    'filename': filename,
                    'down_revision': migration['down_revision'],
                    'upgrade': migration['upgrade'],
                    'downgrade': migration['downgrade']
                }
            except Exception as e:
                logger.error(f"Error loading migration {filename}: {e}")
        
        return revision_map
    
    def _get_upgrade_path(self, current: Optional[str], target: str, 
                          revision_map: dict) -> List[str]:
        """Get list of revisions to upgrade from current to target"""
        path = []
        rev = target
        
        while rev and rev != current:
            if rev not in revision_map:
                raise ValueError(f"Revision not found: {rev}")
            path.insert(0, rev)
            rev = revision_map[rev]['down_revision']
        
        return path
    
    def _get_downgrade_path(self, current: str, target: Optional[str],
                           revision_map: dict) -> List[str]:
        """Get list of revisions to downgrade from current to target"""
        path = []
        rev = current
        
        while rev and rev != target:
            if rev not in revision_map:
                raise ValueError(f"Revision not found: {rev}")
            path.append(rev)
            rev = revision_map[rev]['down_revision']
        
        return path
    
    def revision(self, message: str, autogenerate: bool = False) -> str:
        """
        Create a new revision file
        
        Args:
            message: Description of the migration
            autogenerate: If True, detect schema changes automatically
        
        Returns:
            Filename of created migration
        """
        # Generate revision ID (timestamp-based for sorting)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        revision_id = timestamp
        
        # Get current head
        with mongodb.MongoConn() as mongo_conn:
            current = self._get_current_revision(mongo_conn)
        
        # Create filename
        message_slug = message.lower().replace(' ', '_').replace('-', '_')
        filename = f"{revision_id}_{message_slug}.py"
        filepath = os.path.join(self.migration_dir, filename)
        
        # Generate migration code
        if autogenerate:
            logger.info("Running autogenerate...")
            from baseapp.services.database.autogenerate import autogenerate_migration
            
            migration_code, has_changes = autogenerate_migration(message)
            
            if not has_changes:
                print("⚠️  No schema changes detected")
                print("   If you want to create an empty migration, use --no-autogenerate")
                return None
            
            # Replace placeholders
            migration_code = migration_code.replace('{revision}', revision_id)
            migration_code = migration_code.replace('{down_revision}', f"'{current}'" if current else 'None')
            
            with open(filepath, 'w') as f:
                f.write(migration_code)
            
            logger.info(f"Created autogenerated revision: {filename}")
            print(f"✓ Created autogenerated revision: {filename}")
            print(f"  Revision ID: {revision_id}")
            print(f"  Edit: {filepath}")
            
            return filename
        
        else:
            # Generate empty template
            template = self._generate_empty_template(revision_id, current, message)
            
            with open(filepath, 'w') as f:
                f.write(template)
            
            logger.info(f"Created revision: {filename}")
            print(f"✓ Created new revision: {filename}")
            print(f"  Revision ID: {revision_id}")
            print(f"  Edit: {filepath}")
            
            return filename
    
    def _generate_empty_template(self, revision_id: str, down_revision: Optional[str], message: str) -> str:
        """Generate empty migration template"""
        return f'''"""
{message}

Revision ID: {revision_id}
Revises: {down_revision or 'None'}
Create Date: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
"""

# revision identifiers, used by migration system
revision = '{revision_id}'
down_revision = {f"'{down_revision}'" if down_revision else 'None'}
branch_labels = None
depends_on = None


def upgrade(env):
    """
    Upgrade database schema
    
    Args:
        env: MigrationEnv object with access to:
            - env.db: Database object
            - env.client: MongoClient object
            - env.create_collection(name)
            - env.drop_collection(name)
    
    Example:
        # Create collection
        env.create_collection('users')
        
        # Create indexes (use bracket notation for collections with underscore)
        env.db['users'].create_index([("email", 1)], unique=True)
        env.db['users'].create_index([("created_at", -1)])
        
        # Or dot notation for collections without underscore
        env.db.users.create_index([("email", 1)], unique=True)
        
        # Insert initial data
        env.db['users'].insert_many([
            {{"email": "admin@example.com", "role": "admin"}},
        ])
    """
    pass


def downgrade(env):
    """
    Downgrade database schema
    
    Example:
        # Drop indexes
        env.db['users'].drop_index("email_1")
        
        # Delete data
        env.db['users'].delete_many({{"role": "admin"}})
        
        # Drop collection
        env.drop_collection('users')
    """
    pass
'''
    
    def upgrade(self, revision: str = "head"):
        """
        Upgrade to a specific revision
        
        Args:
            revision: Target revision ID or 'head' for latest
        """
        with mongodb.MongoConn() as mongo_conn:
            # Ensure migration collection exists first
            self._ensure_migration_collection(mongo_conn)
            
            env = MigrationEnv(mongo_conn)
            current = self._get_current_revision(mongo_conn)
            revision_map = self._build_revision_map()
            
            if not revision_map:
                logger.info("No migrations found")
                return
            
            # Determine target
            if revision == "head":
                # Find head (revision with no other revision pointing to it)
                all_revisions = set(revision_map.keys())
                pointed_to = {r['down_revision'] for r in revision_map.values() 
                            if r['down_revision']}
                heads = all_revisions - pointed_to
                
                if len(heads) != 1:
                    raise ValueError(f"Expected 1 head, found {len(heads)}")
                
                target = list(heads)[0]
            else:
                target = revision
            
            if current == target:
                logger.info(f"Already at revision: {target}")
                return
            
            # Get upgrade path
            path = self._get_upgrade_path(current, target, revision_map)
            
            if not path:
                logger.info("No migrations to apply")
                return
            
            logger.info(f"Upgrading from {current or 'base'} to {target}")
            logger.info(f"Will apply {len(path)} migration(s)")
            
            # Apply migrations
            for rev in path:
                logger.info(f"Applying revision: {rev}")
                try:
                    migration = revision_map[rev]
                    migration['upgrade'](env)
                    self._set_revision(mongo_conn, rev)
                    logger.info(f"✓ Applied: {rev}")
                except Exception as e:
                    logger.error(f"✗ Failed to apply {rev}: {e}")
                    raise
            
            logger.info(f"✓ Upgrade complete: {len(path)} migration(s) applied")
    
    def downgrade(self, revision: str = "-1"):
        """
        Downgrade to a specific revision
        
        Args:
            revision: Target revision ID, '-1' for one step back, 
                     or 'base' for complete rollback
        """
        with mongodb.MongoConn() as mongo_conn:
            env = MigrationEnv(mongo_conn)
            current = self._get_current_revision(mongo_conn)
            
            if not current:
                logger.info("Already at base revision")
                return
            
            revision_map = self._build_revision_map()
            
            # Determine target
            if revision == "-1":
                target = revision_map[current]['down_revision']
            elif revision == "base":
                target = None
            else:
                target = revision
            
            # Get downgrade path
            path = self._get_downgrade_path(current, target, revision_map)
            
            if not path:
                logger.info("No migrations to rollback")
                return
            
            logger.info(f"Downgrading from {current} to {target or 'base'}")
            logger.info(f"Will rollback {len(path)} migration(s)")
            
            # Apply downgrades
            for rev in path:
                logger.info(f"Rolling back revision: {rev}")
                try:
                    migration = revision_map[rev]
                    migration['downgrade'](env)
                    self._remove_revision(mongo_conn, rev)
                    logger.info(f"✓ Rolled back: {rev}")
                except Exception as e:
                    logger.error(f"✗ Failed to rollback {rev}: {e}")
                    raise
            
            logger.info(f"✓ Downgrade complete: {len(path)} migration(s) rolled back")
    
    def current(self):
        """Show current revision"""
        with mongodb.MongoConn() as mongo_conn:
            current = self._get_current_revision(mongo_conn)
            
            if current:
                print(f"Current revision: {current}")
            else:
                print("Current revision: <base> (no migrations applied)")
    
    def history(self, verbose: bool = False):
        """Show migration history"""
        with mongodb.MongoConn() as mongo_conn:
            current = self._get_current_revision(mongo_conn)
            applied = set(self._get_all_revisions(mongo_conn))
            revision_map = self._build_revision_map()
            
            print("\n" + "="*70)
            print("  MIGRATION HISTORY")
            print("="*70 + "\n")
            
            if not revision_map:
                print("No migrations found")
                return
            
            # Build tree from base to head
            def build_tree(rev, indent=0):
                is_current = (rev == current)
                is_applied = (rev in applied)
                
                status = "●" if is_current else ("✓" if is_applied else "○")
                marker = " (current)" if is_current else ""
                
                print(f"  {'  ' * indent}{status} {rev}{marker}")
                
                if verbose and rev in revision_map:
                    filename = revision_map[rev]['filename']
                    print(f"  {'  ' * indent}   └─ {filename}")
                
                # Find children
                for child_rev, info in revision_map.items():
                    if info['down_revision'] == rev:
                        build_tree(child_rev, indent + 1)
            
            # Start from base (None)
            for rev, info in revision_map.items():
                if info['down_revision'] is None:
                    build_tree(rev)
            
            print("\n" + "="*70)
            print(f"  ● = current  |  ✓ = applied  |  ○ = pending")
            print("="*70 + "\n")
    
    def heads(self):
        """Show current head(s)"""
        revision_map = self._build_revision_map()
        
        if not revision_map:
            print("No migrations found")
            return
        
        # Find heads
        all_revisions = set(revision_map.keys())
        pointed_to = {r['down_revision'] for r in revision_map.values() 
                     if r['down_revision']}
        heads = all_revisions - pointed_to
        
        print(f"\nCurrent head(s): {', '.join(heads)}")
    
    def show(self, revision: str):
        """Show details of a specific revision"""
        revision_map = self._build_revision_map()
        
        if revision not in revision_map:
            print(f"Revision not found: {revision}")
            return
        
        info = revision_map[revision]
        
        print(f"\nRevision: {revision}")
        print(f"Down revision: {info['down_revision'] or '<base>'}")
        print(f"File: {info['filename']}")
        print(f"Path: {os.path.join(self.migration_dir, info['filename'])}")