"""
MongoDB Autogenerate - Similar to Alembic autogenerate
File: baseapp/services/database/autogenerate.py

Automatically detect schema changes and generate migration code
"""
from typing import List, Dict, Any, Set, Tuple
from baseapp.config import mongodb
from baseapp.model.mongodb_schema import ALL_COLLECTIONS, Index
from baseapp.utils.logger import Logger

logger = Logger("baseapp.services.database.autogenerate")


class SchemaComparator:
    """Compare current database schema with model definitions"""
    
    def __init__(self, mongo_conn):
        self.mongo_conn = mongo_conn
        self.db = mongo_conn.get_database()
    
    def get_existing_collections(self) -> Set[str]:
        """Get list of existing collections in database"""
        collections = set(self.db.list_collection_names())
        
        # Daftar collection internal yang harus diabaikan oleh autogenerate
        # _db_version: tabel history migrasi
        # system.indexes: tabel internal mongodb (biasanya hidden, tapi untuk jaga-jaga)
        ignored_collections = {'_db_version', 'system.indexes', 'system.profile'}
        
        return collections - ignored_collections
    
    def get_existing_indexes(self, collection_name: str) -> Dict[str, Any]:
        """Get existing indexes for a collection"""
        try:
            collection = getattr(self.mongo_conn, collection_name)
            indexes = collection.index_information()
            # Filter out default _id index
            return {k: v for k, v in indexes.items() if k != '_id_'}
        except Exception as e:
            logger.warning(f"Could not get indexes for {collection_name}: {e}")
            return {}
    
    def compare_schemas(self) -> Dict[str, Any]:
        """
        Compare model definitions with database state
        
        Returns:
            Dict with changes:
            {
                'new_collections': [...],
                'removed_collections': [...],
                'new_indexes': {collection: [indexes]},
                'removed_indexes': {collection: [indexes]}
            }
        """
        changes = {
            'new_collections': [],
            'removed_collections': [],
            'new_indexes': {},
            'removed_indexes': {}
        }
        
        # Get current state
        existing_collections = self.get_existing_collections()
        model_collections = {col.get_collection_name() for col in ALL_COLLECTIONS}
        
        # Find new and removed collections
        changes['new_collections'] = list(model_collections - existing_collections)
        changes['removed_collections'] = list(existing_collections - model_collections)
        
        # Compare indexes for existing collections
        for col_class in ALL_COLLECTIONS:
            col_name = col_class.get_collection_name()
            
            if col_name not in existing_collections:
                continue  # Will be in new_collections
            
            model_indexes = self._normalize_indexes(col_class.get_indexes())
            db_indexes = self.get_existing_indexes(col_name)
            
            # Find new indexes
            new_idx = self._find_new_indexes(model_indexes, db_indexes)
            if new_idx:
                changes['new_indexes'][col_name] = new_idx
            
            # Find removed indexes
            removed_idx = self._find_removed_indexes(model_indexes, db_indexes)
            if removed_idx:
                changes['removed_indexes'][col_name] = removed_idx
        
        return changes
    
    def _normalize_indexes(self, indexes: List[Index]) -> Dict[str, Dict]:
        """Normalize index definitions for comparison"""
        normalized = {}
        for idx in indexes:
            # Create a signature for the index
            fields_sig = tuple(idx.fields)
            key = idx.name or f"auto_{hash(fields_sig)}"
            normalized[key] = {
                'fields': fields_sig,
                'unique': idx.unique,
                'sparse': idx.sparse
            }
        return normalized
    
    def _find_new_indexes(self, model_indexes: Dict, db_indexes: Dict) -> List[Dict]:
        """Find indexes that are in model but not in database"""
        new_indexes = []
        
        for name, spec in model_indexes.items():
            # Check if index exists in DB (by field signature)
            found = False
            for db_name, db_spec in db_indexes.items():
                db_keys = db_spec.get('key', [])
                if self._fields_match(spec['fields'], db_keys):
                    found = True
                    break
            
            if not found:
                new_indexes.append(spec)
        
        return new_indexes
    
    def _find_removed_indexes(self, model_indexes: Dict, db_indexes: Dict) -> List[str]:
        """Find indexes that are in database but not in model"""
        removed = []
        
        for db_name, db_spec in db_indexes.items():
            db_keys = db_spec.get('key', [])
            
            # Check if this index is defined in model
            found = False
            for spec in model_indexes.values():
                if self._fields_match(spec['fields'], db_keys):
                    found = True
                    break
            
            if not found:
                removed.append(db_name)
        
        return removed
    
    def _fields_match(self, fields1: tuple, fields2: List[tuple]) -> bool:
        """Check if two field specifications match"""
        return tuple(fields1) == tuple(fields2)


class MigrationGenerator:
    """Generate migration code from schema changes"""
    
    def generate_migration_code(self, changes: Dict[str, Any], 
                                message: str = "autogenerated") -> str:
        """
        Generate migration file content from detected changes
        
        Args:
            changes: Dict with schema changes from SchemaComparator
            message: Migration message
        
        Returns:
            Complete migration file content as string
        """
        upgrade_code = []
        downgrade_code = []
        
        # Generate code for new collections
        for col_name in changes['new_collections']:
            col_class = self._get_collection_class(col_name)
            if col_class:
                upgrade_code.extend(self._generate_create_collection(col_class))
                downgrade_code.append(f"    env.drop_collection('{col_name}')")
        
        # Generate code for removed collections
        for col_name in changes['removed_collections']:
            upgrade_code.append(f"    env.drop_collection('{col_name}')")
            downgrade_code.append(f"    # Recreate {col_name} (not implemented)")
        
        # Generate code for new indexes
        for col_name, indexes in changes['new_indexes'].items():
            for idx in indexes:
                code = self._generate_create_index(col_name, idx)
                upgrade_code.append(code)
                # Downgrade: drop index
                index_name = self._get_index_name(idx['fields'])
                downgrade_code.append(
                    f"    env.db['{col_name}'].drop_index('{index_name}')"
                )
        
        # Generate code for removed indexes
        for col_name, index_names in changes['removed_indexes'].items():
            for idx_name in index_names:
                upgrade_code.append(
                    f"    env.db['{col_name}'].drop_index('{idx_name}')"
                )
                downgrade_code.append(
                    f"    # Recreate index {idx_name} (not implemented)"
                )
        
        # Build complete migration
        upgrade_section = "\n".join(upgrade_code) if upgrade_code else "    pass"
        downgrade_section = "\n".join(downgrade_code) if downgrade_code else "    pass"
        
        summary = self._generate_summary(changes)
        
        return f'''"""
{message}

{summary}
"""

revision = '{{revision}}'
down_revision = {{down_revision}}
branch_labels = None
depends_on = None


def upgrade(env):
    """Apply schema changes"""
{upgrade_section}


def downgrade(env):
    """Revert schema changes"""
{downgrade_section}
'''
    
    def _get_collection_class(self, col_name: str):
        """Get collection class by name"""
        from baseapp.model.mongodb_schema import ALL_COLLECTIONS
        for col in ALL_COLLECTIONS:
            if col.get_collection_name() == col_name:
                return col
        return None
    
    def _generate_create_collection(self, col_class) -> List[str]:
        """Generate code to create collection with indexes"""
        code = []
        col_name = col_class.get_collection_name()
        
        code.append(f"    # Create collection: {col_name}")
        code.append(f"    env.create_collection('{col_name}')")
        
        # Add indexes
        for idx in col_class.get_indexes():
            code.append(self._generate_create_index(col_name, idx.to_mongo_index()))
        
        # Add initial data
        initial_data = col_class.get_initial_data()
        if initial_data:
            code.append(f"    # Initial data for {col_name}")
            code.append(f"    env.db['{col_name}'].insert_many([")
            for item in initial_data:
                code.append(f"        {item},")
            code.append("    ])")
        
        code.append("")  # Empty line
        return code
    
    def _generate_create_index(self, col_name: str, index_spec: Dict) -> str:
        """Generate code to create an index"""
        fields = index_spec['fields']
        
        # Format fields
        if len(fields) == 1 and isinstance(fields[0], str):
            fields_str = f'"{fields[0]}"'
        else:
            fields_str = str(fields)
        
        # Build options
        options = []
        if index_spec.get('name'):
            options.append(f"name='{index_spec['name']}'")
        if index_spec.get('unique'):
            options.append("unique=True")
        if index_spec.get('sparse'):
            options.append("sparse=True")
        
        options_str = ", " + ", ".join(options) if options else ""
        
        return f"    env.db['{col_name}'].create_index({fields_str}{options_str})"
    
    def _get_index_name(self, fields: tuple) -> str:
        """Get default index name from fields"""
        # MongoDB default naming: field1_1_field2_-1
        parts = []
        for field, direction in fields:
            parts.append(f"{field}_{direction}")
        return "_".join(parts)
    
    def _generate_summary(self, changes: Dict[str, Any]) -> str:
        """Generate human-readable summary of changes"""
        lines = ["Autogenerated migration:"]
        
        if changes['new_collections']:
            lines.append(f"- New collections: {', '.join(changes['new_collections'])}")
        
        if changes['removed_collections']:
            lines.append(f"- Removed collections: {', '.join(changes['removed_collections'])}")
        
        if changes['new_indexes']:
            for col, indexes in changes['new_indexes'].items():
                lines.append(f"- New indexes on {col}: {len(indexes)} index(es)")
        
        if changes['removed_indexes']:
            for col, indexes in changes['removed_indexes'].items():
                lines.append(f"- Removed indexes from {col}: {len(indexes)} index(es)")
        
        if len(lines) == 1:
            lines.append("- No changes detected")
        
        return "\n".join(lines)


def autogenerate_migration(message: str = "autogenerated changes") -> Tuple[str, bool]:
    """
    Detect schema changes and generate migration code
    
    Args:
        message: Migration description
    
    Returns:
        Tuple of (migration_code, has_changes)
    """
    with mongodb.MongoConn() as conn:
        # Compare schemas
        comparator = SchemaComparator(conn)
        changes = comparator.compare_schemas()
        
        # Check if there are any changes
        has_changes = (
            bool(changes['new_collections']) or
            bool(changes['removed_collections']) or
            bool(changes['new_indexes']) or
            bool(changes['removed_indexes'])
        )
        
        if not has_changes:
            logger.info("No schema changes detected")
            return "", False
        
        # Generate migration code
        generator = MigrationGenerator()
        code = generator.generate_migration_code(changes, message)
        
        # Log summary
        logger.info(f"Detected changes:")
        logger.info(f"  New collections: {len(changes['new_collections'])}")
        logger.info(f"  Removed collections: {len(changes['removed_collections'])}")
        logger.info(f"  Collections with new indexes: {len(changes['new_indexes'])}")
        logger.info(f"  Collections with removed indexes: {len(changes['removed_indexes'])}")
        
        return code, True