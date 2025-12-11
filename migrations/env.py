"""
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
