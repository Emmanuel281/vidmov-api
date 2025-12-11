# 🔄 Transition Guide: From JSON-based to Alembic-like Migration

## Overview

Guide ini menjelaskan cara transition dari sistem lama (JSON-based) ke sistem baru (Alembic-like) dengan aman.

---

## 📊 Comparison

| Aspect | Old System (JSON) | New System (Alembic-like) |
|--------|------------------|---------------------------|
| **Config Format** | JSON file | Pure Python |
| **Migration Flow** | Load JSON → Create all | Incremental revisions |
| **Version Control** | Single snapshot | Linear history |
| **Rollback** | Drop all collections | Step-by-step |
| **Team Workflow** | Single person | Multi-developer friendly |
| **Industry Standard** | Custom | Alembic-inspired |

---

## 🎯 Migration Strategy

### Option 1: Fresh Start (Recommended for New Projects)

**When to use:**
- New project
- Development environment
- No production data yet

**Steps:**

```bash
# 1. Delete old JSON-based setup (if exists)
# DO NOT run this if you have production data!
# This is only for fresh start

# 2. Initialize migration system
python manage.py revision -m "initial schema"

# 3. Edit migration file with your schema
nano migrations/versions/TIMESTAMP_initial_schema.py

# 4. Apply migration
python manage.py upgrade head

# 5. Verify
python manage.py current
python manage.py history
```

---

### Option 2: Gradual Transition (Recommended for Production)

**When to use:**
- Existing production database
- Have data that must be preserved
- Need zero downtime

**Phase 1: Preparation (Week 1)**

```bash
# 1. Keep old system running (DON'T TOUCH PRODUCTION!)

# 2. In parallel, setup migration system in dev/staging
python manage.py revision -m "baseline - match current production"

# 3. Create migration that matches EXACTLY your current production schema
# Edit the migration file to match mongodb.json structure

# 4. Test in staging
python manage.py upgrade head

# 5. Verify staging matches production schema
```

**Phase 2: Baseline (Week 2)**

```python
# migrations/versions/20241210000000_baseline.py
"""
Baseline - Current Production Schema

This migration represents the current state of production.
It does NOT create collections (they already exist).
It only marks them as managed by migration system.

Revision ID: 20241210000000
Revises: None
Create Date: 2024-12-10 00:00:00
"""

revision = '20241210000000'
down_revision = None
branch_labels = None
depends_on = None


def upgrade(env):
    """
    Baseline: Mark existing collections as managed
    
    This migration does NOT create anything.
    It assumes collections already exist in production.
    """
    # Just verify collections exist
    required = [
        '_user', '_role', '_organization', '_audittrail',
        '_enum', '_feature', '_menu', 'content'
    ]
    
    missing = [c for c in required if c not in env.collections]
    
    if missing:
        raise Exception(f"Missing collections: {missing}. "
                       f"This baseline assumes they already exist!")
    
    print("✓ All required collections exist")
    print("✓ Baseline established - future migrations will be tracked")


def downgrade(env):
    """
    Cannot downgrade baseline
    """
    raise Exception("Cannot downgrade baseline - collections already existed")
```

```bash
# Apply baseline in production
python manage.py upgrade head

# This just marks current state, doesn't change anything
```

**Phase 3: New Changes via Migration (Week 3+)**

```bash
# From now on, ALL schema changes go through migrations

# Example: Add new field
python manage.py revision -m "add user last_login field"
```

```python
def upgrade(env):
    env.db._user.update_many(
        {"last_login": {"$exists": False}},
        {"$set": {"last_login": None}}
    )
    env.db._user.create_index("last_login")

def downgrade(env):
    env.db._user.drop_index("last_login_1")
    env.db._user.update_many({}, {"$unset": {"last_login": ""}})
```

```bash
# Apply to staging first
python manage.py upgrade head

# Test thoroughly

# Then apply to production
python manage.py upgrade head
```

---

### Option 3: Dual System (Transitional Period)

**When to use:**
- Want to try new system without commitment
- Need both systems running temporarily

**Setup:**

```python
# baseapp/api/v1/init.py - Keep BOTH endpoints

# Old system (keep for now)
@router.post("/database")
async def init_database_legacy():
    """Legacy JSON-based initialization"""
    response = _crud.create_db()
    return ApiResponse(...)

# New system (available in parallel)
@router.post("/migration/upgrade")
async def upgrade_database():
    """New Alembic-like migration"""
    manager = MigrationManager()
    manager.upgrade("head")
    return ApiResponse(...)
```

**Workflow:**

```bash
# Dev Environment: Use new system
python manage.py revision -m "add feature"
python manage.py upgrade head

# Staging: Test with new system
python manage.py upgrade head

# Production: Still using old system (for now)
POST /v1/init/database

# After confidence built: Switch production to new system
python manage.py upgrade head
```

---

## 🔨 Practical Examples

### Example 1: Converting Existing JSON Schema to Migration

**Your mongodb.json:**
```json
{
    "wallet": {
        "index": ["user_id", "org_id", {"user_org": ["user_id", "org_id"]}],
        "data": [
            {"id": "system_wallet", "balance": 0}
        ]
    }
}
```

**Converted to Migration:**
```python
# python manage.py revision -m "add wallet collection"

def upgrade(env):
    env.create_collection('wallet')
    
    # Indexes from JSON
    env.db.wallet.create_index("user_id")
    env.db.wallet.create_index("org_id")
    env.db.wallet.create_index(
        [("user_id", 1), ("org_id", 1)],
        name="user_org"
    )
    
    # Initial data from JSON
    env.db.wallet.insert_one({
        "_id": "system_wallet",
        "balance": 0
    })

def downgrade(env):
    env.drop_collection('wallet')
```

---

### Example 2: Adding to Existing System

**Scenario:** Production has collections from JSON, need to add new feature

```bash
# 1. Create baseline (one-time)
python manage.py revision -m "baseline - current production"

# Edit to match current production exactly (from mongodb.json)
# Then apply (this just marks current state)
python manage.py upgrade head

# 2. Create new feature migration
python manage.py revision -m "add loyalty program"
```

```python
def upgrade(env):
    # New collection
    env.create_collection('loyalty_points')
    
    env.db.loyalty_points.create_index([("user_id", 1), ("org_id", 1)])
    env.db.loyalty_points.create_index("created_at")
    
    # Add loyalty_points field to existing users
    env.db._user.update_many(
        {"loyalty_points": {"$exists": False}},
        {"$set": {"loyalty_points": 0}}
    )

def downgrade(env):
    env.drop_collection('loyalty_points')
    env.db._user.update_many({}, {"$unset": {"loyalty_points": ""}})
```

```bash
# 3. Apply new migration
python manage.py upgrade head
```

---

## ⚠️ Important Considerations

### DO's ✅

1. **Test in Staging First**
   ```bash
   # Always test migration path in staging
   python manage.py upgrade head
   python manage.py downgrade -1
   python manage.py upgrade head
   ```

2. **Backup Before Migration**
   ```bash
   # Backup production before first migration
   mongodump --db your_database --out backup_YYYYMMDD
   ```

3. **Use Baseline for Existing Production**
   ```python
   # Don't try to recreate existing collections
   # Use baseline approach instead
   ```

4. **Keep Old System During Transition**
   ```python
   # Keep both endpoints available during transition
   # Remove old system only after confidence
   ```

### DON'Ts ❌

1. **Don't Drop Production Collections**
   ```python
   # ❌ NEVER in production
   def upgrade(env):
       env.drop_collection('users')  # DON'T!
   ```

2. **Don't Apply Untested Migrations to Production**
   ```bash
   # ❌ Bad
   git pull
   python manage.py upgrade head  # on production without testing
   
   # ✅ Good
   # Test in staging → Review → Then production
   ```

3. **Don't Skip Versions**
   ```bash
   # ❌ Bad
   # Currently at: 20241210120000
   python manage.py upgrade 20241210150000  # Skip 2 versions
   
   # ✅ Good
   python manage.py upgrade head  # Apply all in sequence
   ```

---

## 📋 Transition Checklist

### Pre-Transition
- [ ] Backup production database
- [ ] Document current schema (from mongodb.json)
- [ ] Setup staging environment
- [ ] Test new migration system in dev
- [ ] Create baseline migration
- [ ] Test baseline in staging

### During Transition
- [ ] Keep old system available
- [ ] Apply baseline to production
- [ ] All new changes via migration
- [ ] Monitor for issues
- [ ] Train team on new workflow

### Post-Transition
- [ ] All environments using new system
- [ ] Old JSON-based endpoints deprecated
- [ ] Documentation updated
- [ ] Team comfortable with workflow
- [ ] Remove old system code

---

## 🎓 Training Your Team

### For Developers

**Old Workflow:**
```bash
# 1. Edit mongodb.json
# 2. POST /v1/init/database
# 3. Hope it works
```

**New Workflow:**
```bash
# 1. Create migration
python manage.py revision -m "add feature"

# 2. Edit migration file
nano migrations/versions/TIMESTAMP_add_feature.py

# 3. Test in dev
python manage.py upgrade head

# 4. Commit to git
git add migrations/
git commit -m "Add feature migration"

# 5. Deploy
# CI/CD runs: python manage.py upgrade head
```

### Common Questions

**Q: What if I make a mistake in migration?**
```bash
# Rollback
python manage.py downgrade -1

# Fix migration file
nano migrations/versions/TIMESTAMP_xxx.py

# Re-apply
python manage.py upgrade head
```

**Q: Can multiple developers create migrations at same time?**
```bash
# Yes! Each gets unique timestamp
# Developer A: 20241210120000_add_wallet.py
# Developer B: 20241210120100_add_loyalty.py

# Both merge cleanly
# Apply in order: upgrade head applies both
```

**Q: How to handle production emergency?**
```bash
# Create hotfix migration
python manage.py revision -m "hotfix user index"

# Test in staging
python manage.py upgrade head

# Apply to production
python manage.py upgrade head

# Takes ~seconds, not manual SQL
```

---

## ✅ Success Criteria

You've successfully transitioned when:

- ✅ All schema changes go through migrations
- ✅ No more direct mongodb.json edits
- ✅ Team comfortable with `python manage.py` commands
- ✅ CI/CD automatically runs migrations
- ✅ Can rollback any migration safely
- ✅ Production runs smoothly for 1+ month

---

## 🚀 Next Steps

1. **Choose Your Strategy:**
   - Fresh start → Option 1
   - Existing production → Option 2
   - Not sure → Option 3 (dual system)

2. **Start Small:**
   ```bash
   # Begin with one simple migration
   python manage.py revision -m "test migration"
   python manage.py upgrade head
   ```

3. **Build Confidence:**
   - Test thoroughly in dev
   - Apply to staging
   - Monitor results
   - Then production

4. **Train Team:**
   - Share this guide
   - Pair programming for first migrations
   - Document team-specific workflows

5. **Iterate:**
   - Gather feedback
   - Refine process
   - Update documentation

---

## 📞 Support

If issues during transition:

1. **Check Logs:**
   ```bash
   python manage.py history
   python manage.py current
   # Check application logs
   ```

2. **Rollback if Needed:**
   ```bash
   python manage.py downgrade -1
   ```

3. **Restore from Backup:**
   ```bash
   mongorestore --db your_database backup_YYYYMMDD/
   ```

Good luck with your transition! 🎉
