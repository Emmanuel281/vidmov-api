"""
MongoDB Schema Models - Similar to SQLAlchemy Base Models
File: baseapp/models/mongodb_schema.py

Define your MongoDB collections as Python classes,
similar to how you define SQLAlchemy models.
"""
from typing import List, Dict, Any, Optional
from enum import Enum


class IndexType(Enum):
    """Index types"""
    ASCENDING = 1
    DESCENDING = -1
    TEXT = "text"
    GEOSPATIAL = "2dsphere"


class Index:
    """Represents a MongoDB index"""
    
    def __init__(self, 
                 fields: List[tuple] | str,
                 name: Optional[str] = None,
                 unique: bool = False,
                 sparse: bool = False,
                 background: bool = False):
        """
        Define an index
        
        Args:
            fields: List of (field_name, direction) tuples or single field name
            name: Index name (auto-generated if not provided)
            unique: Create unique index
            sparse: Create sparse index
            background: Create in background
        
        Examples:
            Index("email", unique=True)
            Index([("email", 1), ("org_id", 1)], name="email_org_idx")
        """
        if isinstance(fields, str):
            self.fields = [(fields, 1)]
        else:
            self.fields = fields
        
        self.name = name
        self.unique = unique
        self.sparse = sparse
        self.background = background
    
    def to_mongo_index(self):
        """Convert to MongoDB index specification"""
        return {
            'fields': self.fields,
            'name': self.name,
            'unique': self.unique,
            'sparse': self.sparse,
            'background': self.background
        }


class Collection:
    """Base class for MongoDB collection definitions"""
    
    # Override these in subclasses
    __collection_name__: str = None
    __indexes__: List[Index] = []
    __initial_data__: List[Dict[str, Any]] = []
    
    @classmethod
    def get_collection_name(cls) -> str:
        """Get collection name"""
        return cls.__collection_name__ or cls.__name__.lower()
    
    @classmethod
    def get_indexes(cls) -> List[Index]:
        """Get collection indexes"""
        return cls.__indexes__
    
    @classmethod
    def get_initial_data(cls) -> List[Dict[str, Any]]:
        """Get initial data for collection"""
        return cls.__initial_data__


# ============================================
# Core System Collections
# ============================================

class AuditTrail(Collection):
    """Audit trail collection"""
    __collection_name__ = "_audittrail"
    __indexes__ = [
        Index([("rec_date", 1), ("org_id", 1), ("uid", 1)])
    ]


class Organization(Collection):
    """Organization collection"""
    __collection_name__ = "_organization"
    __indexes__ = [
        Index([("rec_date", 1), ("authority", 1), ("ref_id", 1), ("plan_id", 1)])
    ]


class Role(Collection):
    """Role collection"""
    __collection_name__ = "_role"
    __indexes__ = [
        Index("rec_date"),
        Index([("name", 1), ("org_id", 1)])
    ]


class User(Collection):
    """User collection"""
    __collection_name__ = "_user"
    __indexes__ = [
        Index("rec_date"),
        Index("username", unique=True),
        Index("email", unique=True),
        Index("org_id"),
        Index("r_id"),
        Index([("id", 1), ("org_id", 1)], name="id_orgid"),
        Index([("username", 1), ("org_id", 1)], name="username_orgid"),
        Index([("email", 1), ("org_id", 1)], name="email_orgid")
    ]


# ============================================
# Enum and Configuration
# ============================================

class Enum(Collection):
    """Enum collection"""
    __collection_name__ = "_enum"
    __indexes__ = [
        Index("app"),
        Index("mod"),
        Index("code"),
        Index("rec_date"),
        Index("org_id"),
        Index("type"),
        Index([("app", 1), ("mod", 1)], name="app_mod"),
        Index([("app", 1), ("mod", 1), ("org_id", 1)], name="app_mod_org"),
        Index([("org_id", 1), ("type", 1)], name="org_type"),
        Index([("app", 1), ("mod", 1), ("_sort", 1)], name="app_mod_sort")
    ]
    __initial_data__ = [
        {
            "_id": "dmsDataType",
            "app": "baseapp",
            "mod": "dmsDataType",
            "code": "dmsDataType",
            "type": "hardcoded",
            "value": ["String", "Integer", "Datetime"]
        },
        {
            "_id": "SEX",
            "app": "baseapp",
            "mod": "SEX",
            "code": "SEX",
            "type": "hardcoded",
            "value": ["Male", "Female"]
        },
        {
            "_id": "GENRE",
            "app": "baseapp",
            "mod": "_enum",
            "code": "genre",
            "type": "hardcoded",
            "value": "Genre"
        }
    ]


class Feature(Collection):
    """Feature collection"""
    __collection_name__ = "_feature"
    __indexes__ = [
        Index("feature_name")
    ]
    __initial_data__ = [
        {
            "_id": "_user",
            "feature_name": "_user",
            "negasiperm": {
                "1": 248,
                "2": 248,
                "4": 248,
                "8": 248
            },
            "authority": 15
        },
        {
            "_id": "_role",
            "feature_name": "_role",
            "negasiperm": {
                "1": 248,
                "2": 248,
                "4": 255,
                "8": 255
            },
            "authority": 3
        },
        {
            "_id": "_enum",
            "feature_name": "_enum",
            "negasiperm": {
                "1": 240,
                "2": 255,
                "4": 255,
                "8": 255
            },
            "authority": 1
        },
        {
            "_id": "_audittrail",
            "feature_name": "_audittrail",
            "negasiperm": {
                "1": 254,
                "2": 254,
                "4": 254,
                "8": 254
            },
            "authority": 15
        },
        {
            "_id": "_feature",
            "feature_name": "_feature",
            "negasiperm": {
                "1": 250,
                "2": 250,
                "4": 255,
                "8": 255
            },
            "authority": 3
        },
        {
            "_id": "_menu",
            "feature_name": "_menu",
            "negasiperm": {
                "1": 250,
                "2": 255,
                "4": 255,
                "8": 255
            },
            "authority": 1
        },
        {
            "_id": "_organization",
            "feature_name": "_organization",
            "negasiperm": {
                "1": 248,
                "2": 250,
                "4": 250,
                "8": 250
            },
            "authority": 15
        },
        {
            "_id": "_dashboard",
            "feature_name": "_dashboard",
            "negasiperm": {
                "1": 254,
                "2": 254,
                "4": 254,
                "8": 254
            },
            "authority": 15
        },
        {
            "_id": "_myorg",
            "feature_name": "_myorg",
            "negasiperm": {
                "1": 250,
                "2": 250,
                "4": 250,
                "8": 250
            },
            "authority": 15
        },
        {
            "_id": "_myprofile",
            "feature_name": "_myprofile",
            "negasiperm": {
                "1": 250,
                "2": 250,
                "4": 250,
                "8": 250
            },
            "authority": 15
        },
        {
            "_id": "_dmsindexlist",
            "feature_name": "_dmsindexlist",
            "negasiperm": {
                "1": 248,
                "2": 255,
                "4": 255,
                "8": 255
            },
            "authority": 1
        },
        {
            "_id": "_dmsdoctype",
            "feature_name": "_dmsdoctype",
            "negasiperm": {
                "1": 248,
                "2": 255,
                "4": 255,
                "8": 255
            },
            "authority": 1
        },
        {
            "_id": "_dmsbrowse",
            "feature_name": "_dmsbrowse",
            "negasiperm": {
                "1": 96,
                "2": 255,
                "4": 255,
                "8": 255
            },
            "authority": 1
        },
        {
            "_id": "content",
            "feature_name": "content",
            "negasiperm": {
                "1": 64,
                "2": 254,
                "4": 254,
                "8": 254
            },
            "authority": 15
        },
        {
            "_id": "register",
            "feature_name": "register",
            "negasiperm": {
                "1": 255,
                "2": 255,
                "4": 252,
                "8": 255
            },
            "authority": 4
        },
        {
            "_id": "wallet",
            "feature_name": "wallet",
            "negasiperm": {
                "1": 248,
                "2": 252,
                "4": 252,
                "8": 252
            },
            "authority": 15
        },
        {
            "_id": "topup",
            "feature_name": "topup",
            "negasiperm": {
                "1": 248,
                "2": 252,
                "4": 252,
                "8": 252
            },
            "authority": 15
        },
        {
            "_id": "ads",
            "feature_name": "ads",
            "negasiperm": {
                "1": 254,
                "2": 240,
                "4": 255,
                "8": 255
            },
            "authority": 3
        },
        {
            "_id": "qr_product",
            "feature_name": "qr_product",
            "negasiperm": {
                "1": 254,
                "2": 240,
                "4": 255,
                "8": 255
            },
            "authority": 3
        },
        {
            "_id": "bundling",
            "feature_name": "bundling",
            "negasiperm": {
                "1": 254,
                "2": 240,
                "4": 255,
                "8": 255
            },
            "authority": 3
        },
        {
            "_id": "giveaway",
            "feature_name": "giveaway",
            "negasiperm": {
                "1": 254,
                "2": 240,
                "4": 255,
                "8": 252
            },
            "authority": 11
        }
    ]


class FeatureOnRole(Collection):
    """Feature on role mapping"""
    __collection_name__ = "_featureonrole"
    __indexes__ = [
        Index("f_id"),
        Index("r_id"),
        Index("org_id"),
        Index([("r_id", 1), ("f_id", 1)], name="rf_id")
    ]


# ============================================
# DMS Collections
# ============================================

class DMSIndexList(Collection):
    """DMS index list"""
    __collection_name__ = "_dmsindexlist"
    __indexes__ = [
        Index("rec_date"),
        Index("name"),
        Index("org_id"),
        Index([("name", 1), ("org_id", 1)], name="index_orgid")
    ]


class DMSDocType(Collection):
    """DMS document type"""
    __collection_name__ = "_dmsdoctype"
    __indexes__ = [
        Index("rec_date"),
        Index("org_id"),
        Index([("name", 1), ("org_id", 1)], name="name_orgid")
    ]


class DMSFolder(Collection):
    """DMS folder"""
    __collection_name__ = "_dmsfolder"
    __indexes__ = [
        Index("rec_date"),
        Index("folder_name"),
        Index("level"),
        Index("pid"),
        Index("org_id"),
        Index([("level", 1), ("org_id", 1)], name="_lo"),
        Index([("folder_name", 1), ("level", 1), ("org_id", 1)], name="_flo"),
        Index([("folder_name", 1), ("level", 1), ("org_id", 1), ("pid", 1)], name="_flop")
    ]


class DMSFile(Collection):
    """DMS file"""
    __collection_name__ = "_dmsfile"
    __indexes__ = [
        Index("rec_date"),
        Index("doctype"),
        Index("folder_id"),
        Index("refkey_id"),
        Index("org_id"),
        Index([("refkey_id", 1), ("refkey_table", 1)], name="refkey_id_table")
    ]


# ============================================
# Business Domain Collections
# ============================================

class Content(Collection):
    """Content collection"""
    __collection_name__ = "content"
    __indexes__ = [
        Index("rec_date"),
        Index("org_id")
    ]


class ContentVideo(Collection):
    """Content video collection"""
    __collection_name__ = "content_video"
    __indexes__ = [
        Index("rec_date"),
        Index("content_id"),
        Index("org_id")
    ]


class Ads(Collection):
    """Ads collection"""
    __collection_name__ = "ads"
    __indexes__ = [
        Index("rec_date"),
        Index("uid"),
        Index("org_id")
    ]


class QRProduct(Collection):
    """QR Product collection"""
    __collection_name__ = "qr_product"
    __indexes__ = [
        Index("rec_date"),
        Index("uid"),
        Index("org_id")
    ]


class Bundling(Collection):
    """Bundling collection"""
    __collection_name__ = "bundling"
    __indexes__ = [
        Index("rec_date"),
        Index("uid"),
        Index("org_id")
    ]


class Giveaway(Collection):
    """Giveaway collection"""
    __collection_name__ = "giveaway"
    __indexes__ = [
        Index("rec_date"),
        Index("uid"),
        Index("org_id")
    ]


# ============================================
# Schema Registry
# ============================================

# Register all collections here for autogenerate
ALL_COLLECTIONS = [
    # Core
    AuditTrail,
    Organization,
    Role,
    User,
    # Config
    Enum,
    Feature,
    FeatureOnRole,
    # DMS
    DMSIndexList,
    DMSDocType,
    DMSFolder,
    DMSFile,
    # Business
    Content,
    ContentVideo,
    Ads,
    QRProduct,
    Bundling,
    Giveaway,
]


def get_collection_by_name(name: str) -> Optional[Collection]:
    """Get collection class by name"""
    for col in ALL_COLLECTIONS:
        if col.get_collection_name() == name:
            return col
    return None


def get_all_collection_names() -> List[str]:
    """Get all collection names"""
    return [col.get_collection_name() for col in ALL_COLLECTIONS]