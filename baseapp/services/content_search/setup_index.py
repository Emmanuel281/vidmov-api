"""
Script untuk setup OpenSearch index.
Dipanggil saat container startup (init_opensearch).
"""

import sys
from baseapp.config.opensearch import OpenSearchConn
from baseapp.services.content_search.crud import ContentSearchCRUD
from baseapp.utils.logger import Logger

logger = Logger("services.content_search.setup_index")

def setup_index():
    """
    Setup OpenSearch index dengan mapping.
    Idempotent - tidak akan error jika index sudah ada.
    """
    try:
        logger.info("Initializing OpenSearch connection...")
        OpenSearchConn.initialize()
        
        logger.info("Setting up content_search index...")
        
        with ContentSearchCRUD() as crud:
            try:
                crud.setup_index()
                logger.info("✓ OpenSearch index 'content_search' created successfully")
                return True
            except Exception as e:
                # Check if error is "index already exists"
                error_msg = str(e).lower()
                if "already exists" in error_msg or "resource_already_exists" in error_msg:
                    logger.info("✓ OpenSearch index 'content_search' already exists")
                    return True
                else:
                    logger.error(f"Failed to create index: {e}")
                    raise
                    
    except Exception as e:
        logger.error(f"Fatal error during OpenSearch setup: {e}")
        return False
    finally:
        try:
            OpenSearchConn.close_connection()
            logger.info("OpenSearch connection closed")
        except:
            pass

if __name__ == "__main__":
    try:
        success = setup_index()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.warning("Setup interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)