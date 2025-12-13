from pymongo.errors import PyMongoError
from typing import List

from baseapp.config import setting, mongodb
from baseapp.utils.logger import Logger

config = setting.get_settings()
logger = Logger("baseapp.services.permission_check_service")

class PermissionChecker:
    def __init__(self, permissions_collection="_featureonrole"):
        self.permissions_collection = permissions_collection

    def _check_logic(self, mongo_conn, roles: List, f_id: str, required_permission: int) -> bool:
        """
        Memeriksa apakah salah satu role pengguna memiliki izin yang diperlukan.

        :param roles: Array role, misalnya ["role1","role2"].
        :param f_id: ID fitur/entitas yang diperiksa (contoh: "_enum").
        :param required_permission: Izin yang dibutuhkan (contoh: 1 untuk read).
        :return: True jika salah satu role memiliki izin, False jika tidak.
        """
        collection = mongo_conn.get_database()[self.permissions_collection]
        try:
            permissions = collection.find({"r_id": {"$in": roles}, "f_id": f_id})
            for permission in permissions:
                # Cek izin menggunakan bitwise AND
                if (permission["permission"] & required_permission) == required_permission:
                    return True
            return False
        except PyMongoError as pme:
            logger.error(f"Database error occurred: {str(pme)}")
            raise ValueError("Database error occurred while checking permission.") from pme
        except Exception as e:
            logger.exception(f"Unexpected error occurred while checking permission: {str(e)}")
            raise

    def has_permission(self, roles: List, f_id: str, required_permission: int, mongo_conn=None) -> bool:
        """
        Memeriksa izin. 
        Jika `mongo_conn` disediakan, gunakan koneksi tersebut. 
        Jika tidak, buka koneksi baru.
        """
        if mongo_conn:
            # Gunakan koneksi yang dilempar dari CRUD (Reuse Connection)
            return self._check_logic(mongo_conn, roles, f_id, required_permission)
        else:
            # Buka koneksi sendiri (Standalone)
            with mongodb.MongoConn() as mongo:
                return self._check_logic(mongo, roles, f_id, required_permission)
