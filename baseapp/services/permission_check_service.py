from pymongo.errors import PyMongoError
from typing import List

from baseapp.config import setting, mongodb

config = setting.get_settings()

class PermissionChecker:
    def __init__(self, permissions_collection="_featureonrole"):
        self.permissions_collection = permissions_collection

    def has_permission(self, roles: List, f_id: str, required_permission: int) -> bool:
        """
        Memeriksa apakah salah satu role pengguna memiliki izin yang diperlukan.

        :param roles: Array role, misalnya ["role1","role2"].
        :param f_id: ID fitur/entitas yang diperiksa (contoh: "_enum").
        :param required_permission: Izin yang dibutuhkan (contoh: 1 untuk read).
        :return: True jika salah satu role memiliki izin, False jika tidak.
        """
        client = mongodb.MongoConn()
        with client as mongo:
            collection = mongo._db[self.permissions_collection]
            try:               
                # Cari semua role yang relevan di database
                permissions = collection.find({"r_id": {"$in": roles}, "f_id": f_id})

                for permission in permissions:
                    # Cek izin menggunakan bitwise AND
                    if (permission["permission"] & required_permission) == required_permission:
                        return True

                return False
            except PyMongoError as pme:
                self.logger.error(f"Database error occurred: {str(pme)}")
                raise ValueError("Database error occurred while checking permission.") from pme
            except Exception as e:
                self.logger.exception(f"Unexpected error occurred while checking permission: {str(e)}")
                raise
