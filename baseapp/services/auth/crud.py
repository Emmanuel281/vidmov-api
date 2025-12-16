from typing import Optional
from baseapp.config import setting, mongodb
from baseapp.services.auth.model import UserInfo, ClientInfo
from baseapp.model.common import Status, RoleAction
from baseapp.utils.utility import check_password
from baseapp.utils.logger import Logger

config = setting.get_settings()
logger = Logger("baseapp.services.auth.crud")

class CRUD:
    def __init__(self):
        self.user_collection = "_user"
        self.org_collection = "_organization"
        self.permissions_collection = "_featureonrole"
        self.api_credentials = "_api_credentials"

    def __enter__(self):
        self._mongo_context = mongodb.MongoConn()
        self.mongo = self._mongo_context.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if hasattr(self, '_mongo_context'):
            return self._mongo_context.__exit__(exc_type, exc_value, traceback)
        return False

    def check_org(self, org_id) -> Optional[int]:
        collection = self.mongo.get_database()[self.org_collection]
        query = {"_id": org_id}
        find_org = collection.find_one(query)
        if not find_org:
            logger.warning(f"Organization with ID {org_id} not found.")
            raise ValueError("Organization not found")
        return find_org.get("authority")

    def get_role_action(self):
        bitRA = {item.name: item.value for item in RoleAction}
        return bitRA
    
    def get_feature(self, roles):
        _featureDict = {}
        collection = self.mongo.get_database()[self.permissions_collection]
        query = {"r_id": {"$in": roles}}
        find_role = collection.find(query)
        for i in find_role:
            if i['f_id'] not in _featureDict:
                _featureDict[i['f_id']] = i['permission']
            else:
                _featureDict[i['f_id']] = i['permission'] | _featureDict[i['f_id']]
        return _featureDict
    
    def validate_password(self, user_info, password: str) -> UserInfo:
        if user_info.get("status") != Status.ACTIVE.value:
            logger.warning(f"User {user_info.get('username')} is not active.")
            raise ValueError("User is not active.")
        
        if user_info.get("password") is None and user_info.get("google") is not None:
            logger.warning(f"User {user_info.get('username')} attempted login with password but is a Google account.")
            raise ValueError(
                "This account was created using Google Sign-In. "
                "Please use 'Continue with Google' to login."
            )
        
        stored_hash = user_info.get("password")
        if not stored_hash:
            logger.error(f"Password missing for user {user_info.get('username')}.")
            raise ValueError("User data is invalid.")

        if not check_password(password, stored_hash):
            logger.warning(f"User {user_info.get('username')} provided invalid password.")
            raise ValueError("Invalid password.") 
        
        return UserInfo(
            id=user_info["_id"], 
            org_id=user_info["org_id"], 
            roles=user_info["roles"], 
            authority=user_info["authority"],
            bitws=user_info["bitws"],
            feature=user_info["feature"]
        )

    def find_user(self, username: str) -> dict:
        collection = self.mongo.get_database()[self.user_collection]
        query = {"$or": [{"username": username}, {"email": username}]}
        user_info = collection.find_one(query)
        if not user_info:
            logger.warning(f"User with username or email '{username}' not found.")
            raise ValueError("User not found")
        user_info["bitws"]=self.get_role_action()
        user_info["feature"]=self.get_feature(user_info["roles"])
        return user_info

    def validate_user(self, username, password=None) -> UserInfo:
        user_info = self.find_user(username)
        authority = self.check_org(user_info["org_id"])

        user_data = {
            key: user_info.get(key, None)
            for key in ["_id", "username", "org_id", "password", "roles", "status", "bitws", "feature"]
        }
        user_data["authority"] = authority
        if password is None:
            return UserInfo(
                id=user_data["_id"], 
                org_id=user_data["org_id"], 
                roles=user_data["roles"], 
                authority=user_data["authority"],
                bitws=user_data["bitws"],
                feature=user_data["feature"]
            )
        else:
            return self.validate_password(user_data, password)
        
    def find_client_id(self, client_id: str) -> dict:
        collection = self.mongo.get_database()[self.api_credentials]
        query = {"client_id": client_id}
        client_info = collection.find_one(query)
        if not client_info:
            logger.warning(f"Client with ID '{client_id}' not found.")
            raise ValueError("Client not found")
        return client_info
    
    def validate_client(self, client_id, client_secret) -> ClientInfo:
        client_info = self.find_client_id(client_id)

        client_data = {
            key: client_info.get(key, None)
            for key in ["_id", "org_id", "client_id", "client_secret_hash"]
        }

        if client_info.get("status") != Status.ACTIVE.value:
            logger.warning(f"Client {client_id} is not active.")
            raise ValueError("Client is not active.")

        stored_hash = client_info.get("client_secret_hash")
        
        if not check_password(client_secret, stored_hash):
            logger.warning(f"Client {client_id} provided invalid secret.")
            raise ValueError("Invalid client secret.")
        
        return ClientInfo(
            id=client_data["_id"], 
            org_id=client_data["org_id"], 
            client_id=client_data["client_id"]
        )