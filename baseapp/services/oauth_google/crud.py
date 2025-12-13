from urllib import parse

from pymongo.errors import PyMongoError
from typing import Optional
from datetime import datetime, timezone

import requests

from baseapp.config import setting, mongodb
from baseapp.utils.logger import Logger
from baseapp.services.oauth_google.model import Google, GoogleToken
from baseapp.services.audit_trail_service import AuditTrailService

config = setting.get_settings()
logger = Logger("baseapp.services.oauth_google.crud")

class CRUD:
    def __init__(self, collection_name="_user"):
        self.collection_name = collection_name
        self.httpsOptionsForGoogle = "https://www.googleapis.com:443"
        self.redirect_uri = config.google_redirect_uri

    def __enter__(self):
        self._mongo_context = mongodb.MongoConn()
        self.mongo = self._mongo_context.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if hasattr(self, '_mongo_context'):
            return self._mongo_context.__exit__(exc_type, exc_value, traceback)
        return False
    
    def set_context(self, user_id: str, org_id: str, ip_address: Optional[str] = None, user_agent: Optional[str] = None):
        """
        Memperbarui konteks pengguna dan menginisialisasi AuditTrailService.
        """
        self.user_id = user_id
        self.org_id = org_id
        self.ip_address = ip_address
        self.user_agent = user_agent

        # Inisialisasi atau perbarui AuditTrailService dengan konteks terbaru
        self.audit_trail = AuditTrailService(
            user_id=self.user_id,
            org_id=self.org_id,
            ip_address=self.ip_address,
            user_agent=self.user_agent
        )

    def link_to_google(self, data: GoogleToken):
        """
        Link current account to google.
        """
        collection = self.mongo.get_database()[self.collection_name]
        try:
            profile = self._getProfile(data.access_token)
            google_data = Google(email=profile["email"],id=profile["id"],name=profile["name"],picture=profile["picture"])
            obj = {"google":google_data.model_dump()}
            obj["mod_by"] = self.user_id
            obj["mod_date"] = datetime.now(timezone.utc)

            user = collection.find_one({"google.email": profile["email"]})
            if user:
                raise ValueError("This Gmail is already linked to another account. Try using a different one")
            
            update_user = collection.find_one_and_update({"_id": self.user_id}, {"$set": obj}, return_document=True)
            if not update_user:
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    self.mongo,
                    action="update",
                    target=self.collection_name,
                    target_id=self.user_id,
                    details={"$set": obj},
                    status="failure",
                    error_message="User not found"
                )
                raise ValueError("User not found")
            # write audit trail for success
            self.audit_trail.log_audittrail(
                self.mongo,
                action="update",
                target=self.collection_name,
                target_id=self.user_id,
                details={"$set": obj},
                status="success"
            )
            del update_user["password"]
            return update_user
        except PyMongoError as pme:
            logger.error(f"Database error occurred: {str(pme)}")
            # write audit trail for fail
            self.audit_trail.log_audittrail(
                self.mongo,
                action="update",
                target=self.collection_name,
                target_id=self.user_id,
                details={"$set": obj},
                status="failure",
                error_message=str(pme)
            )
            raise ValueError("Database error occurred while update document.") from pme
        except Exception as e:
            logger.exception(f"Error updating user: {str(e)}")
            raise

    def login_google(self, auth_code):
        postData = parse.urlencode({
            "code": auth_code,
            "client_id": config.google_client_id,
            "client_secret": config.google_client_secret,
            "redirect_uri": self.redirect_uri,
            "grant_type": 'authorization_code'
        })

        path = f'/token'

        options = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Content-Length': f"{len(postData)}"
        }

        try:
            res = requests.post(f'https://oauth2.googleapis.com:443{path}', data=postData, headers=options)
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)

        getRes = res.json()
        return getRes
    
    def _getProfile(self, token):
        path = f"/oauth2/v1/userinfo?alt=json"
        options = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + token
        }
        try:
            res = requests.get(f'{self.httpsOptionsForGoogle}{path}', headers=options)
        except requests.exceptions.RequestException as e:
            raise ValueError(f"Google oauth error: {e}.") from e
            # raise SystemExit(e)
        getRes = res.json()
        return getRes
    
    def get_by_google_id(self, data: GoogleToken):
        """
        Retrieve a user by Google ID.
        """
        collection = self.mongo.get_database()[self.collection_name]
        try:
            profile = self._getProfile(data.access_token)
            user = collection.find_one({"google.email": profile["email"]})
            if not user:
                raise ValueError("User not found")
            return user
        except PyMongoError as pme:
            logger.error(f"Database error occurred: {str(pme)}")
            raise ValueError("Database error occurred while find document.") from pme
        except Exception as e:
            logger.exception(f"Unexpected error occurred while finding document: {str(e)}")
            raise

    def unlink_to_google(self):
        """
        Unlink current account from google
        """
        collection = self.mongo.get_database()[self.collection_name]
        try:
            obj = {"google":None}
            obj["mod_by"] = self.user_id
            obj["mod_date"] = datetime.now(timezone.utc)

            update_user = collection.find_one_and_update({"_id": self.user_id}, {"$set": obj}, return_document=True)
            if not update_user:
                # write audit trail for fail
                self.audit_trail.log_audittrail(
                    self.mongo,
                    action="update",
                    target=self.collection_name,
                    target_id=self.user_id,
                    details={"$set": obj},
                    status="failure",
                    error_message="User not found"
                )
                raise ValueError("User not found")
            # write audit trail for success
            self.audit_trail.log_audittrail(
                self.mongo,
                action="update",
                target=self.collection_name,
                target_id=self.user_id,
                details={"$set": obj},
                status="success"
            )
            del update_user["password"]
            return update_user
        except PyMongoError as pme:
            logger.error(f"Database error occurred: {str(pme)}")
            # write audit trail for fail
            self.audit_trail.log_audittrail(
                self.mongo,
                action="update",
                target=self.collection_name,
                target_id=self.user_id,
                details={"$set": obj},
                status="failure",
                error_message=str(pme)
            )
            raise ValueError("Database error occurred while update document.") from pme
        except Exception as e:
            logger.exception(f"Error updating user: {str(e)}")
            raise