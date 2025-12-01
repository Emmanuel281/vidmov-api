import logging

from pymongo.errors import PyMongoError
from typing import Optional, Dict, Any

from baseapp.config import setting, mongodb
from baseapp.services._feature.model import Feature
from baseapp.services.audit_trail_service import AuditTrailService
from baseapp.utils.utility import get_enum, generate_uuid

config = setting.get_settings()
logger = logging.getLogger(__name__)

class CRUD:
    def __init__(self):
        self.collection_feature = "_feature"
        self.collection_feature_on_role = "_featureonrole"

    def set_context(self, user_id: str, org_id: str, authority: int, ip_address: Optional[str] = None, user_agent: Optional[str] = None):
        """
        Memperbarui konteks pengguna dan menginisialisasi AuditTrailService.
        """
        self.user_id = user_id
        self.org_id = org_id
        self.authority = authority
        self.ip_address = ip_address
        self.user_agent = user_agent

        # Inisialisasi atau perbarui AuditTrailService dengan konteks terbaru
        self.audit_trail = AuditTrailService(
            user_id=self.user_id,
            org_id=self.org_id,
            ip_address=self.ip_address,
            user_agent=self.user_agent
        )

    def set_permission(self, data: Feature):
        """
        Update a role's data by ID.
        """
        with mongodb.MongoConn() as mongo:
            collection_feature = mongo.get_database()[self.collection_feature]
            collection_role = mongo.get_database()[self.collection_feature_on_role]
            bitRA = get_enum(mongo,"ROLEACTION")
            bitRA = bitRA["value"]
            obj = data.model_dump()
            try:
                get_feature = collection_feature.find_one({"_id":obj["f_id"]})
                if not get_feature:
                    raise ValueError("Feature not found")
                
                if (get_feature["negasiperm"][str(self.authority)] & bitRA[obj['key_action']]) != 0:
                    raise ValueError("Action not permitted")
                
                get_role = collection_role.find_one({"r_id": obj["r_id"],"f_id":obj["f_id"]})
                if get_role:
                    resPerm = bitRA[obj['key_action']] | get_role['permission']
                    if resPerm == get_role['permission']:
                        resPerm = get_role['permission'] - bitRA[obj['key_action']]

                    upd_obj = {'permission':resPerm}
                    update_permission = collection_role.find_one_and_update({"r_id": obj["r_id"],"f_id":obj["f_id"]}, {"$set": upd_obj}, return_document=True)
                    if not update_permission:
                        # write audit trail for fail
                        self.audit_trail.log_audittrail(
                            mongo,
                            action="update",
                            target=self.collection_feature_on_role,
                            target_id=update_permission["_id"],
                            details={"$set": upd_obj},
                            status="failure",
                            error_message="Update permission failed"
                        )
                        raise ValueError("Update permission failed")
                    # write audit trail for success
                    self.audit_trail.log_audittrail(
                        mongo,
                        action="update",
                        target=self.collection_feature_on_role,
                        target_id=update_permission["_id"],
                        details={"$set": upd_obj},
                        status="success"
                    )
                    return update_permission
                else:
                    resPerm = bitRA[obj['key_action']]
                    obj_add = {}
                    obj_add["_id"] = generate_uuid()
                    obj_add["r_id"] = obj["r_id"]
                    obj_add["f_id"] = obj["f_id"]
                    obj_add["permission"] = resPerm
                    obj_add["org_id"] = self.org_id
                    result = collection_role.insert_one(obj_add) 
                    return obj
            except PyMongoError as pme:
                logger.error(f"Database error occurred: {str(pme)}")
                raise ValueError("Database error occurred while update document.") from pme
            except Exception as e:
                logger.exception(f"Error updating role: {str(e)}")
                raise

    def get_all(self, filters: Optional[Dict[str, Any]] = None):
        """
        Retrieve all documents from the collection with optional filters, pagination, and sorting.
        """
        with mongodb.MongoConn() as mongo:
            collection_feature = mongo.get_database()[self.collection_feature]
            collection_feature_on_role = mongo.get_database()[self.collection_feature_on_role]
            bitRA = get_enum(mongo,"ROLEACTION")
            bitRA = bitRA["value"]
            try:
                # Apply filters
                query_filter = filters or {}

                # Selected fields
                selected_fields_1 = {
                    "id": "$_id",
                    "feature_name": 1,
                    "authority": 1,
                    "negasiperm": 1,
                    "_id": 0
                }
                selected_fields_2 = {
                    "id": "$_id",
                    "r_id": 1,
                    "permission": 1,
                    "f_id": 1,
                    "_id": 0
                }

                # Aggregation pipeline
                pipeline_1 = [
                    {"$project": selected_fields_1}  # Project only selected fields
                ]

                pipeline_2 = [
                    {"$match": query_filter},  # Filter stage
                    {"$project": selected_fields_2}  # Project only selected fields
                ]

                # Execute aggregation pipeline
                cursor_1 = collection_feature.aggregate(pipeline_1)
                cursor_2 = collection_feature_on_role.aggregate(pipeline_2)
                
                results_1 = list(cursor_1)
                results_2 = list(cursor_2)

                rolesFeature = {}
                for i in results_2:
                    rolesFeature[i['f_id']]=i['permission']
                    
                results = []
                for i, data in enumerate(results_1):
                    if self.authority & data['authority']: 
                        # 2 unassigned
                        # 1 : assigned
                        # 0 : disable
                        defaultStatus = 2
                        if filters["r_id"] == 'dataawal':
                            defaultStatus = 0
                        
                        for x1 in bitRA:
                            data[x1] = defaultStatus

                        if len(results_2) > 0:
                            if rolesFeature.get(data['id']) != None:
                                for x2 in bitRA:
                                    data[x2] = 1 if bitRA[x2] & rolesFeature[data['id']] else 2

                        if data['negasiperm'][str(self.authority)] & data['negasiperm'][str(self.authority)] > 0:
                            for x3 in bitRA:
                                data[x3] = 0 if bitRA[x3] & data['negasiperm'][str(self.authority)] else data[x3]

                        if 'negasiperm' in data:
                            del data['negasiperm']
                        if 'authority' in data:
                            del data['authority']

                        results.append(data)

                # write audit trail for success
                self.audit_trail.log_audittrail(
                    mongo,
                    action="retrieve",
                    target=self.collection_feature_on_role,
                    target_id="agregate",
                    details={"aggregate": pipeline_2},
                    status="success"
                )

                return results
            except PyMongoError as pme:
                logger.error(f"Error retrieving role with filters and pagination: {str(e)}")
                # write audit trail for success
                self.audit_trail.log_audittrail(
                    mongo,
                    action="retrieve",
                    target=self.collection_feature_on_role,
                    target_id="agregate",
                    details={"aggregate": pipeline_2},
                    status="failure"
                )
                raise ValueError("Database error while retrieve document") from pme
            except Exception as e:
                logger.exception(f"Unexpected error during deletion: {str(e)}")
                raise
