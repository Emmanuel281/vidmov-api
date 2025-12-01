import logging, uuid

from pymongo.errors import PyMongoError
from pymongo import ASCENDING
from typing import Optional, List
from operator import itemgetter

from baseapp.config import setting, mongodb
from baseapp.services.audit_trail_service import AuditTrailService
from baseapp.utils.utility import get_enum

config = setting.get_settings()
logger = logging.getLogger(__name__)

class CRUD:
    def __init__(self):
        self.collection_feature = "_feature"
        self.collection_feature_on_role = "_featureonrole"
        self.collection_menu = "_menu"

    def set_context(self, user_id: str, org_id: str, authority: int, roles: List, ip_address: Optional[str] = None, user_agent: Optional[str] = None):
        """
        Memperbarui konteks pengguna dan menginisialisasi AuditTrailService.
        """
        self.user_id = user_id
        self.org_id = org_id
        self.authority = authority
        self.roles = roles
        self.ip_address = ip_address
        self.user_agent = user_agent

        # Inisialisasi atau perbarui AuditTrailService dengan konteks terbaru
        self.audit_trail = AuditTrailService(
            user_id=self.user_id,
            org_id=self.org_id,
            ip_address=self.ip_address,
            user_agent=self.user_agent
        )

    def get_all(self):
        """
        Retrieve all documents from the collection with optional filters, pagination, and sorting.
        """
        with mongodb.MongoConn() as mongo:
            collection_feature = mongo.get_database()[self.collection_feature]
            collection_feature_on_role = mongo.get_database()[self.collection_feature_on_role]
            collection_menu = mongo.get_database()[self.collection_menu]
            bitRA = get_enum(mongo,"ROLEACTION")
            bitRA = bitRA["value"]
            try:
                # Apply filters
                query_filter = {"r_id": {"$in": self.roles}}

                # Selected fields
                # menu
                selected_fields_1 = {
                    "id": "$_id",
                    "value": 1,
                    "icon": 1,
                    "details": 1,
                    "feature": 1,
                    "parent": 1,
                    "sortnumber": 1,
                    "_id": 0
                }
                # feature on role
                selected_fields_2 = {
                    "id": "$_id",
                    "r_id": 1,
                    "permission": 1,
                    "f_id": 1,
                    "_id": 0
                }

                # Aggregation pipeline
                # menu
                pipeline_1 = [
                    {"$project": selected_fields_1},  # Project only selected fields
                    {"$sort": {"sortnumber": ASCENDING}},  # Sorting stage
                ]

                # feature on role
                pipeline_2 = [
                    {"$match": query_filter},  # Filter stage
                    {"$project": selected_fields_2}  # Project only selected fields
                ]

                # Execute aggregation pipeline
                cursor_1 = collection_menu.aggregate(pipeline_1)
                cursor_2 = collection_feature_on_role.aggregate(pipeline_2)
                
                results_1 = list(cursor_1)
                results_2 = list(cursor_2)

                rolesFeature = {}
                for i in results_2:
                    if i['f_id'] not in rolesFeature:
                        rolesFeature[i['f_id']] = i['permission']
                    else:
                        rolesFeature[i['f_id']] = i['permission'] | rolesFeature[i['f_id']]
                logger.debug(f"roles of feature: {rolesFeature}")
                    
                resultsMenu = []
                resultsParent = []
                resultsParentID = {}
                for data in results_1:
                    # region join to feature table
                    data["feature_docs"] = None
                    if data['feature'] != "":
                        data["feature_docs"] = collection_feature.find_one({"_id":data['feature']})
                    # endregion

                    # region 
                    # apabila menggunakan model submenu maka script dibawah di aktifkan
                    if data['parent'] == '' and data['feature'] == "":
                        resultsParent.append({
                            'id':data['id'],
                            'value':data['value'],
                            'details':data['details'],
                            'icon':f"mdi mdi-{data['icon']}",
                            'parent':'',
                            'sortnumber':data['sortnumber']
                        })
                    # endregion

                    logger.debug(f"data menu : {data}")
                    if data['feature_docs'] != None:
                        if self.authority & data['feature_docs']['authority']:
                            if  data['feature_docs']['_id'] in rolesFeature:
                                if bitRA['view'] & rolesFeature[data['feature_docs']['_id']] - (data['feature_docs']['negasiperm'][str(self.authority)] & rolesFeature[data['feature_docs']['_id']]):
                                    objMenu = {
                                        'id':data['feature_docs']['feature_name'],
                                        'menuid':data['id'],
                                        'value':data['value'],
                                        'details':data['details'],
                                        'icon':f"mdi mdi-{data['icon']}",
                                        'parent':data['parent'],
                                        'sortnumber':data['sortnumber']
                                    }
                                    if data['feature_docs']['feature_name'] == "_organization":
                                        if self.authority & 1:
                                            objMenu["value"] = "Partner"
                                            objMenu["details"] = "Partner"
                                        elif self.authority & 2:
                                            objMenu["value"] = "Client"
                                            objMenu["details"] = "Client"
                                        elif self.authority & 4:
                                            objMenu["value"] = "Customer"
                                            objMenu["details"] = "Customer"
                                    resultsMenu.append(objMenu)

                                    if data['parent'] != '':
                                        resultsParentID[data['parent']]=1

                for parentdata in resultsParent:
                    if resultsParentID.get(parentdata['id']) != None:
                        resultsMenu.append(parentdata)                

                return sorted(resultsMenu, key=itemgetter('sortnumber'))
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
