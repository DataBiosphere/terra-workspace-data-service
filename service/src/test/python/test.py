# test.py

from unittest import TestCase
import  wds_client
from datetime import datetime

class TryTesting(TestCase):
    def initialize_client(self):
        api_client = wds_client.ApiClient()
        api_client.configuration.host = 'http://localhost:8080'

        # records client is used to interact with Records in the data table
        records_client = wds_client.RecordsApi(api_client)
        
        # general WDS info allows to grab info like version and status of the WDS service running in your workspce
        generalInfo_instance = wds_client.GeneralWDSInformationApi(api_client)
        
        # schema provides more information about the schema of a data table
        schema_instance = wds_client.SchemaApi(api_client)
        
        # instance allows to check how many instance of wds client are active in your workspace
        client_instance = wds_client.InstancesApi(api_client)
        
        return clients

    def generate_records():
        data = {}
        data['key']='value'
        json_data = json.dumps(data)
        dict_values = {"StringTest": "SomeString", "ListTest":["green","red", "yellow"], "NumberTest": 1, "DateTimeTest": datetime.date(2020,5,17), "ArrayBoolTest":[True, False], "jsonTest": json_data }
        return dict_values

    def create_records(records):
        record_request = wds_client.RecordRequest(attributes=dict_values);
        recordCreated = records_client.create_or_replace_record(current_workspaceId, version, 'testType_1', 'testRecord', record_request, primary_key="column_key")

    # tests start here
    def test_check_version(self):
        response = self.initialize_client().version_get()
        print(response.build.version)
        self.assertIsNotNone(response.build.version)

    def test_check_status(self):
        response = self.initialize_client().status_get()
        print(response.status)
        self.assertEqual(response.status, "UP")

    def test_instance(self):
        instances = client_instance.list_wds_instances(version)
        # response is a list
        if instances is None:
            # create an instance and set it to be workspace Id

    def test_record_creation(self):
        self.create_records();

    def test_record_deletion(self):
        response = schema_instance.delete_record_type(current_workspaceId, version, "testType2");

    def test_record_query(self):
        recordRetrieved = records_client.get_record(current_workspaceId, version, 'testType', 'testRecord')

    def test_record_query_all(self):
        record = records_client.query_records(current_workspaceId, version, 'testType', search_request)    

    def test_record_type_describe_all(self):
        workspace_ent_type = schema_instance.describe_all_record_types(current_workspaceId, version)
        for t in workspace_ent_type:
            print ("name:", t.name ,"count:", t.count)

    def test_record_type_describe(self):
        ent_types = schema_instance.describe_record_type(current_workspaceId, version, 'demographic')
        print ("name:", ent_types.name ,"count:", ent_types.count)

    def test_record_type_delete(self):
        response = schema_instance.delete_record_type(current_workspaceId, version, "testType2");

    def test_upload_download_tsv(self):
        # insert tsv into data table
        record = records_client.upload_tsv(current_workspaceId, version, "TestUpload", "random.tsv")

        # read tsv back into a variable from data table
        records = records_client.get_records_as_tsv(current_workspaceId, version, "TestUpload")