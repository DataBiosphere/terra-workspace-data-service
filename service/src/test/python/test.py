# test.py

from unittest import TestCase
import  wds_client
from datetime import date, datetime
import random
import uuid
import json
import csv

def generate_record():
    data = {}
    data['key']='value'
    json_data = json.dumps(data)
    dict_values = {"column_key": "SomeString" + str(random.random()), 
                   "ComplexStringTest": "I said \"hello\"", 
                   "complexStringTest2": ["foo","bar","\"baz\" is the best"], 
                   "ListTest":["green","red", "yellow"], 
                   "NumberTest": 1, 
                   "NumberTest2": -999, 
                   "NumberTest3": 3.14, 
                   "DateTimeTest": "2011-12-03T10:15:30.123456789", 
                   "DateTest": date(2020,5,17), 
                   "BoolTest1": True, 
                   "BoolTest2": "fAlse", 
                   "ArrayBoolTest":[True, False], 
                   "jsonTest": json_data, 
                   "MixTest": ["hello", 123, True] }
    return dict_values

def generate_record_with_relation(type, id):
    dict_values = {"column_key2": "SomeString" + str(random.random()), 
               "NumberTest3": -3.14, 
               "DataRelationTest": f"terra-wds:/{type}/{id}"}
    return dict_values

def generate_csv(numRecords):
    fieldnames=['id','name','age','city']

    with open('test.csv', 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter='\t', fieldnames=fieldnames)

        names=['foo', 'bar', 'foo_bar', 'foo-bar', 'Bar-Foo', 'BARFOO']
        cities=['Seattle', 'Redmond', 'Boston', 'New York']

        writer.writerow(dict(zip(fieldnames, fieldnames)))
        for i in range(0, numRecords):
          writer.writerow(dict([
            ('id', i),
            ('name', random.choice(names)),
            ('age', str(random.randint(24,26))),
            ('city', random.choice(cities))]))

class TryTesting(TestCase):
    api_client = wds_client.ApiClient()
    api_client.configuration.host = 'http://localhost:8080'
    version = "v0.2"
    records_client = wds_client.RecordsApi(api_client)
    generalInfo_client = wds_client.GeneralWDSInformationApi(api_client)
    schema_client = wds_client.SchemaApi(api_client)
    instance_client = wds_client.InstancesApi(api_client)
    current_workspaceId = instance_client.list_wds_instances(version)[0]

    testType1_simple ="s_record_1"
    testType1_complex ="c_record_1"
    testType1_relation ="r_record_1"

    testId1_simple = "s_id_1"
    testId1_complex = "c_id_1"
    testId1_relation = "r_id_1"

    testType2_complex ="c_record_2"
    testType2_relation ="r_record_2"


    def create_record_with_primary_key(self, record, record_type, record_id, key):
        record_request = wds_client.RecordRequest(attributes=record);
        recordCreated = self.records_client.create_or_replace_record(self.current_workspaceId, self.version, record_type, record_id, record_request, primary_key=key)
 
    def create_record(self, record, record_type, record_id):
        record_request = wds_client.RecordRequest(attributes=record);
        recordCreated = self.records_client.create_or_replace_record(self.current_workspaceId, self.version, record_type, record_id, record_request)

    def generate_two_records(self, type1, id1, type2, id2, key):
        recordComplex = generate_record();
        recordRelation = generate_record_with_relation(type1, id1); 
        self.create_record_with_primary_key(recordComplex, type1, id1, key)
        self.create_record(recordRelation, type2, id2)

    # tests start here
    def test_check_version(self):
        response = self.generalInfo_client.version_get()
        self.assertIsNotNone(response.build.version)

    def test_check_status(self):
        response = self.generalInfo_client.status_get()
        self.assertEqual(response.status, "UP")

    def test_simple_record_creation_query_and_delete(self):
        record = {"sys_name": "s_id_1", "column_key": "SomeString"};
        self.create_record(record, self.testType1_simple, self.testId1_simple)
        recordRetrieved = self.records_client.get_record(self.current_workspaceId, self.version, self.testType1_simple, self.testId1_simple)   
        # check that record that was saved is the same as what was retrieved
        self.assertTrue(record == recordRetrieved.attributes)

        # clean up
        response = self.records_client.delete_record(self.current_workspaceId, self.version, self.testType1_simple, self.testId1_simple);

    def test_complex_record_creation_query_and_delete(self):
        record = generate_record(); 
        self.create_record_with_primary_key(record, self.testType1_complex, self.testId1_complex, "testKey")
        recordRetrieved = self.records_client.get_record(self.current_workspaceId, self.version, self.testType1_complex, self.testId1_complex)   
        self.assertIs(record, recordRetrieved.attributes)
        response = self.records_client.delete_record(self.current_workspaceId, self.version, self.testType1_complex, self.testId1_complex);

    def test_relation_record_creation_query_and_delete(self):
        self.generate_two_records(self.testType2_complex, self.testId1_complex, self.testType2_relation, self.testId1_relation, "testKey_complex")
        recordRetrieved = self.records_client.get_record(self.current_workspaceId, self.version, self.testType2_relation, self.testId1_relation)   
        recordRetrieved = self.records_client.get_record(self.current_workspaceId, self.version, self.testType2_complex, self.testId1_complex)   
        workspace_ent_type = self.schema_client.describe_all_record_types(self.current_workspaceId, self.version)
        for t in workspace_ent_type:
            print ("name:", t.name ,"count:", t.count)
        response = self.records_client.delete_record(self.current_workspaceId, self.version, self.testType2_relation, self.testId1_relation);
        response = self.records_client.delete_record(self.current_workspaceId, self.version, self.testType2_complex, self.testId1_complex);

    def test_record_query_all(self):
        search_request = { "offset": 0, "limit": 10};
        record = self.records_client.query_records(self.current_workspaceId, self.version, self.testType2_complex, search_request)    
        print("HI" + str(record))

    def test_record_type_describe(self):
        ent_types = self.schema_client.describe_record_type(self.current_workspaceId, self.version, self.testType1_complex)
        print ("NAME:", ent_types.name ,"COUNT:", ent_types.count)

    def test_record_type_delete(self):
        response = self.schema_client.delete_record_type(self.current_workspaceId, self.version, self.testType1_simple);
        workspace_ent_type = self.schema_client.describe_all_record_types(self.current_workspaceId, self.version)
        for t in workspace_ent_type:
            print ("name:", t.name ,"count:", t.count)

    def test_upload_download_tsv(self):
        generate_csv(5000)
        record = self.records_client.upload_tsv(self.current_workspaceId, self.version, "TestUpload", "test.csv")
        print(record)
        
        # read tsv back into a variable from data table
        records = self.records_client.get_records_as_tsv(self.current_workspaceId, self.version, "TestUpload")
        print(records)