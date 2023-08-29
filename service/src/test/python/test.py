# test.py
# testing WDS python client scenarios

from unittest import TestCase
import  wds_client
from datetime import date, datetime
import random
import csv

# generate records for testing
def generate_record():
    data = {}
    data['key']='value'
    dict_values = {"Column_key": "SomeString" + str(random.random()), 
                   "ComplexStringTest": "I said \"hello\"", 
                   "ComplexStringTest2": ["foo","bar","\"baz\" is the best"], 
                   "ListTest":["green","red", "yellow"], 
                   "NumberTest": 1, 
                   "NumberTest2": -999, 
                   "NumberTest3": 3.14, 
                   "DateTimeTest": "2011-12-03T10:15:30.123456", 
                   "DateTest": date(2020,5,17), 
                   "BoolTest1": True, 
                   "BoolTest2": "fAlse", 
                   "ArrayBoolTest":[True, False], 
                   "JsonTest": data, 
                   "MixTest": ["hello", 123, True] }
    return dict_values

# generate record with relation to another record
def generate_record_with_relation(type, id):
    dict_values = {"Column_key2": "SomeString" + str(random.random()), 
               "NumberTest3": -3.14, 
               "DataRelationTest": f"terra-wds:/{type}/{id}"}
    return dict_values

def generate_csv(numRecords, fileName):
    # columns have to be in alphabetical order since WDS orders them as such when returned back from get_records_as_tsv call
    fieldnames=['id','age','city', 'name']

    with open(fileName, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, delimiter='\t', fieldnames=fieldnames)

        names=['foo', 'bar', 'foo_bar', 'foo-bar', 'Bar-Foo', 'BARFOO']
        cities=['Seattle', 'Redmond', 'Boston', 'New York']

        writer.writerow(dict(zip(fieldnames, fieldnames)))
        for i in range(0, numRecords):
          writer.writerow(dict([
            ('id', i),
            ('age', str(random.randint(24,26))),
            ('city', random.choice(cities)),
            ('name', random.choice(names))]))

def compare_csv(file1, file2):
    differences = []
    with open(file1, 'r') as csv1, open(file2, 'r') as csv2:  # Import CSV files
        import1 = csv1.readlines()
        import2 = csv2.readlines() 
        for row in import2:
            if row not in import1:
                differences.append(row)
    return differences

# change the generated record to ensure it matches the definition expected back from WDS response
def adjust_record_to_wds(record, key_name, key_value):
    # add key and reorder elements so the generated record would match what WDS would return
    record_updated = {}
    # become boolean
    if 'BoolTest2' in record:
        record['BoolTest2'] = False
    # all become strings
    if 'MixTest' in record: 
        record['MixTest'] = ["hello", '123', 'true']
    # can still be treated as datetime, but for comparison easier to change to string
    if 'DateTest' in record:
        record['DateTest']= '2020-05-17'
    
    record = sorted(record.items())

    # is no primary key - set to default
    if key_name is None:
        key_name = "sys_name"

    record_updated[key_name] = key_value
    record_updated.update(record)

    return record_updated

class WdsTests(TestCase):
    # set up clients for testing
    api_client = wds_client.ApiClient()
    api_client.configuration.host = 'http://localhost:8080'
    version = "v0.2"
    records_client = wds_client.RecordsApi(api_client)
    generalInfo_client = wds_client.GeneralWDSInformationApi(api_client)
    schema_client = wds_client.SchemaApi(api_client)
    instance_client = wds_client.InstancesApi(api_client)
    snapshot_client = wds_client.SnapshotsApi(api_client)
    current_workspaceId = instance_client.list_wds_instances(version)[0]

    testType1_simple ="s_record_1"
    testType1_complex ="c_record_1"
    testType1_relation ="r_record_1"

    testId1_simple = "s_id_1"
    testId1_complex = "c_id_1"
    testId1_relation = "r_id_1"

    testType2_complex ="c_record_2"
    testType2_relation ="r_record_2"

    cvsUpload_test = "TestUpload"
    generatedCvs_name = "generated_test.tsv"

    # creates a new record or replaces existing one with specified primary key
    def create_record_with_primary_key(self, record, record_type, record_id, key):
        record_request = wds_client.RecordRequest(attributes=record);
        recordCreated = self.records_client.create_or_replace_record(self.current_workspaceId, self.version, record_type, record_id, record_request, primary_key=key)
 
    # created a new records or replaces existing one with no primary key (default to sys_name)
    def create_record(self, record, record_type, record_id):
        record_request = wds_client.RecordRequest(attributes=record);
        recordCreated = self.records_client.create_or_replace_record(self.current_workspaceId, self.version, record_type, record_id, record_request)

    # generated and adds two separate records that are related to each other
    def generate_two_records(self, type1, id1, type2, id2, key):
        recordComplex = generate_record();
        recordRelation = generate_record_with_relation(type1, id1); 
        self.create_record_with_primary_key(recordComplex, type1, id1, key)
        self.create_record(recordRelation, type2, id2)
        return [recordComplex, recordRelation]

    # tests start here
    # SCENARIO 1
    # first with basic checks that WDS is up and running
    def test_check_version(self):
        response = self.generalInfo_client.version_get()
        self.assertIsNotNone(response.build.version)

    def test_check_status(self):
        response = self.generalInfo_client.status_get()
        self.assertEqual(response.status, "UP")

    # SCENARIO 2
    # create a simple record, retrieve it back and check that the result matches, then update and repeat
    def test_simple_record_creation_query1_and_delete(self):
        record = {"column_key": "SomeString"};
        self.create_record(record, self.testType1_simple, self.testId1_simple)
        recordRetrieved = self.records_client.get_record(self.current_workspaceId, self.version, self.testType1_simple, self.testId1_simple)   
        # check that record that was saved is the same as what was retrieved
        record_updated = adjust_record_to_wds(record, None, self.testId1_simple)
        self.assertTrue(record_updated == recordRetrieved.attributes)

        record["column_key"] = "AnotherString"; 
        record_request = wds_client.RecordRequest(attributes=record)
        response = self.records_client.update_record(self.current_workspaceId, self.version, self.testType1_simple, self.testId1_simple, record_request)
        recordRetrieved = self.records_client.get_record(self.current_workspaceId, self.version, self.testType1_simple, self.testId1_simple)   
        # check that record that was saved is the same as what was retrieved
        record_updated = adjust_record_to_wds(record, None, self.testId1_simple)
        self.assertTrue(record_updated == recordRetrieved.attributes)

        # clean up
        response = self.records_client.delete_record(self.current_workspaceId, self.version, self.testType1_simple, self.testId1_simple);
        response = self.schema_client.delete_record_type(self.current_workspaceId, self.version, self.testType1_simple);
        workspace_ent_type = self.schema_client.describe_all_record_types(self.current_workspaceId, self.version)
        self.assertTrue(len(workspace_ent_type) == 0)

    # SCENARIO 3
    # create a more complex record, get it back to make sure it matches, then describe the record type that was created
    def test_complex_record_creation_query_describe_and_delete(self):
        record = generate_record(); 
        self.create_record_with_primary_key(record, self.testType1_complex, self.testId1_complex, "testKey")
        recordRetrieved = self.records_client.get_record(self.current_workspaceId, self.version, self.testType1_complex, self.testId1_complex)  
        record_updated = adjust_record_to_wds(record, "testKey", self.testId1_complex)
        self.assertTrue(record_updated == recordRetrieved.attributes)
        
        ent_types = self.schema_client.describe_record_type(self.current_workspaceId, self.version, self.testType1_complex)
        #print ("NAME:", ent_types.name ,"COUNT:", ent_types.count)

        # clean up
        response = self.schema_client.delete_record_type(self.current_workspaceId, self.version, self.testType1_complex);
        workspace_ent_type = self.schema_client.describe_all_record_types(self.current_workspaceId, self.version)
        self.assertTrue(len(workspace_ent_type) == 0)

    # SCENARIO 4
    # create 2 record sets, query the records back, then check record types
    def test_relation_record_creation_queryall_and_delete(self):
        records = self.generate_two_records(self.testType2_complex, self.testId1_complex, self.testType2_relation, self.testId1_relation, "testKey_complex")
        search_request = { "offset": 0, "limit": 10, "sort": "DESC", "sortAttribute": "NumberTest3"}
        
        recordsRetrieved = self.records_client.query_records(self.current_workspaceId, self.version, self.testType2_complex, search_request)    
        record_updated = adjust_record_to_wds(records[0], "testKey_complex", self.testId1_complex)
        self.assertTrue(record_updated == recordsRetrieved.records[0].attributes)

        recordsRetrieved = self.records_client.query_records(self.current_workspaceId, self.version, self.testType2_relation, search_request)    
        record_updated = adjust_record_to_wds(records[1], None, self.testId1_relation)
        self.assertTrue(record_updated == recordsRetrieved.records[0].attributes)
        
        workspace_ent_type = self.schema_client.describe_all_record_types(self.current_workspaceId, self.version)
        #print(workspace_ent_type)
        for t in workspace_ent_type:
            if t.name == self.testType2_complex:
                self.assertTrue(t.count, 1)
                self.assertTrue(t.primary_key, "testKey_complex")
            if t.name == self.testType2_relation:
                self.assertTrue(t.count, 1)
                self.assertTrue(t.primary_key, "sys_name")

        # clean up
        response = self.records_client.delete_record(self.current_workspaceId, self.version, self.testType2_relation, self.testId1_relation);
        response = self.records_client.delete_record(self.current_workspaceId, self.version, self.testType2_complex, self.testId1_complex);  
        response = self.schema_client.delete_record_type(self.current_workspaceId, self.version, self.testType2_relation);
        response = self.schema_client.delete_record_type(self.current_workspaceId, self.version, self.testType2_complex);
        workspace_ent_type = self.schema_client.describe_all_record_types(self.current_workspaceId, self.version)
        self.assertTrue(len(workspace_ent_type) == 0)

    # SCENARIO 5
    # upload a larger file, then retrieve it back to check results
    def test_upload_download_tsv(self):
        num_records = 5000
        generate_csv(num_records, self.generatedCvs_name)
        record = self.records_client.upload_tsv(self.current_workspaceId, self.version, self.cvsUpload_test, self.generatedCvs_name)
        self.assertEqual(record.records_modified, num_records)
        
        ent_types = self.schema_client.describe_record_type(self.current_workspaceId, self.version, self.cvsUpload_test)
        self.assertEqual(ent_types.count, num_records)

        # read tsv back into a variable from data table and verify it matches what was originally created
        tsv_path = self.records_client.get_records_as_tsv(self.current_workspaceId, self.version, self.cvsUpload_test)
        diff = compare_csv(self.generatedCvs_name, tsv_path)
        self.assertTrue(len(diff) == 0)

        # clean up
        response = self.schema_client.delete_record_type(self.current_workspaceId, self.version, self.cvsUpload_test);
        workspace_ent_type = self.schema_client.describe_all_record_types(self.current_workspaceId, self.version)
        self.assertTrue(len(workspace_ent_type) == 0)

    # SCENARIO 6
    # import snapshot from TDR with appropriate permissions
    def test_import_snapshot(self):
        self.snapshot_client.import_snapshot(self.current_workspaceId, self.version, "123e4567-e89b-12d3-a456-426614174000")
        # should create a tdr-imports table
        ent_types = self.schema_client.describe_record_type(self.current_workspaceId, self.version, "tdr-imports")
        self.assertEqual(ent_types.count, 2)
        search_request = { "offset": 0, "limit": 10}
        records = self.records_client.query_records(self.current_workspaceId, self.version, "tdr-imports", search_request)
        testRecord = records.records[0]
        self.assertEqual(testRecord.id, "table1")
        self.assertEqual(testRecord.attributes['Snapshot Id'], "123e4567-e89b-12d3-a456-426614174000")
        self.assertIsNotNone(testRecord.attributes['Import Time'])
        # clean up
        response = self.schema_client.delete_record_type(self.current_workspaceId, self.version, "tdr-imports")
        workspace_ent_type = self.schema_client.describe_all_record_types(self.current_workspaceId, self.version)
        self.assertTrue(len(workspace_ent_type) == 0)
