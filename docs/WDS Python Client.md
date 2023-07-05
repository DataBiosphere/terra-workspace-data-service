# Workspace Data Service Python Client

Workspace Data Service currently is only supported for workflows that are created in an Azure billing project. 

## Set up

In terminal, simply run:
```
pip install wds-client
```
Or in jupyter notebook: 
```
!pip install wds-client
```

To upgrade since it will not happen automatically
```
pip install wds-client --upgrade
```

### Auth set up

The proper way to authenticate inside a notebook in Terra is to leverage the managed identity the notebook is running under. These commands will acquire a token from the managed identity that will be used by the python client to connect to WDS.

```
!az login --identity --allow-no-subscriptions
cli_token = !az account get-access-token | jq .accessToken
azure_token = cli_token[0].replace('"', '')
```

### WDS Endpoint

WDS endpoint URL can be acquired from [**Leonardo APIs**](https://github.com/DataBiosphere/leonardo) by calling the following code. The api will return a list of apps runing in a given workspace, WDS url will be present in its proxyUrls.wds value. Below is a code snippet to help get the proxyUrls.wds value from a Leonarado api response. 

Replace env with "dev" or "prod", based on where the workspace is running. Workspace Id can be located either on the Data tab by clicking "Data Table Status" or from the following env variable in the notebook: os.environ['WORKSPACE_ID'] 

```
def get_wds_url(workspaceId, env):
    """"Get url for wds."""
    
    uri = f"https://leonardo.dsde-{env}.broadinstitute.org/api/apps/v2/{workspaceId}?includeDeleted=false"
    
    headers = {"Authorization": "Bearer " + azure_token,
               "accept": "application/json"}
    
    response = requests.get(uri, headers=headers)
    status_code = response.status_code
    
    if status_code != 200:
        return response.text
    
    print(f"Successfully retrieved details.")

    return json.loads(response.text)

# the response will return all proxy urls for the workspace
response = get_wds_url(current_workspaceId, "dev")

for entries in response:
    if entries['appType'] == 'WDS' and entries['proxyUrls']['wds'] is not None:
            wds_url = entries['proxyUrls']['wds']
            break

if wds_url is None: 
    print("WDS is missing in current workspace")
else:
    print(wds_url)
```

Once you have bearer token and the WDS endpoint url, you are all set to create the WDS Api Client. Do note that token will expire and will be renewed every 60 minutes. 
To call WDS, you will always need to provide a version and workspace Id. 

```
import wds_client
api_client = wds_client.ApiClient(header_name='Authorization', header_value="Bearer " + azure_token)
api_client.configuration.host = wds_url

version = "v0.2"
# if running outside of Terra notebook VM, will need to set this to the desired workspace Id 
current_workspaceId = os.environ['WORKSPACE_ID'];
```

## WDS Available Client APIs

You can check out the swagger generated documentation for all available APIs by following instruction to generate it [**here**](#Generate-Full-Client-Documentation). Below find a few specific examples for the most common use cases. 

## General WDS Info
General WDS Info client checks the version and current status of WDS.

```
generalInfo_instance = wds_client.GeneralWDSInformationApi(api_client)

# this returns a VersionResponse object, to get the build version: response.build.version
# you can also just simply print the whole response as an str: print(response)
response = generalInfo_instance.version_get()
print(response.build.version)

# this returns the current status of WDS; if WDS is down it indicates where
# the underlying failure may be
response = generalInfo_instance.status_get()
print(response.status)
```

### Records
RecordsApi is the main client that does all the interaction with the actual data in the Data Tables in WDS. A record is single row inside a WDS Data Table.

A few examples for the following functions are provided below. 
- create_or_replace_record
- get_records_as_tsv
- upload_tsv

Example of adding a new record (create_or_replace_record): 
```
from datetime import datetime
records_client = wds_client.RecordsApi(api_client)
dict_values = {"Colors":["green","red", "blue"], "Number": 2023, "DateTimeCreatedAt": datetime.now()}
record_request = wds_client.RecordRequest(attributes=dict_values);
# this will create a record with table name "testType" and record row name "testRecord"
# if you dont provide the primary_key, the operation will complete and the primary key column will be called "sys_name" by default
# in this context:
# "TestType" is the name of the Data Table
# "testRecord" is the value that will be populated for the primary key for the row to be added (left most column in the Data Table)
# Primary key is the name of the column that defines the primary key for the given Data Table
recordCreated = records_client.create_or_replace_record(current_workspaceId, version, 'testType', 'testRecord', record_request,  primary_key="column_key")
```

Example of get_records_as_tsv and then placing those into a pandas data frame:

```
import pandas as pd
records_client = wds_client.RecordsApi(api_client)
# query records that were just added
records = records_client.get_records_as_tsv(current_workspaceId, version, 'testType')
print(records)
testType = pd.read_csv(records, sep='\t')
testType.head()
```

Example of upload_tsv:


```
tsv_file_name = "TestType_uploaded.tsv";
# this will create a tsv on the notebook VM or locally where this code is run
testType = testType.to_csv(tsv_file_name, sep="\t", index = False)

# Upload entity to workspace data table with name "testType_uploaded"
response = records_client.upload_tsv(current_workspaceId, version, "testType_uploaded", tsv_file_name)
print(response)
```

### Schema

Schema defines the column name and types inside a Data Table. The code below assumes that the workspace has a Data Table with the name "testType" that contains data in it. The code to create the record type and add data to it was covered in Records section.

Example of describe_record_type:

```
schema_instance = wds_client.SchemaApi(api_client)

# get data specifics for a specific data table
ent_types = schema_instance.describe_record_type(current_workspaceId, version, 'testType')
print ("name:", ent_types.name ,"count:", ent_types.count)
```

Example of describe_all_record_types:

```
schema_instance = wds_client.SchemaApi(api_client)

# get workspace data specifics, such as what tables exist
workspace_ent_type = schema_instance.describe_all_record_types(current_workspaceId, version)
for t in workspace_ent_type:
    print ("name:", t.name ,"count:", t.count)
```

### Instances

Currently the Terra UI only supports a single WDS instance per workspace, however in the future there will be support for multiple instances per workspace and these functions will be helpful on checking the state of running WDS instances. 

Here is how to set up the instance client and get back Ids of all WDS instances running in the current workspace.
```
client_instance = wds_client.InstancesApi(api_client)
response = client_instance.list_wds_instances(version)
print(response)
```

## Generate Full Client Documentation

Navigate to Swagger [**Editor**](https://editor.swagger.io/?_ga=2.235527304.809800039.1678223236-2085963831.1674688894) and then replace the contents in the editor with the WDS config that can be found [**here**](https://github.com/DataBiosphere/terra-workspace-data-service/blob/main/service/src/main/resources/static/swagger/openapi-docs.yaml). Next on the top left, click "Generate Client" and select "python". The website will have the browser download a zip file, unzip the file and open the README.md found in the root folder. This will provide the documentation of all functions/objects included in the client. 
