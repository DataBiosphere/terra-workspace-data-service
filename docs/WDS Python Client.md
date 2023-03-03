# Workspace Data Service Python Client

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

You will need a user entity that has access to the WDS client you need to reach, you will have this access if you have access to the workspace where data that analysis is being run on is present. 

To authenticate in terminal, using azure cli with a user account 

```
az login --use-device-code
```

This will provide a link and a code that one can visit in a browser to log in. Use the same account to authenticate with that you would lob into azure to view the subscription of where your Terra billing is deployed to. If you are unsure if you have an account that has that permission, autenticate in the context of a notebook running inside of Terra. 

To authenticate inside a notebook in Terra, leveraging the managed identity the notebook is running under. 

```
!az login --identity --allow-no-subscriptions
cli_token = !az account get-access-token | jq .accessToken
azure_token = cli_token[0].replace('"', '')
```

### WDS Endpoint

WDS endpoint URL can be aquired from Leo APIs and should soon be available in a enviromental variable in the notebook VM. 

However, if for some reason it is not, or you are looking to write data into a different workspace data table that you also have access to (remember this will only work if the token you generated has access to that workspace), it can be aquired by calling the following code

Replae env with "dev" or "prod", based on where your workspace is running. Workspace Id can be located either on the Data tab by clinking "data table status" or from the following env variable in the notebook: os.environ['WORKSPACE_ID'] 
```
def get_wds_url(workspaceId):
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
response = get_wds_url(current_workspaceId)
# specifically grab the wds one
wds_url = response[0]['proxyUrls']['wds']
print(wds_url)
```

Once you have bearer token, you are all set to create the WDS Api Client. Do note that token will expire and will be renewed every 60 minutes. 
To call WDS, you will always need to provide a version and workspace Id. 

```
api_client = wds_client.ApiClient(header_name='Authorization', header_value="Bearer " + azure_token)
api_client.configuration.host = wds_url

version = "v0.2"
# if running outside of Terra notebook VM, will need to set this to the desired workspace Id 
workspaceId = os.environ['WORKSPACE_ID'];
```

## WDS Available Client APIs

## General WDS Info
General WDS Info client allows to check the version of WDS that is running in a given workspace. It can also check the status of WDS, as well as its subcomponents. 

```
generalInfo_instance = wds_client.GeneralWDSInformationApi(api_client)

# this returns a VersionResponse object, to get the build version: response.build.version
# you can also just simply print the whole response as an str: print(response)
response = generalInfo_instance.version_get()
print(response.build.version)

response = generalInfo_instance.status_get()
print(response.status)
```

### Records
RecordsApi is the main client that does all the interaction with the actual data in the Data Tables in WDS. To view full list of functions, navigate to: 

A few examples are provided below: 
- create_or_replace_record
- get_records_as_tsv
- upload_tsv


Example of adding a new record (create_or_replace_record):
```
records_client = wds_client.RecordsApi(api_client)
dict_values = {"Colors":["green","red", "blue"], "numberYay": 1}
record_request = wds_client.RecordRequest(attributes=dict_values);
recordCreated = records_client.create_or_replace_record(current_workspaceId, version, 'testType', 'testRecord', record_request)
```

Example of get_records_as_tsv and then placing those into a pandas dataframe:

```
records_client = wds_client.RecordsApi(api_client)
# query records that were just added
records = records_client.get_records_as_tsv(current_workspaceId, version, 'testType')
print(records)
testType = pd.read_csv(records, sep='\t')
testType.head()
```

Example of upload_tsv:


```
tsv_file_name = "TestType_uploaded".tsv";
testType = testType.to_csv(tsv_file_name, sep="\t", index = False)

# Upload entity to workspace data table
response = records_client.upload_tsv(current_workspaceId, version, upload_data_table_name, tsv_file_name)
print(response)
```

### Schema

Available schema api are: 
- delete_record_type
- describe_all_record_types
- describe_record_type

The code below assumes that the workspace has a Data Table with the name "testType" that contains data in it. The code to create the record type and add data to it was covered in Records section.

Example of describe_record_type:

```
schema_instance = wds_client.SchemaApi(api_client)

# get data specifics for a specific data set vs all
ent_types = schema_instance.describe_record_type(current_workspaceId, version, 'testType')
print ("name:", ent_types.name ,"count:", ent_types.count)
```

Example of describe_all_record_types:

```
schema_instance = wds_client.SchemaApi(api_client)

# get workspace data specifics, such as what records exist
workspace_ent_type = schema_instance.describe_all_record_types(current_workspaceId, version)
for t in workspace_ent_type:
    print ("name:", t.name ,"count:", t.count)
```

### Instances

Currently WDS only supports a singel WDS instance by workspace, however in the future there will be support for multiple insances per workspace and these functions will be helpful on checking the state of running WDS instances. 

Here is how to set up the instance client and get back Ids of all WDS instances running in the current workspace.
```
client_instance = wds_client.InstancesApi(api_client)
response = client_instance.list_wds_instances(version)
print(response)
```
