
import sys
import os
import json
import requests
import io
import uuid
import random
import string
import wds_client
import requests
import time

workspace_manager_url = ""
rawls_url = ""
leo_url = ""

def setup(bee_name):
# define major service endpoints based on bee name
    global workspace_manager_url
    workspace_manager_url = f"https://workspace.{bee_name}.bee.envs-terra.bio"
    global rawls_url
    rawls_url = f"https://rawls.{bee_name}.bee.envs-terra.bio"
    global leo_url
    leo_url = f"https://leonardo.{bee_name}.bee.envs-terra.bio"
    return workspace_manager_url, rawls_url, leo_url

# CREATE WORKSPACE ACTION
def create_workspace(cbas, billing_project_name, header):
    # create a new workspace, need to have attributes or api call doesnt work
    api_call2 = f"{rawls_url}/api/workspaces";
    request_body= {
      "namespace": billing_project_name, # Billing project name
      "name": f"api-workspace-{''.join(random.choices(string.ascii_lowercase, k=5))}", # workspace name
      "attributes": {}};
    
    response = requests.post(url=api_call2, json=request_body, headers=header)
    
    #example json that is returned by request: 'attributes': {}, 'authorizationDomain': [], 'bucketName': '', 'createdBy': 'yulialovesterra@gmail.com', 'createdDate': '2023-08-03T20:10:59.116Z', 'googleProject': '', 'isLocked': False, 'lastModified': '2023-08-03T20:10:59.116Z', 'name': 'api-workspace-1', 'namespace': 'yuliadub-test2', 'workspaceId': 'ac466322-2325-4f57-895d-fdd6c3f8c7ad', 'workspaceType': 'mc', 'workspaceVersion': 'v2'}
    json2 = response.json()
    print(json2)
    data = json.loads(json.dumps(json2))
    
    print(data['workspaceId'])
    
    # enable CBAS if specified
    if cbas is True:
        print(f"Enabling CBAS for workspace {data['workspaceId']}")
        api_call3 = f"{leo_url}/api/apps/v2/{data['workspaceId']}/terra-app-{str(uuid.uuid4())}";
        request_body2 = {
          "appType": "CROMWELL"
        } 
        
        response = requests.post(url=api_call3, json=request_body2, headers=header)
        # will return 202 or error
        print(response)

    return data['workspaceId']

# GET WDS OR CROMWELL ENDPOINT URL FROM LEO
def get_app_url(workspaceId, app, azure_token):
    """"Get url for wds/cbas."""
    uri = f"{leo_url}/api/apps/v2/{workspaceId}?includeDeleted=false"

    headers = {"Authorization": azure_token,
               "accept": "application/json"}

    response = requests.get(uri, headers=headers)
    status_code = response.status_code

    if status_code != 200:
        return response.text
    print(f"Successfully retrieved details.")
    response = json.loads(response.text)

    app_url = ""
    app_type = "CROMWELL" if app != 'wds' else app.upper();
    print(f"App type: {app_type}")
    for entries in response: 
        if entries['appType'] == app_type and entries['proxyUrls'][app] is not None:
            print(entries['status'])
            if(entries['status'] == "PROVISIONING"):
                print(f"{app} is still provisioning")
                break
            print(f"App status: {entries['status']}")
            app_url = entries['proxyUrls'][app]
            break 

    if app_url is None: 
        print(f"{app} is missing in current workspace")
    else:
        print(f"{app} url: {app_url}")

    return app_url

# UPLOAD DATA TO WORSPACE DATA SERVICE IN A WORKSPACE
def upload_wds_data(wds_url, current_workspaceId, tsv_file_name, recordName, azure_token):
    version="v0.2"
    api_client = wds_client.ApiClient(header_name='Authorization', header_value=azure_token)
    api_client.configuration.host = wds_url
    
    # records client is used to interact with Records in the data table
    records_client = wds_client.RecordsApi(api_client)
    # data to upload to wds table
    
    # Upload entity to workspace data table with name "testType_uploaded"
    response = records_client.upload_tsv(current_workspaceId, version, recordName, tsv_file_name)
    print(response)

# KICK OFF A WORKFLOW INSIDE A WORKSPACE
def submit_workflow_assemble_refbased(workspaceId, dataFile, azure_token):
    cbas_url = get_app_url(workspaceId, "cbas", azure_token)
    print(cbas_url)
    #open text file in read mode
    text_file = open(dataFile, "r")
    request_body2 = text_file.read();
    text_file.close()
    
    uri = f"{cbas_url}/api/batch/v1/run_sets"
    
    headers = {"Authorization": azure_token,
               "accept": "application/json", 
              "Content-Type": "application/json"}
    
    response = requests.post(uri, data=request_body2, headers=headers)
    # example of what it returns: {'run_set_id': 'cdcdc570-f6f3-4425-9404-4d70cd74ce2a', 'runs': [{'run_id': '0a72f308-4931-436b-bbfe-55856f7c1a39', 'state': 'UNKNOWN', 'errors': 'null'}, {'run_id': 'eb400221-efd7-4e1a-90c9-952f32a10b60', 'state': 'UNKNOWN', 'errors': 'null'}], 'state': 'RUNNING'}
    print(response.json())


def delete_workspace():
    # todo
    return ""