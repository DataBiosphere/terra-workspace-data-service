
from helper import *
import os

# Setup configuration
# These values should be injected into the environment before setup
azure_token = os.environ.get("AZURE_TOKEN")
bee_name = os.environ.get("BEE_NAME")
billing_project_name = os.environ.get("BILLING_PROJECT_NAME")
# azure_token = ""
# bee_name = ""; # add bee name here
# billing_project_name = ""; # add billing account name here
number_of_workspaces = 1;
cbas=False;
wds_upload=True
cbas_submit_workflow=False
number_of_workflow_to_kick_off = 0


workspace_manager_url, rawls_url, leo_url = setup(bee_name)

 # WHERE E2E test actually begins
api_call1 = f"{workspace_manager_url}/api/workspaces/v1";
header = {"Authorization": "Bearer " + azure_token};

response = requests.get(url=api_call1, headers=header)
print(response)

workspaceIds = []

while number_of_workspaces != 0:
    # create the specified number os workspaces
    workspaceId = create_workspace(cbas, billing_project_name, header)
    workspaceIds.append(workspaceId)
    number_of_workspaces-=1


# track to see when the workspace WDS are ready to upload data into them 
# sleep for 5 minutes to allow WDS to start up, if no wds, only sleep 2 minutes to let cbas start up
if wds_upload:
    time.sleep(300)
elif cbas_submit_workflow:
    time.sleep(120) 
else:
    print("LOAD TEST COMPLETE.")

if wds_upload:
    for workspace in workspaceIds:
        print(f"trying to see wds is ready to upload stuff for workspace {workspace}")
        wds_url = get_app_url(workspace, "wds", azure_token)
        if wds_url == "":
            print(f"wds not ready or errored out for workspace {workspace}")
            continue
        upload_wds_data(wds_url, workspace, "resources/test.tsv", "test", azure_token)

if cbas_submit_workflow:
    # next trigger a workflow in each of the workspaces, at this time this doesnt monitor if this was succesful or not
    # upload file needed for workflow to run
    upload_wds_data(wds_url, workspace, "resources/sraloadtest.tsv", "sraloadtest", azure_token)
    submit_workflow_assemble_refbased(workspace, "resources/assemble_refbased.json", azure_token)

print("LOAD TEST COMPLETE.")