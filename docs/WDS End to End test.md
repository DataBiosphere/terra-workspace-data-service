# Workspace Data Service End to End test

To continuously test basic WDS functionality and make sure it is working with no errors, there is an automated end to end test that runs twice a day on the latest WDS bits available in Leonardo. 

# How does the test work

The test gets kicked off via a github action that runs on a schedule. The github action is defined [**here**](https://github.com/DataBiosphere/terra-workspace-data-service/blob/main/.github/workflows/run-e2e-tests.yaml). 
You can see the github action runs [**here**](https://github.com/DataBiosphere/terra-workspace-data-service/actions/workflows/run-e2e-tests.yaml). 

That action actually kicks of a few other actions in other repos, mainly: 
- a github action to create a bee and connect a landing zone to it (using repo: [**terra-github-workflows**](https://github.com/broadinstitute/terra-github-workflows/actions/workflows/bee-create.yaml))
- a github action to run the e2e (defined in this repo: [**dsp-reusable-workflows**](https://github.com/broadinstitute/dsp-reusable-workflows/blob/main/e2e-test/wds_azure_e2etest.py))
- a github action to destroy the bee (not used at this time, also defined in terra-github-workflows)

# How to test e2e test if making changes

Adjust which branch github actions are pulling from. Most common scenario will be changing the branch you call for getting the e2e test, instead of pulling from main, you can pull directly from your own branch (as long as it was pushed into the repo)

Change branch in your branch in WDS to pull from a different e2e branch, adjusted [**here**](https://github.com/DataBiosphere/terra-workspace-data-service/blob/main/.github/workflows/run-e2e-tests.yaml#L79)
Then change the github action that actually runs the test that is defined in [**dsp-reusable-workflows**](https://github.com/broadinstitute/dsp-reusable-workflows/blob/main/.github/workflows/run-e2e-tests.yaml#L36)
After these changes, you can execute the e2e test from your branch by using the manual "Run workflow" trigger [**here**](https://github.com/DataBiosphere/terra-workspace-data-service/actions/workflows/run-e2e-tests.yaml), and selecting your branch vs main. 

# What does the test currently cover

- Workspace creation
- Polling for Wds app to start as expected
- Wds data upload
- Wds cloning
- Wds check to ensure cloning worked as expected
- Workspace deletion
