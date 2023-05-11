Reminder:

PRs merged into main will no tautomatically generate a PR in https://github.com/broadinstitute/terra-helmfile to update the WDS image deployed to kubernetes - this action will need to be triggered manually by running the following github action: https://github.com/DataBiosphere/terra-workspace-data-service/actions/workflows/tag.yml. 
After your manually trigger the github action (and it completes with no errors), you must go to [this](https://github.com/broadinstitute/terra-helmfile) repo and verify that this generated a PR that merged succesfully.
The teraa-helmfile PR merge will then generate a PR in https://github.com/DataBiosphere/leonardo.  This may or may not automerge; be sure to watch it to ensure it merges.
