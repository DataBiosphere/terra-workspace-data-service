Reminder:

#### Releasing WDS ####
PRs merged into main will not automatically generate a PR in https://github.com/broadinstitute/terra-helmfile to update the WDS image deployed to kubernetes - this action will need to be triggered manually by running the following github action: https://github.com/DataBiosphere/terra-workspace-data-service/actions/workflows/tag-and-publish.yml. Dont forget to provide a Jira Id when triggering the manual action, if no Jira ID is provided the action will not fully succeed. 

After you manually trigger the github action (and it completes with no errors), you must go to [the terra-helmfile](https://github.com/broadinstitute/terra-helmfile) repo and verify that this generated a PR that merged successfully.

The terra-helmfile PR merge will then generate a PR in [leonardo](https://github.com/DataBiosphere/leonardo).  This will automerge if all tests pass, but if jenkins tests fail it will not; be sure to watch it to ensure it merges. To trigger jenkins retest simply comment on PR with "jenkins retest". 

#### Keeping Docs Up To Date ####
If you make changes to the github actions or workflows, particularly when they are run or which other workflows they call, please update [the GHA wiki page](https://github.com/DataBiosphere/terra-workspace-data-service/wiki/GHA-structure-in-WDS) to stay up to date.
