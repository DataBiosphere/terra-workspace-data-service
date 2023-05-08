## Contributing to Workspace Data Service

Please see the [README](README.md) for instructions on how to set up the repo for development and testing.
Be sure to check the [wiki](https://github.com/DataBiosphere/terra-workspace-data-service/wiki) for coding style guidelines and other useful info.

## Before You Open a Pull Request

You most likely want to do your work on a feature branch based on develop. There is no explicit naming convention; we usually use some combination of the JIRA issue number and something alluding to the work we're doing.
Make sure all unit tests pass, including those you wrote for any new functionality you added (you did write unit tests, didn't you?).

## Opening a Pull Request

When you open a pull request, add the JIRA issue number (e.g. `ABC-123`) to the PR title. This will make a reference from JIRA to the GitHub issue. Describe your changes, including instructions on how to reproduce the behavior and any screenshots if applicable.

By default, each merge to main will be considered a 'patch' and the version will update accordingly.  For larger version updates, include '#minor' or '#major' as appropriate in one of the comments to your PRs to increment the version.

## PR approval process

Your PR is ready to merge when all of the following things are true:

1. Two reviewers have thumbed (or otherwise approved) your PR
2. All tests pass

## PR deployment process

To get a PR into newly-created WDS apps, you must:

1. update the WDS version in the [`terra-helmfile`](https://github.com/broadinstitute/terra-helmfile) repo.
   Here is an example PR which illustrates a WDS version update: https://github.com/broadinstitute/terra-helmfile/pull/4126.
2. After `terra-helmfile` publishes an updated WDS chart, you must also update the chart version in the [`leonardo`](https://github.com/DataBiosphere/leonardo) repo.
   Here is an example PR which illustrates a chart update: https://github.com/DataBiosphere/leonardo/pull/3324
