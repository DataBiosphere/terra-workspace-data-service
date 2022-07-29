## Contributing to Workspace Data Service

Please see the [README](README.md) for instructions on how to set up the repo for development and testing.

## Before You Open a Pull Request

You most likely want to do your work on a feature branch based on develop. There is no explicit naming convention; we usually use some combination of the JIRA issue number and something alluding to the work we're doing.
Make sure all unit tests pass, including those you wrote for any new functionality you added (you did write unit tests, didn't you?).  

## Opening a Pull Request

When you open a pull request, add the JIRA issue number (e.g. `ABC-123`) to the PR title. This will make a reference from JIRA to the GitHub issue. Describe your changes, including instructions on how to reproduce the behavior and any screenshots if applicable. 
In the comments for one of your PRs, include '#patch', '#minor', or '#major' to increment the version.

## PR approval process

Your PR is ready to merge when all of the following things are true:

1. Two reviewers have thumbed (or otherwise approved) your PR
2. All tests pass
