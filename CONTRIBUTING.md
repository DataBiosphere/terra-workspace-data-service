## Contributing to Workspace Data Service

Please see the [README](README.md) for instructions on how to set up the repo for development and testing.

## Coding Style
### Exceptions
Custom exception classes should be located in `service/src/main/java/org.databiosphere.workspacedataservice/service/model/exception`, extend `RuntimeException` and should use the `@ResponseStatus` annotation. 
See [`MissingReferencedTableException`](https://github.com/DataBiosphere/terra-workspace-data-service/blob/main/service/src/main/java/org/databiosphere/workspacedataservice/service/model/exception/MissingReferencedTableException.java) for an example. 
Use custom exception classes wherever possible; if it is necessary to throw a generic exception from the controller, use a `ResponseStatusException` with appropriate HTTPStatus and a clear message.

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
