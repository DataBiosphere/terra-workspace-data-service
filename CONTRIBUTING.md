## Contributing to Workspace Data Service

Please see the [README](README.md) for instructions on how to set up the repo for development and testing.
Be sure to check the [wiki](https://github.com/DataBiosphere/terra-workspace-data-service/wiki) for coding style guidelines and other useful info.

## IDE Configuration

IntelliJ and VSCode can both be configured to automatically apply our coding style guidelines.

Intellij:
1. Note: [.editorconfig](.editorconfig) is used to configure some basic formatting rules
1. Install the [google-java-format plugin](https://plugins.jetbrains.com/plugin/8527-google-java-format) and enable it for the project.
1. To enable formatting on save: go to *Settings* -> *Tools* -> *Actions on Save* and choose _Enable Reformat Code_, _Optimize Imports_ and _Run code cleanup_
1. (Optional): Intellij won't automatically run the formatter unless there is an actual code change.  To make the formatter run on save even if there are no changes, we suggest you create a macro bound to the keystroke you prefer.  See: [IntelliJ Reformat on File Save](https://stackoverflow.com/questions/946993/intellij-reformat-on-file-save).

VSCode:
1. VSCode will work out of the box and is configured by [.vscode/settings.json](.vscode/settings.json).  The formatter will run automatically whenever you save a file.

## (Optional) Install pre-commit hooks
1. [scripts/git-hooks/pre-commit] has been provided to help ensure all submitted changes are formatted correctly.  To install all hooks in [scripts/git-hooks], run:
```bash
git config core.hooksPath scripts/git-hooks
```

## Ignoring code formatting changes from `git blame`
The style guide formatting changes can make it difficult to use `git blame` to find the original author of a line of code after https://github.com/DataBiosphere/terra-workspace-data-service/commit/c795c46f2ce4bf30fce829979bcfc40ec12eef0c.  To ignore these (and any other similarly obfuscating) changes, run the following command to add the appropriate commits to the blame ignore list:
```bash
git config blame.ignoreRevsFile .git-blame-ignore-revs
```

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

1. Verify that the auto github action updated the WDS version in the [`terra-helmfile`](https://github.com/broadinstitute/terra-helmfile) repo. This action happens once a week on Monday morning EST. You can also manually trigger the github action to run if update needs to be published sooner.
2. After `terra-helmfile` publishes an updated WDS chart, another PR will auto create that will update the chart version in the [`leonardo`](https://github.com/DataBiosphere/leonardo) repo. That PR should auto merge but it would be a good idea to verify that it merged succesfully.
