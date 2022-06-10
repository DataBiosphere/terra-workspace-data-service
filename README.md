# Workspace Data Service

Workspace Data Service provides an API to manage entities in Terra.

Design docs and other references may be found [here](https://broadworkbench.atlassian.net/wiki/spaces/AS/pages/2437742606/Workspace+Data+Service).

## Setup
### Prerequisites
//TODO: Secrets

Make sure you have Java 17 installed.  
We find [jenv](https://www.jenv.be/) helpful for managing active versions.  On a Mac,jenv can be installed and used like so:
```
brew install jenv
# follow postinstall instructions to activate jenv...

# to add previously installed versions of Java to jEnv, list them:
# /usr/libexec/java_home -V
# and then add them:
# jenv add /Library/Java/JavaVirtualMachines/<JAVA VERSION HERE>/Contents/Home

brew install homebrew/cask-versions/temurin17

jenv add /Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home
```

We are using Postgres 13.1.  You do not need to have postgres installed on your system.  
Instead use the `run_postgres.sh` script to set up a docker container running postgres (see below).

## Running
To build the code, from the root directory run
```
./gradlew build
```
A postgres database must be running for the application to work. To run a local postgres, use:
```
./local-dev/run_postgres.sh start
```

To run WDS locally, you can either use the command line:
```
./gradlew bootRun
```
Or, from Intellij, run `WorkspaceDataServiceApplication`.

When done, stop postgres:
```
./local-dev/run_postgres.sh stop
```
## Tests
//TODO: Tests and testing!