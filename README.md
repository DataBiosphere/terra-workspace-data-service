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
Before running WDS, you must populate secrets from Vault.  To do this run
```
./scripts/render-config.sh [env] [execution env]
```
Here `env` may be `dev`
`execution env` can be `docker` or `local`.

To just build the code, from the root directory run
```
./gradlew build
```
To run the application, first a postgres database must be running:
```
./local-dev/run_postgres.sh start
```

To run WDS locally, you can either use the command line:
```
./gradlew bootRun
```
Or, from Intellij, go to `src/main/java/org.databiosphere.workspacedataservice/WorkspaceDataServiceApplication` and click the green play button.
This will run liquibase to create the schema and launch the service on port 8080.

At the moment, WDS is only available through this port.  It can be reached from the command line:

To create entities:
`curl -v -H "Content-type: application/json" -X POST "http://localhost:8080/api/workspaces/default/test/entities/batchUpsert" -d '[{"name": "first_entity", "entityType": "first_entity_type", "operations": [{"op": "AddUpdateAttribute", "attributeName": "col1", "addUpdateAttribute": "Spam"}]}]}]'`

All entities must belong to the workspace `default:test`.

To read entities: 
`curl http://localhost:8080/api/workspaces/default/test/entityQuery/first_entity_type`

To remove entities:
`curl -H "Content-type: application/json" "http://localhost:8080/api/workspaces/default/test/entities/delete" -d '[{"entityName": "first_entity", "entityType": "first_entity_type"}]'`

When done, stop postgres:
```
./local-dev/run_postgres.sh stop
```
## Tests
//TODO: Tests and testing!