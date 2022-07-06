# Workspace Data Service
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=DataBiosphere_workspace-data-service&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=DataBiosphere_workspace-data-service)

Workspace Data Service provides an API to manage entities in Terra.

Design docs and other references may be found [here](https://broadworkbench.atlassian.net/wiki/spaces/AS/pages/2437742606/Workspace+Data+Service).

## Setup
### Prerequisites
Make sure you have Java 17 installed.  Our favorite way to do this is with [SDKMAN](https://sdkman.io/jdks).  Once it is installed , use `sdk list java` to see available versions, and `sdk install java 17.0.2-tem` to install, for example, the Temurin version of Java 17.

[jenv](https://www.jenv.be/) is also helpful for managing active versions.  On a Mac,jenv can be installed and used like so:
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
### Pulling local secrets
Before running WDS, you must populate secrets from Vault. 
After [setting up your vault-token](https://docs.google.com/document/d/11pZE-GqeZFeSOG0UpGg_xyTDQpgBRfr0MLxpxvvQgEw) run:
```
./scripts/render-config.sh [wds env] [vault env]
```
Here `wds env` may be `local`,`dev`,`alpha`,`perf`, or `prod`
`vault env` can be `docker` or `local`.

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

## Swagger UI
When running locally, a Swagger UI is available at http://localhost:8080/swagger/swagger-ui.html.
