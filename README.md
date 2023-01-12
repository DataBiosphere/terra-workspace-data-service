# Workspace Data Service
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=DataBiosphere_workspace-data-service&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=DataBiosphere_workspace-data-service)

Workspace Data Service provides an API to manage records in Terra.

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

We are using Postgres 11.17.  You do not need to have postgres installed on your system.
Instead use the `run_postgres.sh` script to set up a docker container running postgres (see below).

## Running
### Pulling local secrets
Before running WDS, you must populate secrets from Vault.
After [setting up your vault-token](https://docs.google.com/document/d/11pZE-GqeZFeSOG0UpGg_xyTDQpgBRfr0MLxpxvvQgEw) run:
```bash
./scripts/render-config.sh [wds env] [vault env]
```
Here `wds env` may be `local`,`dev`,`alpha`,`perf`, or `prod`
`vault env` can be `docker` or `local`.

To just build the code, from the root directory run
```bash
./gradlew build
```
To run the application, first a postgres database must be running:
```bash
./local-dev/run_postgres.sh start
```

To run WDS locally, you can either use the command line:
```bash
./gradlew bootRun
```
Or, from Intellij, go to `src/main/java/org.databiosphere.workspacedataservice/WorkspaceDataServiceApplication` and click the green play button.
This will launch the service on port 8080.

At the moment, WDS is only available through this port.  It can be reached from the command line:

To query for a single record:
```bash
curl http://localhost:8080/<instanceid>/records/v0.2/<table/type>/<record_id>
```

To add new attribute or update values for existing attributes (this won't create a new record however):
``` bash
curl -H "Content-type: application/json" -X PATCH "http://localhost:8080/<instanceid guid>/records/v0.2/<table/type>/<record_id>" -d '{
"id": "<record_name>",
"type": "<record_type>",
"attributes": {
"new-int": -77,
"new-date-time": "2011-01-11T11:00:50",
"new-double": -122.45,
"new-json": "{\"key\": \"Richie\"}"
}
}'
```
Note that attribute and record type names are subject to the logic in [validateName](https://github.com/DataBiosphere/terra-workspace-data-service/blob/5375c8c846ad163e38a5540c4487ea05fb292f45/service/src/main/java/org/databiosphere/workspacedataservice/dao/RecordDao.java#L80)

When done, stop postgres:
```bash
./local-dev/run_postgres.sh stop
```

## Tests
To run unit tests locally, first make sure your local postgres is up and running:
```bash
./local-dev/run_postgres.sh start
```
From the command line, run
```bash
./gradlew test
```

## Swagger UI
When running locally, a Swagger UI is available at http://localhost:8080/swagger/swagger-ui.html.

You can also view the swagger definitions via this third-party hosted UI: https://petstore.swagger.io/?url=https://raw.githubusercontent.com/DataBiosphere/terra-workspace-data-service/main/service/src/main/resources/static/swagger/openapi-docs.yaml.
Note that in this third-party UI, the APIs are not active; this is only valuable for viewing the definitions.

## Docker
To build the WDS docker image _(replace "wdsdocker" with your desired image name)_:
```bash
./gradlew --build-cache jibDockerBuild --image=wdsdocker -Djib.console=plain
```

To run the built docker image (_replace "wdsdocker" the image name specified during build_):
* Docker for Mac:
```bash
docker run -e WDS_DB_HOST='host.docker.internal' -p 8080:8080 wdsdocker
```
* Docker on Linux:
```bash
docker run --network=host -e WDS_DB_HOST='127.0.0.1' -p 8080:8080 wdsdocker
```


## Using Workspace Data Service
A client of WDS is published in the Broad Artifactory.  To include it in your Gradle project, add the following to your `build.gradle` file:
```
repositories {
    maven {
        url "https://broadinstitute.jfrog.io/broadinstitute/libs-snapshot-local/"
    }
}

dependencies {
    implementation(group: 'org.databiosphere', name: 'workspacedataservice-client', version: 'x.x.x')
```
The latest version can be seen under [Tags](https://github.com/DataBiosphere/terra-workspace-data-service/tags).
