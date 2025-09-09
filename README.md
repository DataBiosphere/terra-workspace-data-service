# Workspace Data Service

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=DataBiosphere_workspace-data-service&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=DataBiosphere_workspace-data-service)

Workspace Data Service provides an API to manage records in Terra.

Design docs and other references may be
found [here](https://broadworkbench.atlassian.net/wiki/spaces/AS/pages/2437742606/Workspace+Data+Service).

## Setup

### Prerequisites

Make sure you have Java 17 installed. Our favorite way to do this is
with [SDKMAN](https://sdkman.io/jdks). Once it is installed , use `sdk list java` to see available
versions, and `sdk install java 17.0.2-tem` to install, for example, the Temurin version of Java 17.

[jenv](https://www.jenv.be/) is also helpful for managing active versions. On a Mac,jenv can be
installed and used like so:

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

We are using Postgres 14. You do not need to have postgres installed on your system.
Instead use the `run_postgres.sh` script to set up a docker container running postgres (see below).

#### Environment Variables

For WDS to work properly, several environment variables need to be set, and exactly one of the
following active profiles must be set:

- control-plane
- data-plane

To run WDS locally and therefore use the local application properties, set the profile as follows:

```bash
export SPRING_PROFILES_ACTIVE=local,control-plane
```

Other profiles that are available are:

- staging
- dev
- prod
- bee
- data-plane OR control-plane

You are unlikely to use prod and bee when running locally, those profiles are leveraged when WDS is
deployed in Terra.

If you would like to not use the local profile, you can set some of the environment variables
manually. The variables that need to be set are described below. You can also add them to your
`~/.zshrc` or similar shell profile. However, in order for the app to run correctly, you still
have to at least specify data-plane or control-plane.

##### Control Plane Setup

If you are running locally with the `control-plane` profile, you'll need some additional setup to
use PubSub.

1. [Set up application default credentials](https://cloud.google.com/docs/authentication/provide-credentials-adc).
   ```bash
   gcloud auth application-default login
   ```

2. Configure a Google Cloud Project for CWDS to use. This can be done by setting a default project
   with `gcloud` or by setting the `GOOGLE_CLOUD_PROJECT` environment variable.
   ```bash
   gcloud config set project broad-dsde-dev
   export GOOGLE_CLOUD_PROJECT=broad-dsde-dev
   ```

3. Configure PubSub topics by setting environment variables.
   ```bash
   # Topic for outgoing notifications to Rawls
   export RAWLS_NOTIFY_TOPIC=rawls-async-import-topic-dev
   # Topic for incoming notifications from Rawls
   export IMPORT_STATUS_UPDATES_TOPIC=cwds-import-job-status-updates-dev
   # Create a subscription dedicated to your local CWDS
   export IMPORT_STATUS_UPDATES_SUBSCRIPTION=cwds-import-job-status-updates-sub-local-$(whoami)
   ```

   This will send imports the dev Rawls; you may want to use a dummy project/topic instead.

4. Configure the Google Cloud Storage bucket to write Rawls JSON to.
   ```bash
   export SERVICE_GOOGLE_BUCKET=cwds-batchupsert-dev
   ```

##### SAM_URL

WDS contacts Sam for permission checks. You will need to configure Sam's URL by setting an
environment variable, such as:

```
export SAM_URL=https://sam.dsde-dev.broadinstitute.org/
```

##### WORKSPACE_ID

WDS requires a valid workspace id to check permissions in Sam and to import snapshots from the Terra
Data Repo.
This is controlled by a `WORKSPACE_ID` environment variable. You should set this to the UUID
of a workspace you own, e.g.

```
export WORKSPACE_ID=123e4567-e89b-12d3-a456-426614174000
```

## Running

To just build the code, from the root directory run

```bash
./gradlew build --exclude-task test
```

To run the application, first a postgres database must be running:

```bash
./local-dev/run_postgres.sh start
```

To bypass Sam permission checks locally, you can run a fake Sam nginx backend that will allow
everything. Run the following from WDS root folder to launch the docker container and export the
environment variable WDS will use on the next startup to connect to Sam.

```bash
# start the SAM mock as a docker container in detached mode
docker run -v `pwd`/service/src/test/resources/nginx.conf:/etc/nginx/nginx.conf -p 9889:80 -d nginx:1.23.3
export SAM_URL=http://localhost:9889
```

To run WDS locally, you can either use the command line:

```bash
./gradlew bootRun
```

Or, from Intellij, go
to `src/main/java/org.databiosphere.workspacedataservice/WorkspaceDataServiceApplication` and click
the green play button.
This will launch the service on port 8080.

At the moment, WDS is only available through this port. It can be reached from the command line:

To query for a single record:

```bash
curl http://localhost:8080/<instanceid>/records/v0.2/<table/type>/<record_id>
```

To add new attribute or update values for existing attributes (this won't create a new record
however):

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

Note that attribute and record type names are subject to the logic
in [validateName](https://github.com/DataBiosphere/terra-workspace-data-service/blob/5375c8c846ad163e38a5540c4487ea05fb292f45/service/src/main/java/org/databiosphere/workspacedataservice/dao/RecordDao.java#L80)

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

To run one test suite, run

```bash
./gradlew test --tests '*RecordDaoTest'
```

To run a single test, run

```bash
./gradlew test --tests '*RecordDaoTest.testGetSingleRecord'
```

## Troubleshooting

Some problems during build and test may be solved by running the Gradle `clean` task:

```bash
./gradlew clean
```

## Postgres

When running the local docker postgres, you can access the shell directly:

```bash
./local-dev/run_posgres.sh shell
```

## Swagger UI

When running locally, a Swagger UI is available at http://localhost:8080/swagger/swagger-ui.html.

You can also view the swagger definitions via this third-party hosted
UI: https://petstore.swagger.io/?url=https://raw.githubusercontent.com/DataBiosphere/terra-workspace-data-service/main/service/src/main/resources/static/swagger/openapi-docs.yaml.
Note that in this third-party UI, the APIs are not active; this is only valuable for viewing the
definitions.

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

A client of WDS is published in the Broad Artifactory. To include it in your Gradle project, add the
following to your `build.gradle` file:

```
repositories {
    maven {
        url "https://us-central1-maven.pkg.dev/dsp-artifact-registry/libs-snapshot/"
    }
}

dependencies {
    implementation(group: 'org.databiosphere', name: 'workspacedataservice-client', version: 'x.x.x')
```

The latest version can be seen
under [Tags](https://github.com/DataBiosphere/terra-workspace-data-service/tags).

## For Developers

### Code Style

We use follow the [Google Java style guide](https://google.github.io/styleguide/javaguide.html) and
implement it with a combination of the [spotless](https://github.com/diffplug/spotless) plugin and
IDE tooling.

To run spotless from the CLI:

```bash
./gradlew spotlessApply
```

For details on how to make the most of your IDE to apply the style guide automatically,
see [CONTRIBUTING.md](CONTRIBUTING.md).

### API Definitions and Code Generation

WDS's `v0.2` APIs are documented in `service/src/main/resources/static/swagger/openapi-docs.yaml`
and
their implementations are hand-coded.

APIs at `v1` or later use code generation. To create or update an API, you must:

1. Define the API using OpenAPI syntax
   at `service/src/main/resources/static/swagger/apis-v1.yaml`
2. Include the API, via `$ref:`, in `service/src/main/resources/static/swagger/openapi-docs.yaml`
3. Run the `generateApiInterfaces` Gradle task
4. Note the new/updated Java interfaces and models under the `o.d.w.generated` package
5. Create or update a controller class that implements the new Java interface

The `generateApiInterfaces` Gradle task is configured to run as a dependency of other
important Gradle tasks, such as `compileJava`. You should not have to run it manually
in the course of most development; however, if you are actively changing definitions for
an API you may need to run it to see your changes.
