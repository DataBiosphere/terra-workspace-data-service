# Running python tests locally

This page provides instructions for running the wds_client python tests from your dev machine.

These tests are normally run from the "Build, test and publish Python client to PyPI" GitHub Action
at [release-python-client.yml](https://github.com/DataBiosphere/terra-workspace-data-service/actions/workflows/release-python-client.yml).

You may need to run tests locally to replicate and debug failures from the GitHub Action.

## Prerequisites

_You should only need to install prerequisites once._ Python tests require:

* Python 3.11 or later
* openapi-generator 7.6.0
* pytest
* optionally, a python virtual environment

WDS defaults to python 3.11, but should work with earlier versions. To check the version of python
you have installed, run the following:

```bash
python --version
```

Install openapi-generator-cli:

```bash
npm install @openapitools/openapi-generator-cli -g
```

Configure openapi-generator-cli to use the same version as the GitHub Action.
Depending on your user permissions, sudo may be required to run these commands.

```bash
openapi-generator-cli version-manager set 7.6.0
```

At times, openapi-generator-cli won't set the right version even if the command above is run, so it
is recommended to verify. Sometimes the proper version needs to be explicitly downloaded before it
can be set. To see all available commands:

```bash
openapi-generator-cli
```

Run this command to see what versions are downloaded, if 7.6.0 is not, use arrow keys to navigate to
it and download it (or if already downloaded to use it).

```bash
openapi-generator-cli version-manager list
```

Optional, but recommended: create and activate a python virtual environment:

```bash
python -m venv venv
source venv/bin/activate
```

To run tests, WDS uses pytest. Ensure that is installed by running:

```bash
pip install pytest
```

## Set up

_You must make sure these are running every time you run tests:_

* Postgres
* nginx mock server
* WDS

These steps are covered in detail in WDS's main [README](../README.md#setup), and that README is the
source of truth. The steps are copied here for ease of copy/paste.

Start Postgres:

```bash
./local-dev/run_postgres.sh start
```

Start the nginx mock server:

```bash
# start the server as a docker container in detached mode
docker run -v `pwd`/service/src/test/resources/nginx.conf:/etc/nginx/nginx.conf -v `pwd`/service/src/test/resources:/usr/share/nginx/html -p 9889:80 -d nginx:1.23.3
```

Start WDS pointing at the mock server:

```bash
SAM_URL=http://localhost:9889 \
WORKSPACE_MANAGER_URL=http://localhost:9889 \
./gradlew bootRun \
--args='--twds.data-import.require-validation=false'
```

## Build wds_client locally

Optionally, activate your python virtual environment:

```bash
source venv/bin/activate
```

Once you confirm that you have python and openapitools, you will need to build and create a local
version of the wds_client package (make sure you are in the right branch that has the changes you
want to test). Note that you will need to re-generate the client for each code change you make. To
do that, run the following command (from the root of your repo or adjust path accordingly). If this
command is generating errors, it is likely because the openapi is set to the wrong version. Note
that package version is hard coded since it is not important to track locally - adjust as you see
fit if you want to the version to change when you install the package locally.

```bash
  openapi-generator-cli generate \
  -i service/src/main/resources/static/swagger/openapi-docs.yaml \
  -g python \
  -o wds-client \
  --additional-properties=projectName=wds-client,packageName=wds_client,packageVersion=0.0.1
```

This will generate a folder called `wds-client` containing the generated client. To install from
that folder:

```bash
pip install wds-client
```

That should use your local python installation to install the wds_client locally. Once complete with
no errors, you should be able to open a python shell and run "import wds_client" with no error.
However, if you are still not able to do that, it is possible that your pip is tied to a different
version of python installed on your machine that is not the default. There are a few ways to solve
this problem but the easiest way to go about this is to specify a python version to run pip,
something like this:

```
python3 -m pip install wds-client
```

## Running tests

Once you have the wds_client package importing locally with no issues, you are ready to run the
tests! Run the following command from the repo root (or adjust path accordingly):

```
pytest service/src/test/python/test.py
```

Note: Make sure your local WDS database does not have data in it before running these tests, as its
presence may cause the tests to fail.

To debug tests, you may want to try out
python's [pdb debugger](https://realpython.com/python-debugging-pdb/).

In case you get frustrated with python, go [here](https://xkcd.com/1987/) for a quick laugh.
