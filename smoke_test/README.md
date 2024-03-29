# cWDS Smoke Tests

These smoke tests provide a means for running a small set of tests against a live running cWDS
instance to validate that it is up and functional. These tests should verify more than the `/status`
endpoint and should additionally try to verify some basic functionality of cWDS.

These tests should run quickly (no longer than a few seconds), should be idempotent, and when
possible, should not make any changes to the state of the service or its data.

_These tests are valid for cWDS in the control plane. They have not been evaluated against WDS as an
app in the data plane._

## Quickstart

```
python smoke_test.py -h
```

## Requirements

Python 3.10.3 or higher

## Setup

**_All code examples throughout this README assume you are in the `smoke_test` directory!_**

Recommended, but optional, create and activate a Python virtual env

```
python -m venv venv
source venv/bin/activate
```

You will need to install required pip libraries:

```pip install -r requirements.txt```

## Run

The smoke tests have 2 different modes that they can run in: authenticated or unauthenticated. The
mode will be automatically selected based on the arguments you pass to `smoke_test.py`.

### To run the _unauthenticated_ smoke tests:

syntax: python smoke_test.py {CWDS_HOST}

example to run tests against dev:

```python smoke_test.py cwds.dsde-dev.broadinstitute.org```

### To run all (_authenticated_ and _unauthenticated_) smoke tests:

first, `gcloud auth login` as a registered user in the environment against which you are running
tests.

next, run tests.

syntax: python smoke_test.py {CWDS_HOST} $(gcloud auth print-access-token)

example to run tests against dev:

```python smoke_test.py cwds.dsde-dev.broadinstitute.org $(gcloud auth print-access-token)```

## Required and Optional Arguments

### CWDS_HOST

Required - Can be just a domain or a domain and port:

* `cwds.dsde-dev.broadinstitute.org`
* `cwds.dsde-dev.broadinstitute.org:443`

The protocol can also be added if you desire, however, most cWDS instances can and should use HTTPS
and this is the default if no protocol is specified:

* `http://cwds.dsde-dev.broadinstitute.org`
* `https://cwds.dsde-dev.broadinstitute.org`

### USER_TOKEN

Optional - A `gcloud` access token. If present, `smoke_test.py` will execute all unauthenticated
tests as well as all authenticated tests using the access token provided in this argument.

### Verbosity

Optional - You may control how much information is printed to `STDOUT` while running the smoke tests
by passing a verbosity argument to `smoke_test.py`. For example to print more information about the
tests being
run:

```python smoke_test.py -v 2 {CWDS_HOST}```
or
```python smoke_test.py -v 2 {CWDS_HOST} $(gcloud auth print-access-token)```
