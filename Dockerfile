### This Dockerfile is used to extend the app-sec blessed Docker image for JRE-17
### We extend the Dockerfile within WDS to include a PostgreSQL client -- the client is primarily used
### to faciliate the cloning of a WDS instance/workspace in Terra

### Sourced from https://github.com/broadinstitute/dsp-appsec-blessed-images/blob/main/jre/Dockerfile.17-debian
FROM us.gcr.io/broad-dsp-gcr-public/sandbox/debian-test-wds

# refresh the repository
RUN apt-get update

# Add postgres client for pg_dump command
RUN apt-get install postgresql-client-14 -y
