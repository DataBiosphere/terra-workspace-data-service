### This Dockerfile is used to extend the app-sec blessed Docker image for JRE-17
### We extend the Dockerfile within WDS to include a PostgreSQL client -- the client is primarily used
### to faciliate the cloning of a WDS instance/workspace in Terra

### Sourced from https://github.com/broadinstitute/dsp-appsec-blessed-images/blob/main/jre/Dockerfile.17-alpine
FROM us.gcr.io/broad-dsp-gcr-public/base/jre:17-debian

# Add postgres client for pg_dump command
RUN apt-get update && \
    apt-get install -y postgresql-client

# Temp storage location for pg_dump outputs on Azure backups
VOLUME /backup