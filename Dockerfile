### This Dockerfile is used to extend the app-sec blessed Docker image for JRE-17
### We extend the Dockerfile within WDS to include a PostgreSQL client -- the client is primarily used
### to faciliate the cloning of a WDS instance/workspace in Terra

### Sourced from https://github.com/broadinstitute/dsp-appsec-blessed-images/blob/main/jre/Dockerfile.17-debian
FROM us.gcr.io/broad-dsp-gcr-public/base/jre:17-debian

# Add postgres client for pg_dump command
RUN apt-get update && apt-get dist-upgrade -y
RUN apt-get install lsb-core -y
RUN apt-get install gnupg2 ca-certificates wget -y
# use the latest postgres repository
RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
# install the postgres public key
RUN wget -qO- https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
# refresh the repository
RUN apt-get update
RUN apt-get install postgresql-client -y

# Temp storage location for pg_dump outputs on Azure backups
VOLUME /backup