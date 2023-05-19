### This Dockerfile is used to extend the app-sec blessed Docker image for JRE-17
### We extend the Dockerfile within WDS to include a PostgreSQL client -- the client is primarily used
### to faciliate the cloning of a WDS instance/workspace in Terra

### Sourced from https://github.com/broadinstitute/dsp-appsec-blessed-images/blob/main/jre/Dockerfile.17-debian
FROM us.gcr.io/broad-dsp-gcr-public/base/jre:17-debian



# freshen up
RUN apt-get update
# install the prerequisites needed to use latest postgres repo and public key
RUN apt-get install gnupg2 ca-certificates wget -y
# use the latest postgres repository.
# This hardcodes "focal-pgdg" instead of "$(lsb_release -cs)-pgdg" to prevent installing lsb-core
# Note that if we change the underlying distro away from focal, this will fail
RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt focal-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
# install the postgres public key
RUN wget -qO- https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
# refresh the repository
RUN apt-get update
# Add postgres client for pg_dump command
RUN apt-get install postgresql-client -y
# remove prerequisites we no longer need
RUN apt-get remove wget gnupg2 -y

# Temp storage location for pg_dump outputs on Azure backups
VOLUME /backup
