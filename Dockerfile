# https://github.com/docker-library/docs/blob/master/eclipse-temurin/README.md#creating-a-jre-using-jlink

########## start of code directly from https://github.com/broadinstitute/dsp-appsec-blessed-images/blob/main/jre/Dockerfile.17-alpine
FROM eclipse-temurin:17-jdk-alpine as jre-build

# Create a custom Java runtime
RUN apk add --no-cache binutils && \
    $JAVA_HOME/bin/jlink \
         --add-modules java.base \
         --strip-debug \
         --no-man-pages \
         --no-header-files \
         --compress=2 \
         --output /javaruntime

# Define your base image
FROM alpine

RUN apk --no-cache -U upgrade

ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY --from=jre-build /javaruntime $JAVA_HOME
########## end of code directly from https://github.com/broadinstitute/dsp-appsec-blessed-images/blob/main/jre/Dockerfile.17-alpine

# Add postgres client for pg_dump command
RUN apk add --no-cache postgresql-client

# Temp storage location for pg_dump outputs on Azure backups
VOLUME /backup