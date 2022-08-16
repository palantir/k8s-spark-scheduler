# In the first stage, create our non-privileged user and directories
FROM alpine:3.16

ENV SERVICE_HOME=/opt/palantir/services/spark-scheduler

RUN adduser -S -u 5001 spark-scheduler && \
    mkdir -p $SERVICE_HOME && \
    mkdir -p $SERVICE_HOME/service/bin && \
    mkdir -p $SERVICE_HOME/var/conf && \
    chown -R 5001:5001 $SERVICE_HOME

FROM scratch

ENV SERVICE_HOME=/opt/palantir/services/spark-scheduler

# Copy the spark-scheduler user from the alpine stage
COPY --from=0 /etc/passwd /etc/passwd

# Copy the service home from the alpine stage
COPY --from=0 $SERVICE_HOME $SERVICE_HOME

ENV SPARK_SCHEDULER_STARTUP_CONFIG=$SERVICE_HOME/var/conf/install.yml

COPY --chown=5001:5001 var/conf/install.yml $SERVICE_HOME/var/conf/install.yml
COPY --chown=5001:5001 {{InputBuildArtifact Product "linux-amd64"}} $SERVICE_HOME/service/bin/spark-scheduler

# Expose service port
EXPOSE 8483
# Expose management port
EXPOSE 8484

WORKDIR $SERVICE_HOME

USER 5001
ENTRYPOINT ["service/bin/spark-scheduler"]
CMD ["server"]
