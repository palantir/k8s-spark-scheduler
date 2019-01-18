FROM alpine:3.7

ENV SERVICE_HOME=/opt/palantir/services/spark-scheduler
ENV SPARK_SCHEDULER_STARTUP_CONFIG=$SERVICE_HOME/var/conf/install.yml

RUN mkdir -p $SERVICE_HOME && mkdir -p $SERVICE_HOME/service/bin && mkdir -p $SERVICE_HOME/var/conf

COPY var/conf/install.yml $SERVICE_HOME/var/conf/install.yml
COPY {{InputBuildArtifact Product "linux-amd64"}} $SERVICE_HOME/service/bin/spark-scheduler

# Expose service port
EXPOSE 8483
# Expose management port
EXPOSE 8484

WORKDIR $SERVICE_HOME

ENTRYPOINT ["service/bin/spark-scheduler"]
CMD ["server"]
