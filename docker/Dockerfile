FROM alpine:3.7

ENV SERVICE_HOME=/opt/palantir/services/spark-scheduler
ENV SPARK_SCHEDULER_STARTUP_CONFIG=$SERVICE_HOME/var/conf/install.yml

RUN adduser -S -u 5001 spark-scheduler && \
    mkdir -p $SERVICE_HOME && \
    mkdir -p $SERVICE_HOME/service/bin && \
    mkdir -p $SERVICE_HOME/var/conf && \
    mkdir -p $SERVICE_HOME/var/keys && \
    chown -R 5001:5001 $SERVICE_HOME

COPY --chown=5001:5001 var/conf/install.yml $SERVICE_HOME/var/conf/install.yml
COPY --chown=5001:5001 var/conf/localcluster.crt $SERVICE_HOME/var/keys/localcluster.crt
COPY --chown=5001:5001 var/conf/localcluster.key $SERVICE_HOME/var/keys/localcluster.key
COPY --chown=5001:5001 var/conf/rootCACert.pem $SERVICE_HOME/var/keys/rootCACert.pem
COPY --chown=5001:5001 {{InputBuildArtifact Product "linux-amd64"}} $SERVICE_HOME/service/bin/spark-scheduler

# Expose service port
EXPOSE 8483
# Expose management port
EXPOSE 8484

WORKDIR $SERVICE_HOME

USER 5001
CMD GODEBUG="x509ignoreCN=0" service/bin/spark-scheduler server
