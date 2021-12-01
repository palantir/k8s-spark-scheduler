FROM alpine:3.7

ENV SERVICE_HOME=/opt/palantir/services/resource-reservation-conversion-webhook
ENV SPARK_SCHEDULER_STARTUP_CONFIG=$SERVICE_HOME/var/conf/install.yml

RUN adduser -S -u 5001 resource-reservation-conversion-webhook && \
    mkdir -p $SERVICE_HOME && \
    mkdir -p $SERVICE_HOME/service/bin && \
    mkdir -p $SERVICE_HOME/var/conf && \
    chown -R 5001:5001 $SERVICE_HOME

COPY --chown=5001:5001 var/conf/install.yml $SERVICE_HOME/var/conf/install.yml
COPY --chown=5001:5001 {{InputBuildArtifact Product "linux-amd64"}} $SERVICE_HOME/service/bin/resource-reservation-conversion-webhook

# Expose service port
EXPOSE 8485
# Expose management port
EXPOSE 8486

WORKDIR $SERVICE_HOME

USER 5001
ENTRYPOINT ["service/bin/resource-reservation-conversion-webhook"]
CMD ["server"]
