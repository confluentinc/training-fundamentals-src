FROM confluentinc/cp-kafka-connect:7.1.1-1-ubi8
ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"
USER root
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-mqtt:latest
USER appuser