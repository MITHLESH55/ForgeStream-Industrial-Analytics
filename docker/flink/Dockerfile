FROM flink:1.18-scala_2.12

# Install Python 3.10 and dependencies
USER root
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
      python3 \
      python3-pip \
      python3-dev \
      build-essential \
      curl \
      ca-certificates && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install PyFlink 1.18.1 and runtime dependencies
RUN pip3 install --no-cache-dir \
    apache-flink==1.18.1 \
    pydantic>=2.7.0 \
    pydantic-settings>=2.2.0 \
    numpy>=1.26.0 \
    python-dateutil>=2.9.0 \
    confluent-kafka>=2.4.0 \
    psycopg2-binary>=2.9.9 \
    sqlalchemy>=2.0.30

# Download Flink Kafka and JSON connector JARs into Flink lib folder
RUN curl -sS -o /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar && \
    curl -sS -o /opt/flink/lib/flink-json-1.18.1.jar \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/1.18.1/flink-json-1.18.1.jar

# Set PYTHONPATH so forgestream package is discoverable
ENV PYTHONPATH="/opt/forgestream:${PYTHONPATH}"

USER flink
WORKDIR /opt/forgestream
