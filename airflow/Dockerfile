FROM apache/airflow:2.9.1

USER root

# Install Java 17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

RUN pip install --no-cache-dir \
    fastf1 \
    duckdb \
    dbt-duckdb \
    dbt-core \
    pyspark==3.5.0 \
    delta-spark==3.2.0 \
    setuptools