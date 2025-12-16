FROM apache/airflow:2.8.1

USER root

# Install Java 17 (available in Debian bookworm)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir \
    pyspark==3.4.1 \
    kagglehub \
    psycopg2-binary 
