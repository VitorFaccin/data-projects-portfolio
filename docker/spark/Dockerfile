FROM apache/spark:3.5.1

USER root

# Install pip so we can install the project package.
# The Spark driver runs in this container (deploy-mode=cluster), so it needs
# to be able to import data_projects_portfolio.
RUN apt-get update && apt-get install -y --no-install-recommends python3-pip \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Pre-bake Delta Lake and Hadoop-AWS JARs so the driver can read/write Delta on MinIO (S3A).
# Version matrix: Spark 3.5 → Delta 3.1.0 → Hadoop-AWS 3.3.4 → AWS SDK 1.12.262
COPY docker/spark/jars/delta-spark_2.12-3.1.0.jar /opt/spark/jars/delta-spark_2.12-3.1.0.jar
COPY docker/spark/jars/delta-storage-3.1.0.jar /opt/spark/jars/delta-storage-3.1.0.jar
COPY docker/spark/jars/hadoop-aws-3.3.4.jar /opt/spark/jars/hadoop-aws-3.3.4.jar
COPY docker/spark/jars/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

# Install the project package so the driver process can import data_projects_portfolio.
# src/ is also mounted as a volume at runtime for live code changes without rebuilding.
COPY pyproject.toml /opt/spark/work/pyproject.toml
COPY src/ /opt/spark/work/src/
RUN pip3 install --no-cache-dir -e /opt/spark/work/

USER spark
