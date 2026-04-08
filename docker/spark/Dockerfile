FROM apache/spark:3.5.1

USER root

# Pre-bake Delta Lake and Hadoop-AWS JARs so the driver can read/write Delta on MinIO (S3A).
# Version matrix: Spark 3.5 → Delta 3.1.0 → Hadoop-AWS 3.3.4 → AWS SDK 1.12.262
COPY docker/spark/jars/delta-spark_2.12-3.1.0.jar /opt/spark/jars/delta-spark_2.12-3.1.0.jar
COPY docker/spark/jars/delta-storage-3.1.0.jar /opt/spark/jars/delta-storage-3.1.0.jar
COPY docker/spark/jars/hadoop-aws-3.3.4.jar /opt/spark/jars/hadoop-aws-3.3.4.jar
COPY docker/spark/jars/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

# Make the project package importable in the Spark driver process.
# We set PYTHONPATH to src/ so Python can find data_projects_portfolio directly —
# no pip install needed. src/ is mounted as a volume at runtime, so code changes
# on the host are reflected immediately without rebuilding the image.
COPY src/ /opt/app/src/
ENV PYTHONPATH=/opt/app/src

USER spark
