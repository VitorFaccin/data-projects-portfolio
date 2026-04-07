FROM apache/spark:3.5.1

USER root

# Pre-bake Delta Lake and Hadoop-AWS JARs so Spark workers can read/write Delta on MinIO (S3A).
# Version matrix: Spark 3.5 → Delta 3.1.0 → Hadoop-AWS 3.3.4 → AWS SDK 1.12.262
#
# If you are behind a corporate VPN with SSL inspection, these curl commands will fail.
# In that case, download the JARs manually (links in the README) and place them in
# docker/spark/jars/, then replace the RUN block below with:
#   COPY jars/delta-spark_2.12-3.1.0.jar /opt/spark/jars/delta-spark_2.12-3.1.0.jar
#   COPY jars/delta-storage-3.1.0.jar /opt/spark/jars/delta-storage-3.1.0.jar
#   COPY jars/hadoop-aws-3.3.4.jar /opt/spark/jars/hadoop-aws-3.3.4.jar
#   COPY jars/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar
COPY jars/delta-spark_2.12-3.1.0.jar /opt/spark/jars/delta-spark_2.12-3.1.0.jar
COPY jars/delta-storage-3.1.0.jar /opt/spark/jars/delta-storage-3.1.0.jar
COPY jars/hadoop-aws-3.3.4.jar /opt/spark/jars/hadoop-aws-3.3.4.jar
COPY jars/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

USER spark
