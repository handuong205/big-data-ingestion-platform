# syntax=docker/dockerfile:1
# Spark 3.5.6 ships Hadoop 3.3.4, so hadoop-aws must be 3.3.4 and the AWS SDK
# bundle must be the one hadoop-aws 3.3.4 was built against (1.12.262).
FROM apache/spark:3.5.6-python3

ARG MAVEN=https://repo1.maven.org/maven2

USER root
ADD --chmod=644 ${MAVEN}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/spark/jars/
USER spark
