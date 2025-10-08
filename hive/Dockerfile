# syntax=docker/dockerfile:1
FROM apache/hive:4.0.0

LABEL maintainer="you@example.com"

ENV HIVE_HOME=/opt/hive
WORKDIR $HIVE_HOME

# Versions
ENV POSTGRES_VERSION=42.7.5
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.367

USER root
# Install curl and clean up
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
        procps \
        && rm -rf /var/lib/apt/lists/*

# Download required JARs
RUN curl -L https://jdbc.postgresql.org/download/postgresql-${POSTGRES_VERSION}.jar \
                -o $HIVE_HOME/lib/postgresql-${POSTGRES_VERSION}.jar \
        && curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
                -o $HIVE_HOME/lib/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
        && curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
                -o $HIVE_HOME/lib/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Optionally copy configs (uncomment if you want to bake them in)
# COPY conf/hive-site.xml $HIVE_HOME/conf/hive-site.xml

EXPOSE 9083

# Default: run Hive Metastore service
ENTRYPOINT ["bin/hive"]
CMD ["--service", "metastore"]
