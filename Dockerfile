FROM apache/airflow:2.10.3-python3.11

USER root
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk ant && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# RUN curl -O -L https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz \
#     && tar -zxvf spark-3.5.0-bin-hadoop3.tgz \
#     && rm -rf spark-3.5.0-bin-hadoop3.tgz \
#     && mv spark-3.5.0-bin-hadoop3/ /usr/local/ \
#     && rm -rf /usr/local/spark \
#     && ln -s /usr/local/spark-3.5.0-bin-hadoop3 /usr/local/spark

# ENV SPARK_HOME=/usr/local/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# ENV PATH=$SPARK_HOME/bin:$JAVA_HOME/bin:$PATH
# RUN export SPARK_HOME
RUN export JAVA_HOME
# RUN export PATH

# RUN curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.29.26/s3-2.29.26.jar \
#     && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.779/aws-java-sdk-bundle-1.12.779.jar \
#     && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar \
#     && mkdir -p /usr/local/spark/jars \
#     && mv s3-2.29.26.jar /usr/local/spark/jars \
#     && mv aws-java-sdk-bundle-1.12.779.jar /usr/local/spark/jars \
#     && mv hadoop-aws-3.4.1.jar /usr/local/spark/jars

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip && pip install -r /requirements.txt