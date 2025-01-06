FROM apache/airflow:2.10.3-python3.11

USER root
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk ant && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN export JAVA_HOME

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip && pip install -r /requirements.txt