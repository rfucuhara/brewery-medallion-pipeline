FROM apache/airflow:2.7.1

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jdk-headless && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

USER airflow


COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt