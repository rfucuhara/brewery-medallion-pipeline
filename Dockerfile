FROM apache/airflow:2.7.1

USER root
# Instala Java 11 (Headless é mais leve e suficiente para o Spark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jdk-headless && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Define a variável JAVA_HOME (ajuda o Spark a encontrar o Java sem erro)
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

USER airflow

# Copia e instala dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt