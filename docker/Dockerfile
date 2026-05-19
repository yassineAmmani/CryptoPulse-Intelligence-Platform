FROM eclipse-temurin:11

# Installer Python et dépendances système
RUN apt-get update && \
    apt-get install -y python3 python3-pip wget curl && \
    apt-get clean

# Installer les dépendances Python
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt

# Installer Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

RUN mkdir -p $SPARK_HOME && \
    wget -q https://downloads.apache.org/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz -O /tmp/spark.tgz && \
    tar xzf /tmp/spark.tgz -C /opt && \
    mv /opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION/* $SPARK_HOME && \
    rm -rf /tmp/spark.tgz

# Télécharger le connecteur Kafka pour Spark
RUN mkdir -p $SPARK_HOME/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar -P $SPARK_HOME/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar -P $SPARK_HOME/jars

# Télécharger le driver JDBC PostgreSQL (Supabase)
RUN wget -q https://jdbc.postgresql.org/download/postgresql-42.7.1.jar -P $SPARK_HOME/jars

WORKDIR /app

CMD ["pyspark"]