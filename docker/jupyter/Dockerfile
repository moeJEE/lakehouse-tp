FROM python:3.11-slim-bookworm

# ---- Versions figées ----
ARG ICEBERG_VERSION=1.10.0
ARG DELTA_VERSION=3.3.2
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_V1_VERSION=1.12.262
ARG PG_JDBC_VERSION=42.7.4
ARG MAVEN=https://repo1.maven.org/maven2

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk

# Java 17 (requis par Spark), outils de diagnostic, tini (gestion des signaux)
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps tini \
 && ln -s /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) $JAVA_HOME \
 && rm -rf /var/lib/apt/lists/*

# Bibliothèques Python (versions figées)
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Connecteurs Java intégrés à l'image : aucun téléchargement au premier lancement
RUN JARS=$(python -c "import pyspark,os;print(os.path.join(os.path.dirname(pyspark.__file__),'jars'))") \
 && cd "$JARS" \
 && curl -fsSLO $MAVEN/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/$ICEBERG_VERSION/iceberg-spark-runtime-3.5_2.12-$ICEBERG_VERSION.jar \
 && curl -fsSLO $MAVEN/org/apache/iceberg/iceberg-aws-bundle/$ICEBERG_VERSION/iceberg-aws-bundle-$ICEBERG_VERSION.jar \
 && curl -fsSLO $MAVEN/io/delta/delta-spark_2.12/$DELTA_VERSION/delta-spark_2.12-$DELTA_VERSION.jar \
 && curl -fsSLO $MAVEN/io/delta/delta-storage/$DELTA_VERSION/delta-storage-$DELTA_VERSION.jar \
 && curl -fsSLO $MAVEN/org/apache/hadoop/hadoop-aws/$HADOOP_AWS_VERSION/hadoop-aws-$HADOOP_AWS_VERSION.jar \
 && curl -fsSLO $MAVEN/com/amazonaws/aws-java-sdk-bundle/$AWS_SDK_V1_VERSION/aws-java-sdk-bundle-$AWS_SDK_V1_VERSION.jar \
 && curl -fsSLO $MAVEN/org/postgresql/postgresql/$PG_JDBC_VERSION/postgresql-$PG_JDBC_VERSION.jar

# dbt (TP 2/3) dans un environnement Python séparé : ses dépendances (protobuf…)
# n'entrent pas en conflit avec celles de MLflow. Il réutilise le PySpark de l'image.
ARG DBT_SPARK_VERSION=1.9.3
ARG DBT_TRINO_VERSION=1.10.6
RUN python -m venv --system-site-packages /opt/dbt \
 && /opt/dbt/bin/pip install --no-cache-dir \
      "dbt-core>=1.10,<1.12" \
      "dbt-spark[session]==${DBT_SPARK_VERSION}" \
      "dbt-trino==${DBT_TRINO_VERSION}" \
 && ln -s /opt/dbt/bin/dbt /usr/local/bin/dbt \
 && dbt --version

# Utilisateur non root (UID 1000 : correspond à l'utilisateur courant sous Linux)
RUN useradd -m -u 1000 -s /bin/bash etudiant
USER etudiant
WORKDIR /home/etudiant/work

# Extensions DuckDB pré-installées (lecture S3, Iceberg, Delta)
RUN python -c "import duckdb; [duckdb.sql(f'INSTALL {e}') for e in ('httpfs','iceberg','delta')]" \
 || echo "AVERTISSEMENT : extensions DuckDB non pré-installées"

EXPOSE 8888 4040 8501 5000
ENTRYPOINT ["tini", "--"]
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", \
     "--ServerApp.root_dir=/home/etudiant/work"]
