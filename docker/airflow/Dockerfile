# Image Airflow du cours : construite sur l'image JupyterLab, elle dispose donc de Spark,
# des connecteurs Iceberg/S3 et de dbt. Airflow est installé dans son propre environnement
# Python (/opt/airflow-venv) pour ne pas perturber les bibliothèques du cours.
ARG BASE_IMAGE=ghcr.io/moejee/lakehouse-jupyter:2026.2
FROM ${BASE_IMAGE}

ARG AIRFLOW_VERSION=3.1.0
ARG PYTHON_VERSION=3.11
USER root
RUN python -m venv /opt/airflow-venv \
 && /opt/airflow-venv/bin/pip install --no-cache-dir \
      "apache-airflow[postgres]==${AIRFLOW_VERSION}" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
 && ln -s /opt/airflow-venv/bin/airflow /usr/local/bin/airflow \
 && airflow version

COPY demarrer.sh /opt/airflow-demarrer.sh
RUN chmod +x /opt/airflow-demarrer.sh && mkdir -p /opt/airflow && chown etudiant /opt/airflow
USER etudiant
ENV AIRFLOW_HOME=/opt/airflow
EXPOSE 8080
ENTRYPOINT ["tini", "--"]
CMD ["/opt/airflow-demarrer.sh"]
