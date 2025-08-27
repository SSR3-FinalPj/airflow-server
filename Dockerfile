FROM python:3.8-slim

ARG AIRFLOW_UID=50000
ENV AIRFLOW_HOME=/opt/airflow

RUN addgroup --system airflow && \
    adduser --system --ingroup airflow --uid ${AIRFLOW_UID} airflow && \
    mkdir -p ${AIRFLOW_HOME}/logs

RUN apt-get update && apt-get install -y --no-install-recommends git build-essential default-libmysqlclient-dev && rm -rf /var/lib/apt/lists/*

WORKDIR ${AIRFLOW_HOME}

COPY requirements.txt .
RUN pip install --no-cache-dir --prefer-binary -r requirements.txt

# 최신 코드 복사
COPY . ${AIRFLOW_HOME}

# DAG 폴더 권한 보장
RUN mkdir -p ${AIRFLOW_HOME}/dags && chown -R airflow:airflow ${AIRFLOW_HOME}

ENV PYTHONPATH=${AIRFLOW_HOME}

ENV AIRFLOW__CORE__EXECUTOR=KubernetesExecutor
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow

USER airflow
