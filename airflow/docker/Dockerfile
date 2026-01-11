FROM apache/airflow:2.8.1-python3.10

USER root

RUN apt-get update && \
    apt-get install -y gcc g++ && \
    apt-get clean

USER airflow

# Copy project assets (context = repo root)
COPY src /opt/airflow/src
COPY sql /opt/airflow/sql
COPY data /opt/airflow/data
COPY requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

ENV PYTHONPATH="/opt/airflow:/opt/airflow/src"
