FROM apache/airflow:2.9.3-python3.11

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-trino==5.6.2 \
    kafka-python-ng==2.2.2 \
    requests==2.32.3 \
    trino==0.328.0

