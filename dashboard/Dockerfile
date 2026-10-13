FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir trino==0.328.0 kafka-python-ng==2.2.2 requests==2.32.3 pandas==2.2.2

COPY phase1/ /app/phase1/
COPY common/ /app/common/
COPY dashboard/ /app/dashboard/

EXPOSE 8050
CMD ["python", "dashboard/server_trino.py"]

