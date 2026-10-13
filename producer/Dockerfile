FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python-ng==2.2.2 pandas==2.2.2

COPY phase1/ /app/phase1/
COPY common/ /app/common/
COPY producer/ /app/producer/
COPY master_endangered_species.csv /app/master_endangered_species.csv

CMD ["python", "-m", "producer.producer", "--mode", "live", "--rate", "10"]

