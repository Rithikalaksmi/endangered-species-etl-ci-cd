FROM apache/superset:4.0.2

USER root
RUN pip install --no-cache-dir trino[sqlalchemy]==0.328.0 psycopg2-binary==2.9.9
USER superset

