FROM apache/spark:3.5.3-python3

USER root

# Install Python packages compatible with Spark's Python version
RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.9 \
    kafka-python-ng==2.2.2 \
    pandas==2.0.3

# Copy pre-downloaded JAR dependencies
COPY jars/ /opt/spark/jars/

# Create work directory and set permissions
WORKDIR /opt/spark/work-dir
RUN chown -R spark:spark /opt/spark/work-dir

USER spark

