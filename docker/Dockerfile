FROM openmetadata/ingestion:1.2.0

# Let's use the same workdir as the ingestion image
WORKDIR ingestion
USER airflow

# Install our custom connector
COPY connector connector
COPY setup.py .
RUN pip install fiona
RUN pip install --no-deps .