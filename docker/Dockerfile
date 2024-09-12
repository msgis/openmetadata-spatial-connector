FROM openmetadata/ingestion:1.4.4

# Let's use the same workdir as the ingestion image
WORKDIR /ingestion
USER airflow

# Install our custom connector
COPY connector connector
COPY setup.py .
RUN pip install --no-deps .
RUN pip install fiona
RUN pip install rasterio
