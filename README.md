# OpenMetadata Spatial Connector
This repository is an custom [OpenMetadata](https://open-metadata.org/) Connector for GIS data formats.

![OMD Spatial Connector](images%2Fhead.png)


## Step 1 - Prepare the package installation
We'll need to package the code so that it can be shipped to the ingestion container and used there. You can find a simple `setup.py` that builds the `connector` module.

## Step 2 - Prepare the Ingestion Image

If you want to use the connector from the UI, the `openmetadata-ingestion` image should be aware of your new package.

We will be running the against the OpenMetadata version `1.4.4`, therefore, our Dockerfile looks like:

```Dockerfile
# Base image from the right version
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
```
Build and use the new openmetadata-ingestion images in Docker compose:
```yaml
  ingestion:
    container_name: openmetadata_ingestion
    build:
      context: ../
      dockerfile: docker/Dockerfile
```

## Step 3 - Run OpenMetadata with the custom Ingestion image

We have a `Makefile` prepared for you to run `make run`. This will get OpenMetadata up in Docker Compose using the custom Ingestion image.

You may also just run:

```cmd
docker compose -f ./docker/docker-compose.yml up -d
```

## Step 4 - Configure the Connector

In this guide we prepared a Database Connector. Thus, go to `Database Services > Add New Service > Custom` and set the `Source Python Class Name` as `connector.spatial_connector.SpatialConnector`.

Note how we are specifying the full module name so that the Ingestion Framework can import the Source class.

![demo.gif](images%2Fsetup_demo.gif)

## OpenMetadata Spatial Connector

To run the OpenMetadata Spatial Connector, the Python class will be `connector.spatial_connector.SpatialConnector` and we'll need to set the following Connection Options:
- `skip`: Comma-separated list of file extensions that should be skipped. Can be omitted otherwise.
- `search_directory`: The path or URL to a vector/raster file or folder containing multiple vector/raster files. The connector uses [Fiona](https://fiona.readthedocs.io/en/latest/index.html) and [Rasterio](https://rasterio.readthedocs.io/en/stable/) which support [GDAL VFS](https://gdal.org/user/virtual_file_systems.html).
- `remote`: `true` if `search_directory` is a vfs url, e.g. `zip+https://raw.githubusercontent.com/OSGeo/gdal/master/autotest/ogr/data/shp/poly.zip` or `/vsizip//vsicurl/https://raw.githubusercontent.com/OSGeo/gdal/master/autotest/ogr/data/shp/poly.zip`. Can be omitted otherwise.  

## Contributing

Everyone is invited to get involved and contribute to the project.

Simply create a [fork and pull request](https://docs.github.com/en/get-started/quickstart/contributing-to-projects) for code contributions or
feel free to [open an issue](https://github.com/msgis/openmetadata-spatial-connector/issues) for any other contributions or issues.