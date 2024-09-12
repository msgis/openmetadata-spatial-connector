import json
import fiona
from fiona import drvsupport
from rasterio import drivers
import rasterio
import os
from pathlib import Path
from typing import Iterable, Optional, Any, List
from re import sub

from metadata.ingestion.api.common import Entity
from metadata.ingestion.api.models import Either
from metadata.ingestion.api.steps import Source, InvalidSourceException
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)
from metadata.generated.schema.entity.services.connections.database.customDatabaseConnection import (
    CustomDatabaseConnection,
)
from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
from metadata.generated.schema.api.data.createDatabaseSchema import (
    CreateDatabaseSchemaRequest,
)
from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
from metadata.generated.schema.entity.services.databaseService import (
    DatabaseService,
)
from metadata.generated.schema.entity.data.table import (
    Column,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()

class InvalidSpatialConnectorException(Exception):
    """
    Sample data is not valid to be ingested
    """

class SpatialConnector(Source):
    """
    Custom connector to ingest Database metadata from various formats.
    """
    def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
        self.config = config
        self.metadata = metadata

        self.service_connection = config.serviceConnection.__root__.config
        self.search_directory: str = self.service_connection.connectionOptions.__root__.get("search_directory")
        self.skip: str = self.service_connection.connectionOptions.__root__.get("skip")
        self.remote: str = self.service_connection.connectionOptions.__root__.get("remote")

        if not self.search_directory:
            raise InvalidSpatialConnectorException("Missing required connection options")
        
        if not self.remote:
            self.remote = "false"

        base_skip_list = ['shx', 'dbf', 'prj', 'sbn', 'sbx', 'spx', 'atx', 'idx', 'freelist', 'xml', 'gdbtablx', 'gdbtable', 'gdbindexes', 'dwg', 'bak', 'cpg', 'lock', 'lyr', 'dwl', 'dwl2']
        if self.skip:
            self.skip_list : List = base_skip_list + self.skip.split(',')
        else:
            self.skip_list : List = base_skip_list
            logger.info(f"Skip list: {self.skip_list}")
    
        self.data: Optional[dict[Any]] = None
        super().__init__()

    @classmethod
    def create(
        cls, config_dict: dict, metadata: OpenMetadata, pipeline_name: Optional[str] = None
    ) -> "SpatialConnector":
        config: WorkflowSource = WorkflowSource.parse_obj(config_dict)
        connection: CustomDatabaseConnection = config.serviceConnection.__root__.config
        if not isinstance(connection, CustomDatabaseConnection):
            raise InvalidSourceException(
                f"Expected CustomDatabaseConnection, but got {connection}"
            )
        return cls(config, metadata)

    def prepare(self):
        # Validate that the file exists if local
        search_directory = Path(self.search_directory)
        if self.remote != "true":
            if not search_directory.exists():
                raise InvalidSpatialConnectorException("Source Data path does not exist")
            try:
                if search_directory.is_file():
                    [driver, lib] = self.parse_geospatial_file_driver(search_directory)
                    if lib == '':
                        raise InvalidSpatialConnectorException(f"No viable driver found for file: {e}")
                    self.data.update({f'{driver}': {}})
                    if lib == 'fiona':
                        self.data[driver].update(self.parse_geospatial_file(search_directory))
                    elif lib == 'rasterio':
                        self.data[driver].update(self.parse_geospatial_raster_file(search_directory))
                elif search_directory.is_dir():
                    self.data = self.parse_geospatial_directory(search_directory)
            except Exception as e:
                raise InvalidSpatialConnectorException(f"Unknown error reading the source file: {e}")
        else:
            try:
                self.data = self.parse_geospatial_file(self.search_directory)
            except Exception as e:
                raise InvalidSpatialConnectorException(f"Unknown error reading the remote source file: {e}")

    def parse_geospatial_file_driver(self, file_path):
        try:
            driver = drvsupport.driver_from_extension(file_path)
            logger.info(f"Fiona driver found for file {file_path}")
            return [driver, 'fiona']
        except Exception as e:
            logger.info(f"No fiona driver found for file {file_path}: {e}")
        try:
            driver = drivers.driver_from_extension(file_path)
            logger.info(f"Rasterio driver found for file {file_path}")
            return [driver, 'rasterio']
        except Exception as e:
            logger.info(f"No rasterio driver found for file {file_path}: {e}")
        return ['', '']
    
    def parse_geospatial_directory(self, search_directory):
        data : dict = {}
        parsing_dict = []
        no_fiona_driver_dict = []
        try:
            for root, _, files in os.walk(search_directory, topdown=False):
                for file in files:
                    if str(file.split('.')[-1]).lower() in self.skip_list:
                        continue
                    file_path = os.path.join(root,file)
                    try:
                        driver = drvsupport.driver_from_extension(file_path)
                        parsing_dict.append([file_path, 'fiona', driver])
                    except Exception as e:
                        logger.info(f"Error finding fiona driver for file {file_path}: {e}")
                        no_fiona_driver_dict.append([file_path, '', ''])
        except Exception as e:
            logger.error(f"Error reading geospatial directory: {e}")
            raise InvalidSpatialConnectorException(f"Error reading geospatial directory: {e}")
        
        for [file_path, lib, driver] in no_fiona_driver_dict:
            try:
                driver = drivers.driver_from_extension(file_path)
                parsing_dict.append([file_path, 'rasterio', driver])
            except Exception as e:
                logger.info(f"Error finding rasterio driver for file {file_path}: {e}")

        for [file_path, lib, driver] in parsing_dict:
            try:
                if lib == 'fiona':
                    if data == {} or driver not in data.keys():
                        data.update({f'{driver}': {}})
                    data[driver].update(self.parse_geospatial_file(file_path))
                elif lib == 'rasterio':
                    if data == {} or driver not in data.keys():
                        data.update({f'{driver}': {}})
                    data[driver].update(self.parse_geospatial_raster_file(file_path))
            except Exception as e:
                logger.error(f"Error reading geospatial file: {e}")
                raise InvalidSpatialConnectorException(f"Error reading geospatial file: {e}")
        return data

    def parse_geospatial_file(self, file_path):
        data : dict = {}
        try:
            layers = fiona.listlayers(file_path)
            parsed_file_data = {}
            for layer in layers:
                try:
                    with fiona.open(file_path, layer=layer) as layer_data:
                        parsed_file_data.update(self.convert_geospatial_feature(layer_data))
                except Exception as e:
                    logger.error(f"Error reading layer {layer} in file {file_path}: {e}")
            data.update({f'{file_path}': parsed_file_data})
        except Exception as e:
            logger.error(f"Error listing layers in file {file_path}: {e}")
            raise InvalidSpatialConnectorException(f"Error reading geospatial file: {e}")
        return data
    
    def parse_geospatial_raster_file(self, file_path):
        data : dict = {}
        try:
            with rasterio.open(file_path) as dataset:
                data.update({f'{file_path}': self.convert_geospatial_raster_feature(dataset)})
        except Exception as e:
            logger.error(f"Error reading raster file: {file_path}")
            raise InvalidSpatialConnectorException(f"Error reading raster file: {e}")
        return data
    
    def convert_geospatial_feature(self, layer_data):
        prop = layer_data.schema['properties']
        if layer_data.schema['geometry'] != "None":
            prop['geometry'] = layer_data.schema['geometry'] + "/" + sub(r"\D", "", str(layer_data.crs))
        return {
            f'{str(layer_data.name)}': json.dumps(prop),
        }
    
    def convert_geospatial_raster_feature(self, dataset):
        prop = {}
        profile = dataset.profile

        prop.update({"#crs": str(profile['crs'])})
        prop.update({"#count": profile['count']})
        prop.update({"#res": dataset.res})
        prop.update({"#width": profile['width']})
        prop.update({"#height": profile['height']})

        return {
            'profile': json.dumps(prop),
        }

    def yield_create_request_database_service(self):
        yield Either(
            right=self.metadata.get_create_service_from_source(
                entity=DatabaseService, config=self.config
            )
        )

    def yield_driver_db(self):
        # Pick up the service we just created (if not UI)
        service_entity: DatabaseService = self.metadata.get_by_name(
            entity=DatabaseService, fqn=self.config.serviceName
        )

        for key in self.data.keys():
            db_name = key.replace(' ', '_')
            yield Either(
                right=CreateDatabaseRequest(
                    name=db_name,
                    service=service_entity.fullyQualifiedName,
                    description=f"Database for {key}",
                )
            )

    def yield_filename_schema(self):
        # Pick up the service we just created (if not UI)
        for key in self.data.keys():
            for int_key in self.data[key].keys():
                root_len = len(self.search_directory)
                db_name = key.replace(' ', '_')
                filename = int_key[root_len:].split('.')[0]
                database_entity: Database = self.metadata.get_by_name(
                    entity=Database, fqn=f"{self.config.serviceName}.{db_name}"
                )
                yield Either(
                    right=CreateDatabaseSchemaRequest(
                        name=filename,
                        database=database_entity.fullyQualifiedName,
                        description=f"Path: {int_key}",
                    )
                )

    def yield_data(self):
        """
        Iterate over the data list to create tables
        """

        for key in self.data.keys():
            driver_files = self.data[key]
            for int_key in driver_files.keys():
                root_len = len(self.search_directory)
                db_name = key.replace(' ', '_')
                filename = int_key[root_len:].split('.')[0]
                database_schema: DatabaseSchema = self.metadata.get_by_name(
                    entity=DatabaseSchema,
                    fqn=f"{self.config.serviceName}.{db_name}.{filename}",
                )
                file_data = driver_files[int_key]
                for lyr_key in file_data.keys():
                    lyr_data = json.loads(file_data[lyr_key])
                    yield Either(
                    right=CreateTableRequest(
                        name=lyr_key,
                        databaseSchema=database_schema.fullyQualifiedName,
                        columns=[
                            Column(
                                name=(field_key if field_key[0] != '#' else field_key[1:]),
                                dataType=self.map_datatypes(field_key, lyr_data[field_key]),
                                description=str(lyr_data[field_key]),
                                )
                            for field_key in lyr_data.keys()
                            ],
                        )
                    )

         
    def map_datatypes(self, key, datatype):
        if key == '#crs':
            return 'STRING'
        elif key == '#count':
            return 'INT'
        elif key == '#res':
            return 'TUPLE'
        elif key == '#width':
            return 'INT'
        elif key == '#height':
            return 'INT'
        elif key == 'geometry':
            if 'point' in datatype:
                return 'POINT'
            return 'GEOMETRY'
        elif 'datetime' in datatype:
            return 'DATETIME'
        elif 'date' in datatype:
            return 'DATE'
        elif 'bool' in datatype:
            return 'BOOLEAN'
        elif 'decimal' in datatype:
            return 'DECIMAL'
        elif 'double' in datatype:
            return 'DOUBLE'
        elif 'long' in datatype:
            return 'LONG'
        elif 'varchar' in datatype:
            return 'VARCHAR'
        elif 'timestamp' in datatype:
            return 'TIMESTAMP'
        elif 'time' in datatype:
            return 'TIME'
        elif 'int' in datatype:
            return 'INT'
        elif 'float' in datatype:
            return 'FLOAT'
        elif 'str' in datatype:
            return 'STRING'
        else:
            return 'STRING'
        
    def _iter(self) -> Iterable[Entity]:
        yield from self.yield_create_request_database_service()
        yield from self.yield_driver_db()
        yield from self.yield_filename_schema()
        yield from self.yield_data()

    def test_connection(self) -> None:
        pass

    def close(self):
        pass
