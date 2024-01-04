import json
import fiona
from pathlib import Path
from typing import Iterable, Optional, List, Any
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
        self.file_source: str = self.service_connection.connectionOptions.__root__.get("file_source")
        self.remote: str = self.service_connection.connectionOptions.__root__.get("remote")
        self.business_unit: str = self.service_connection.connectionOptions.__root__.get("business_unit")

        if not self.file_source:
            raise InvalidSpatialConnectorException("Missing required connection options")
        
        if not self.remote:
            self.remote = "false"
    
        self.data: Optional[List[Any]] = None
        super().__init__()

    @classmethod
    def create(
        cls, config_dict: dict, metadata_config: OpenMetadataConnection
    ) -> "SpatialConnector":
        config: WorkflowSource = WorkflowSource.parse_obj(config_dict)
        connection: CustomDatabaseConnection = config.serviceConnection.__root__.config
        if not isinstance(connection, CustomDatabaseConnection):
            raise InvalidSourceException(
                f"Expected CustomDatabaseConnection, but got {connection}"
            )
        return cls(config, metadata_config)

    def prepare(self):
        # Validate that the file exists if local
        if self.remote != "true":
            source_data = Path(self.file_source)
            if not source_data.exists():
                raise InvalidSpatialConnectorException("Source Data path does not exist")
        else:
            source_data = self.file_source
        try:
            self.data = self.read_geospatial_file(source_data)
        except:
            raise InvalidSpatialConnectorException("Unknown error reading the source file")

    def read_geospatial_file(self, file_path):
        data : List = []
        try:
            layers = fiona.listlayers(file_path)
            for layer in layers:
                try:
                    with fiona.open(file_path, layer=layer) as layer_data:
                        data.append(self.convert_geospatial_feature(layer_data))
                except Exception as e:
                    logger.error(f"Error reading layer {layer} in file {file_path}: {e}")
        except Exception as e:
            logger.error(f"Error listing layers in file {file_path}: {e}")
            raise InvalidSpatialConnectorException(f"Error reading geospatial file: {e}")
    
        return data

    def convert_geospatial_feature(self, layer_data):
        prop = layer_data.schema['properties']
        if layer_data.schema['geometry'] != "None":
            prop['geometry'] = layer_data.schema['geometry'] + "/" + sub(r"\D", "", str(layer_data.crs))
        return {
            'layer': {
                'name': str(layer_data.name),
                'schema': json.dumps(prop),
            }
        }

    def yield_create_request_database_service(self):
        yield Either(
            right=self.metadata.get_create_service_from_source(
                entity=DatabaseService, config=self.config
            )
        )

    def yield_business_unit_db(self):
        # Pick up the service we just created (if not UI)
        service_entity: DatabaseService = self.metadata.get_by_name(
            entity=DatabaseService, fqn=self.config.serviceName
        )

        yield Either(
            right=CreateDatabaseRequest(
                name=self.business_unit,
                service=service_entity.fullyQualifiedName,
            )
        )

    def yield_default_schema(self):
        # Pick up the service we just created (if not UI)
        database_entity: Database = self.metadata.get_by_name(
            entity=Database, fqn=f"{self.config.serviceName}.{self.business_unit}"
        )

        yield Either(
            right=CreateDatabaseSchemaRequest(
                name="default",
                database=database_entity.fullyQualifiedName,
            )
        )

    def yield_data(self):
        """
        Iterate over the data list to create tables
        """

        database_schema: DatabaseSchema = self.metadata.get_by_name(
            entity=DatabaseSchema,
            fqn=f"{self.config.serviceName}.{self.business_unit}.default",
        )

        for layer in self.data:
            data = json.loads(layer['layer']['schema'])
            yield Either(
                right=CreateTableRequest(
                    name=layer['layer']['name'],
                    databaseSchema=database_schema.fullyQualifiedName,
                    columns=[
                        Column(
                            name=key,
                            dataType=self.map_datatypes(key, data[key]),
                            description=data[key],
                        )
                        for key in data.keys()
                    ],
                )
            )
         
    def map_datatypes(self, key, datatype):
        if key == 'geometry':
            return 'GEOMETRY'
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
        yield from self.yield_business_unit_db()
        yield from self.yield_default_schema()
        yield from self.yield_data()

    def test_connection(self) -> None:
        pass

    def close(self):
        pass
