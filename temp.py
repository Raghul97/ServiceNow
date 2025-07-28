from typing import Optional, Dict, Any, List, Tuple
import requests
from fastapi import APIRouter, HTTPException, Depends
from app.schemas import (
    MessageResponse, DatabaseServicePayload, DatabaseMetadataResponse,
    TablesResponse, ServiceModel, DatabaseModel, SchemaModel, TableModel,
    ColumnModel, OwnerModel, TagModel, MetadataSummary, TableFilterInfo
)
from app.core.logging import get_logger
from app.core.config import settings
from app.core.openmetadata_api import get_openmetadata_client_dependency, OpenMetadataAPIError
 
router = APIRouter()
logger = get_logger(__name__)
 
 
def _build_include_params(include_sample_data: bool, include_table_profiles: bool, include_lineage: bool) -> List[str]:
    """Build comprehensive include parameters for detailed table fetch."""
    include_params = [
        "all", "joins", "tags", "owner", "followers", "extension", "domain",
        "dataProducts", "votes", "lifeCycle", "certification", "sourceHash"
    ]
    
    if include_sample_data:
        include_params.append("sampleData")
    if include_table_profiles:
        include_params.extend(["tableProfile", "profile"])
    if include_lineage:
        include_params.extend(["lineage", "dataProducts", "upstream", "downstream"])
    
    return include_params
 
 
def _create_column_model(column_data: Dict[str, Any]) -> ColumnModel:
    """Create a ColumnModel from OpenMetadata column data."""
    data_type_info = column_data.get("dataType", {})
    
    return ColumnModel(
        name=column_data.get("name", ""),
        dataType=data_type_info.get("name") if isinstance(data_type_info, dict) else column_data.get("dataType"),
        dataTypeDisplay=column_data.get("dataTypeDisplay") or data_type_info.get("displayName"),
        description=column_data.get("description"),
        ordinalPosition=column_data.get("ordinalPosition"),
        constraint=column_data.get("constraint", {}).get("name") if isinstance(column_data.get("constraint"), dict) else column_data.get("constraint"),
        tags=[
            TagModel(
                tagFQN=tag.get("tagFQN", ""),
                description=tag.get("description"),
                source=tag.get("source")
            ) for tag in column_data.get("tags", [])
        ],
        # Enhanced metadata
        nullable=column_data.get("nullable"),
        defaultValue=column_data.get("defaultValue"),
        precision=data_type_info.get("precision") if isinstance(data_type_info, dict) else None,
        scale=data_type_info.get("scale") if isinstance(data_type_info, dict) else None,
        maxLength=data_type_info.get("length") if isinstance(data_type_info, dict) else None,
        # Additional OpenMetadata fields
        arrayDataType=column_data.get("arrayDataType"),
        dataLength=column_data.get("dataLength"),
        jsonSchema=column_data.get("jsonSchema"),
        fullyQualifiedName=column_data.get("fullyQualifiedName"),
        children=[
            _create_column_model(child) for child in column_data.get("children", [])
        ],
        customMetrics=column_data.get("customMetrics")
    )
 
 
def _fetch_service_metadata(session: requests.Session, base_url: str, service_name: str) -> Dict[str, Any]:
    """Fetch comprehensive service metadata from OpenMetadata."""
    try:
        service_response = session.get(
            f"{base_url}/services/databaseServices/name/{service_name}",
            params={
                "include": "all,tags,owners,followers,domain,dataProducts",
                "fields": "owners,tags,connection,version,ingestionSchedule,sourceUrl,domains,dataProducts,lifeCycle,certification,votes,followers,sourceHash"
            },
            timeout=30
        )
        service_response.raise_for_status()
        
        if service_response.status_code != 200:
            raise HTTPException(status_code=404, detail=f"Service '{service_name}' not found")
        
        return service_response.json()
    except requests.HTTPError as e:
        logger.error(f"Failed to fetch service metadata for {service_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch service metadata: {str(e)}")
 
 
def _fetch_detailed_table_metadata(session: requests.Session, base_url: str, table_id: str,
                                 include_sample_data: bool, include_table_profiles: bool, include_lineage: bool) -> Optional[Dict[str, Any]]:
    """Fetch detailed table metadata from OpenMetadata."""
    try:
        include_params = _build_include_params(include_sample_data, include_table_profiles, include_lineage)
        
        detailed_response = session.get(
            f"{base_url}/tables/{table_id}",
            params={
                "include": ",".join(include_params),
                "fields": "columns,tableConstraints,tablePartition,owners,tags,followers,usageSummary,profile,sampleData,joins,lineage,testSuite,dataModel,location,extension,domain,dataProducts,votes,lifeCycle,certification,sourceUrl,schemaDefinition,retentionPeriod,sourceHash,queries,customMetrics"
            },
            timeout=30
        )
        
        if detailed_response.status_code == 200:
            return detailed_response.json()
        else:
            logger.warning(f"Could not fetch detailed metadata for table {table_id}: HTTP {detailed_response.status_code}")
            return None
    except Exception as e:
        logger.warning(f"Error fetching detailed metadata for table {table_id}: {str(e)}")
        return None
 
 
def _create_table_model(table_data: Dict[str, Any], include_sample_data: bool, include_table_profiles: bool, include_lineage: bool) -> TableModel:
    """Create a TableModel from OpenMetadata table data."""
    # Create enhanced table columns
    columns_list = [
        _create_column_model(col) for col in table_data.get("columns", [])
    ]
    
    return TableModel(
        id=table_data.get("id", ""),
        name=table_data.get("name", ""),
        fullyQualifiedName=table_data.get("fullyQualifiedName", ""),
        tableType=table_data.get("tableType"),
        description=table_data.get("description"),
        displayName=table_data.get("displayName"),
        owners=[
            OwnerModel(
                id=owner.get("id"),
                name=owner.get("name", ""),
                type=owner.get("type", ""),
                fullyQualifiedName=owner.get("fullyQualifiedName")
            ) for owner in table_data.get("owners", [])
        ],
        tags=[
            TagModel(
                tagFQN=tag.get("tagFQN", ""),
                description=tag.get("description"),
                source=tag.get("source")
            ) for tag in table_data.get("tags", [])
        ],
        columns=columns_list,
        column_count=len(table_data.get("columns", [])),
        # Core metadata fields
        tableConstraints=table_data.get("tableConstraints"),
        partitionKeys=table_data.get("tablePartition", {}).get("columns") if table_data.get("tablePartition") else None,
        distributionKeys=table_data.get("distributionKey"),
        sortKeys=table_data.get("sortKey"),
        tableProfile=table_data.get("tableProfile") if include_table_profiles else None,
        sampleData=table_data.get("sampleData") if include_sample_data else None,
        usageSummary=table_data.get("usageSummary"),
        lineage=table_data.get("lineage") if include_lineage else None,
        # Additional OpenMetadata fields
        schemaDefinition=table_data.get("schemaDefinition"),
        location=table_data.get("location", {}).get("name") if table_data.get("location") else None,
        locationPath=table_data.get("locationPath"),
        fileFormat=table_data.get("fileFormat"),
        retentionPeriod=table_data.get("retentionPeriod"),
        sourceUrl=table_data.get("sourceUrl"),
        domains=[domain.get("name", "") for domain in table_data.get("domains", [])],
        dataProducts=[dp.get("name", "") for dp in table_data.get("dataProducts", [])],
        lifeCycle=table_data.get("lifeCycle"),
        certification=table_data.get("certification"),
        votes=table_data.get("votes"),
        testSuite=table_data.get("testSuite", {}).get("name") if table_data.get("testSuite") else None,
        queries=table_data.get("queries"),
        customMetrics=table_data.get("customMetrics"),
        sourceHash=table_data.get("sourceHash"),
        processedLineage=table_data.get("processedLineage"),
        joins=table_data.get("joins"),
        followers=[follower.get("name", "") for follower in table_data.get("followers", [])]
    )
 
 
def _process_database_tables(session: requests.Session, base_url: str, schema_data: Dict[str, Any],
                           include_sample_data: bool, include_table_profiles: bool, include_lineage: bool) -> Tuple[List[TableModel], int]:
    """Process tables for a database schema."""
    tables_list = []
    
    # Get tables for this schema with comprehensive metadata
    tables_response = session.get(
        f"{base_url}/tables",
        params={
            "databaseSchema": schema_data.get("fullyQualifiedName"),
            "include": "all,tags,owners",
            "fields": "columns,owners,tags,tableType,description,displayName,tableConstraints,tablePartition,usageSummary"
        },
        timeout=30
    )
    
    if tables_response.status_code != 200:
        logger.warning(f"Could not fetch tables for schema {schema_data.get('name')}: HTTP {tables_response.status_code}")
        return [], 0
    
    tables_data = tables_response.json()
    tables = tables_data.get("data", [])
    
    for table in tables:
        table_id = table.get("id")
        detailed_table = table  # Default to basic table info
        
        # Fetch detailed table metadata if table ID is available
        if table_id:
            detailed_metadata = _fetch_detailed_table_metadata(
                session, base_url, table_id, include_sample_data, include_table_profiles, include_lineage
            )
            if detailed_metadata:
                detailed_table = detailed_metadata
                logger.debug(f"Fetched detailed metadata for table {table.get('name')}")
        
        table_model = _create_table_model(detailed_table, include_sample_data, include_table_profiles, include_lineage)
        tables_list.append(table_model)
    
    return tables_list, len(tables)
 
 
def _create_service_model(service_data: Dict[str, Any]) -> ServiceModel:
    """Create a ServiceModel from OpenMetadata service data."""
    return ServiceModel(
        id=service_data.get("id", ""),
        name=service_data.get("name", ""),
        serviceType=service_data.get("serviceType", ""),
        description=service_data.get("description"),
        displayName=service_data.get("displayName"),
        fullyQualifiedName=service_data.get("fullyQualifiedName", ""),
        owners=[
            OwnerModel(
                id=owner.get("id"),
                name=owner.get("name", ""),
                type=owner.get("type", ""),
                fullyQualifiedName=owner.get("fullyQualifiedName")
            ) for owner in service_data.get("owners", [])
        ],
        tags=[
            TagModel(
                tagFQN=tag.get("tagFQN", ""),
                description=tag.get("description"),
                source=tag.get("source")
            ) for tag in service_data.get("tags", [])
        ],
        connection=service_data.get("connection"),
        version=service_data.get("version"),
        ingestionSchedule=service_data.get("ingestionSchedule"),
        # Additional OpenMetadata fields
        sourceUrl=service_data.get("sourceUrl"),
        domains=[domain.get("name", "") for domain in service_data.get("domains", [])],
        dataProducts=[dp.get("name", "") for dp in service_data.get("dataProducts", [])],
        lifeCycle=service_data.get("lifeCycle"),
        certification=service_data.get("certification"),
        votes=service_data.get("votes"),
        followers=[follower.get("name", "") for follower in service_data.get("followers", [])],
        sourceHash=service_data.get("sourceHash")
    )
 
 
def _process_databases(session: requests.Session, base_url: str, service_name: str,
                      include_sample_data: bool, include_table_profiles: bool, include_lineage: bool) -> Tuple[List[DatabaseModel], Dict[str, int]]:
    """Process all databases for a service and return database models with summary statistics."""
    # Get databases for this service with comprehensive metadata
    databases_response = session.get(
        f"{base_url}/databases",
        params={
            "service": service_name,
            "include": "all,tags,owners,domain,dataProducts",
            "fields": "owners,tags,location,version,usageSummary,retentionPeriod,sourceUrl,domains,dataProducts,votes,lifeCycle,certification,followers,sourceHash,default"
        },
        timeout=30
    )
    databases_response.raise_for_status()
    
    databases_data = databases_response.json()
    databases = databases_data.get("data", [])
    
    logger.info("Found databases", count=len(databases))
    
    databases_list = []
    total_schemas = 0
    total_tables = 0
    total_columns = 0
    
    for database in databases:
        # Create database owners
        db_owners = [
            OwnerModel(
                id=owner.get("id"),
                name=owner.get("name", ""),
                type=owner.get("type", ""),
                fullyQualifiedName=owner.get("fullyQualifiedName")
            ) for owner in database.get("owners", [])
        ]
        
        # Process schemas for this database
        schemas_list = _process_database_schemas(
            session, base_url, database, include_sample_data, include_table_profiles, include_lineage
        )
        
        # Calculate counts for this database
        db_table_count = sum(schema.table_count for schema in schemas_list)
        db_column_count = sum(len(table.columns) for schema in schemas_list for table in schema.tables)
        
        total_schemas += len(schemas_list)
        total_tables += db_table_count
        total_columns += db_column_count
        
        # Create enhanced database model
        database_model = DatabaseModel(
            id=database.get("id", ""),
            name=database.get("name", ""),
            fullyQualifiedName=database.get("fullyQualifiedName", ""),
            description=database.get("description"),
            owners=db_owners,
            schemas=schemas_list,
            schema_count=len(schemas_list),
            table_count=db_table_count,
            tags=[
                TagModel(
                    tagFQN=tag.get("tagFQN", ""),
                    description=tag.get("description"),
                    source=tag.get("source")
                ) for tag in database.get("tags", [])
            ],
            location=database.get("location", {}).get("name") if database.get("location") else database.get("location"),
            databaseVersion=database.get("version"),
            # Additional OpenMetadata fields
            dataProducts=[dp.get("name", "") for dp in database.get("dataProducts", [])],
            usageSummary=database.get("usageSummary"),
            retentionPeriod=database.get("retentionPeriod"),
            sourceUrl=database.get("sourceUrl"),
            domains=[domain.get("name", "") for domain in database.get("domains", [])],
            votes=database.get("votes"),
            lifeCycle=database.get("lifeCycle"),
            certification=database.get("certification"),
            followers=[follower.get("name", "") for follower in database.get("followers", [])],
            sourceHash=database.get("sourceHash"),
            default=database.get("default")
        )
        
        databases_list.append(database_model)
    
    summary_stats = {
        "total_databases": len(databases),
        "total_schemas": total_schemas,
        "total_tables": total_tables,
        "total_columns": total_columns
    }
    
    return databases_list, summary_stats
 
 
def _process_database_schemas(session: requests.Session, base_url: str, database_data: Dict[str, Any],
                            include_sample_data: bool, include_table_profiles: bool, include_lineage: bool) -> List[SchemaModel]:
    """Process schemas for a database."""
    # Get schemas for this database with comprehensive metadata
    schemas_response = session.get(
        f"{base_url}/databaseSchemas",
        params={
            "database": database_data.get("fullyQualifiedName"),
            "include": "all,tags,owners",
            "fields": "owners,tags,retentionPeriod"
        },
        timeout=30
    )
    
    if schemas_response.status_code != 200:
        logger.warning(f"Could not fetch schemas for database {database_data.get('name')}: HTTP {schemas_response.status_code}")
        return []
    
    schemas_data = schemas_response.json()
    schemas = schemas_data.get("data", [])
    
    schemas_list = []
    for schema in schemas:
        # Process tables for this schema
        tables_list, table_count = _process_database_tables(
            session, base_url, schema, include_sample_data, include_table_profiles, include_lineage
        )
        
        # Create enhanced schema model
        schema_model = SchemaModel(
            id=schema.get("id", ""),
            name=schema.get("name", ""),
            fullyQualifiedName=schema.get("fullyQualifiedName", ""),
            description=schema.get("description"),
            tables=tables_list,
            table_count=table_count,
            owners=[
                OwnerModel(
                    id=owner.get("id"),
                    name=owner.get("name", ""),
                    type=owner.get("type", ""),
                    fullyQualifiedName=owner.get("fullyQualifiedName")
                ) for owner in schema.get("owners", [])
            ],
            tags=[
                TagModel(
                    tagFQN=tag.get("tagFQN", ""),
                    description=tag.get("description"),
                    source=tag.get("source")
                ) for tag in schema.get("tags", [])
            ],
            retentionPeriod=schema.get("retentionPeriod")
        )
        
        schemas_list.append(schema_model)
    
    return schemas_list
 
 
def _create_metadata_summary(service_model: ServiceModel, databases_list: List[DatabaseModel], summary_stats: Dict[str, int]) -> MetadataSummary:
    """Create comprehensive metadata summary with detailed statistics."""
    from datetime import datetime
    
    total_views = sum(
        1 for db in databases_list
        for schema in db.schemas
        for table in schema.tables
        if table.tableType and 'view' in table.tableType.lower()
    )
    
    # Count unique owners and tags across all entities
    all_owners = set()
    all_tags = set()
    
    # Collect from service
    all_owners.update(owner.name for owner in service_model.owners if owner.name)
    all_tags.update(tag.tagFQN for tag in service_model.tags if tag.tagFQN)
    
    # Collect from databases, schemas, and tables
    for db in databases_list:
        all_owners.update(owner.name for owner in db.owners if owner.name)
        all_tags.update(tag.tagFQN for tag in db.tags if tag.tagFQN)
        
        for schema in db.schemas:
            all_owners.update(owner.name for owner in schema.owners if owner.name)
            all_tags.update(tag.tagFQN for tag in schema.tags if tag.tagFQN)
            
            for table in schema.tables:
                all_owners.update(owner.name for owner in table.owners if owner.name)
                all_tags.update(tag.tagFQN for tag in table.tags if tag.tagFQN)
                
                # Also collect from columns
                for column in table.columns:
                    all_tags.update(tag.tagFQN for tag in column.tags if tag.tagFQN)
    
    return MetadataSummary(
        total_databases=summary_stats["total_databases"],
        total_schemas=summary_stats["total_schemas"],
        total_tables=summary_stats["total_tables"],
        total_columns=summary_stats["total_columns"],
        total_views=total_views,
        total_owners=len(all_owners),
        total_tags=len(all_tags),
        data_extraction_timestamp=datetime.utcnow().isoformat() + "Z"
    )
 
 
@router.post("/service", response_model=MessageResponse, tags=["OpenMetadata API"])
def create_database_service(
    payload: DatabaseServicePayload,
    session: requests.Session = Depends(get_openmetadata_client_dependency)
) -> MessageResponse:
    """
    Create a database service in OpenMetadata using direct API calls.
    
    Uses OpenMetadata REST API: POST /services/databaseServices
    API Documentation: https://docs.open-metadata.org/swagger.html
    """
    try:
        logger.info("Database service creation requested",
                   name=payload.name,
                   service_type=payload.serviceType)
        
        # Validate service type
        valid_service_types = [
            "MySQL", "PostgreSQL", "BigQuery", "Snowflake", "Redshift",
            "Oracle", "MSSQL", "MongoDB", "Cassandra", "Clickhouse",
            "Databricks", "Athena", "Hive", "Presto", "Trino"
        ]
        
        if payload.serviceType not in valid_service_types:
            logger.warning("Invalid service type", service_type=payload.serviceType)
            raise HTTPException(
                status_code=400,
                detail=f"Invalid serviceType. Must be one of: {', '.join(valid_service_types)}"
            )
        
        base_url = settings.openmetadata_url.rstrip('/') + '/api/v1'
        
        # Check if service already exists
        try:
            existing_response = session.get(f"{base_url}/services/databaseServices/name/{payload.name}", timeout=30)
            if existing_response.status_code == 200:
                existing_service = existing_response.json()
                logger.warning("Database service already exists",
                              service_name=payload.name,
                              service_id=existing_service.get("id"))
                return MessageResponse(
                    message=f"Database service '{payload.name}' already exists in OpenMetadata with ID: {existing_service.get('id')}"
                )
        except requests.HTTPError:
            # Service doesn't exist, which is what we want
            logger.debug("Service doesn't exist yet, proceeding with creation", service_name=payload.name)
        
        # Build OpenMetadata service creation request
        service_request = {
            "name": payload.name,
            "serviceType": payload.serviceType,
            "connection": {
                "config": payload.connection
            }
        }
        
        # Add optional fields if provided
        if payload.description:
            service_request["description"] = payload.description
        
        if payload.displayName:
            service_request["displayName"] = payload.displayName
        
        # Handle tags - convert to OpenMetadata format
        if payload.tags:
            service_request["tags"] = [{"tagFQN": tag} for tag in payload.tags]
        
        # Handle owners - convert to OpenMetadata EntityReference format
        if payload.owners:
            owner_refs = []
            for owner in payload.owners:
                owner_refs.append({
                    "name": owner.name,
                    "type": owner.type.value if hasattr(owner.type, 'value') else owner.type
                })
            service_request["owners"] = owner_refs
        
        logger.info("Database service creation request prepared",
                   service_name=payload.name,
                   has_owners=len(payload.owners) if payload.owners else 0,
                   has_tags=len(payload.tags) if payload.tags else 0)
        
        # Create service using OpenMetadata API
        try:
            response = session.post(f"{base_url}/services/databaseServices", json=service_request, timeout=30)
            response.raise_for_status()
            
            created_service = response.json()
            
            logger.info("Database service created successfully",
                       service_id=created_service.get("id"),
                       service_name=created_service.get("name"),
                       service_type=created_service.get("serviceType"))
            
            return MessageResponse(
                message=f"Database service '{created_service.get('name')}' created successfully in OpenMetadata with ID: {created_service.get('id')}"
            )
                
        except requests.HTTPError as http_error:
            logger.error("OpenMetadata API error during service creation",
                        status_code=http_error.response.status_code if http_error.response else None,
                        response_text=http_error.response.text if http_error.response else str(http_error),
                        service_name=payload.name)
            
            # Check if it's a conflict error (service already exists)
            if http_error.response and http_error.response.status_code == 409:
                return MessageResponse(
                    message=f"Database service '{payload.name}' already exists in OpenMetadata"
                )
            else:
                error_detail = "Unknown error"
                try:
                    if http_error.response:
                        error_data = http_error.response.json()
                        error_detail = error_data.get("message", error_data.get("detail", str(error_data)))
                    else:
                        error_detail = str(http_error)
                except:
                    error_detail = http_error.response.text if http_error.response else str(http_error)
                
                raise HTTPException(
                    status_code=500,
                    detail=f"OpenMetadata service creation failed: {error_detail}"
                )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error creating database service", exc_info=e)
        raise HTTPException(status_code=500, detail="Internal server error")
 
 
@router.get("/service/{service_name}/metadata", response_model=DatabaseMetadataResponse, tags=["OpenMetadata API"])
def extract_database_metadata(
    service_name: str,
    include_sample_data: bool = False,
    include_table_profiles: bool = False,
    include_lineage: bool = False,
    session: requests.Session = Depends(get_openmetadata_client_dependency)
) -> DatabaseMetadataResponse:
    """
    Extract COMPLETE metadata for a database service using OpenMetadata API.
    
    This is a comprehensive consolidation API that provides:
    - Service metadata (configuration, owners, tags)
    - Database metadata (all databases in the service)
    - Schema metadata (all schemas in each database)
    - Table metadata (all tables with complete schema information)
    - Column metadata (data types, constraints, descriptions, tags)
    - Optional: Sample data, table profiles, and lineage information
    
    Uses multiple OpenMetadata REST API endpoints:
    - GET /services/databaseServices/name/{serviceName}
    - GET /databases?service={serviceName}
    - GET /databaseSchemas?database={databaseFQN}
    - GET /tables?databaseSchema={schemaFQN}
    - GET /tables/{tableId}?include=all (for detailed table info)
    
    Args:
        service_name: Name of the database service
        include_sample_data: Include sample data from tables (default: False)
        include_table_profiles: Include table profiling data (default: False)  
        include_lineage: Include data lineage information (default: False)
    """
    try:
        logger.info("Database metadata extraction requested", service_name=service_name)
        
        base_url = settings.openmetadata_url.rstrip('/') + '/api/v1'
        
        # Fetch service metadata
        service_data = _fetch_service_metadata(session, base_url, service_name)
        
        logger.info("Found database service",
                   service_id=service_data.get("id"),
                   service_type=service_data.get("serviceType"))
        
        # Create enhanced service model
        service_model = _create_service_model(service_data)
        
        # Fetch and process databases
        databases_list, summary_stats = _process_databases(
            session, base_url, service_name, include_sample_data, include_table_profiles, include_lineage
        )
        
        # Create comprehensive summary
        summary = _create_metadata_summary(service_model, databases_list, summary_stats)
        
        # Create final response
        response = DatabaseMetadataResponse(
            service=service_model,
            databases=databases_list,
            summary=summary
        )
        
        logger.info("Comprehensive metadata extraction completed",
                   service_name=service_name,
                   databases=summary_stats["total_databases"],
                   schemas=summary_stats["total_schemas"],
                   tables=summary_stats["total_tables"],
                   columns=summary_stats["total_columns"],
                   include_sample_data=include_sample_data,
                   include_table_profiles=include_table_profiles,
                   include_lineage=include_lineage)
        
        return response
        
    except HTTPException:
        raise
    except requests.HTTPError as http_error:
        logger.error("OpenMetadata API error during metadata extraction",
                    status_code=http_error.response.status_code if http_error.response else None,
                    service_name=service_name)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to extract metadata: {str(http_error)}"
        )
    except Exception as e:
        logger.error("Error extracting database metadata", exc_info=e, service_name=service_name)
        raise HTTPException(status_code=500, detail=f"Failed to extract metadata: {str(e)}")
 
 
@router.get("/service/{service_name}/tables", response_model=TablesResponse, tags=["OpenMetadata API"])
def get_service_tables(
    service_name: str,
    database_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    include_columns: bool = True,
    session: requests.Session = Depends(get_openmetadata_client_dependency)
) -> TablesResponse:
    """
    Get detailed table information for a service using OpenMetadata API.
    
    Uses OpenMetadata REST API: GET /tables
    
    Args:
        service_name: Name of the database service
        database_name: Optional database name filter
        schema_name: Optional schema name filter (requires database_name)
        include_columns: Whether to include column details (default: True)
    """
    try:
        logger.info("Table listing requested",
                   service_name=service_name,
                   database_name=database_name,
                   schema_name=schema_name)
        
        base_url = settings.openmetadata_url.rstrip('/') + '/api/v1'
        
        # Build filter params
        params = {}
        
        if database_name and schema_name:
            params["databaseSchema"] = f"{service_name}.{database_name}.{schema_name}"
        elif database_name:
            params["database"] = f"{service_name}.{database_name}"
        else:
            params["service"] = service_name
        
        tables_response = session.get(f"{base_url}/tables", params=params, timeout=30)
        tables_response.raise_for_status()
        
        tables_data = tables_response.json()
        tables = tables_data.get("data", [])
        
        # Create filter info
        filter_info = TableFilterInfo(
            database_name=database_name,
            schema_name=schema_name,
            include_columns=include_columns
        )
        
        tables_list = []
        
        if tables:
            for table in tables:
                # Create columns list if requested
                columns_list = []
                if include_columns and table.get("columns"):
                    columns_list = [
                        ColumnModel(
                            name=col.get("name", ""),
                            dataType=col.get("dataType"),
                            dataTypeDisplay=col.get("dataTypeDisplay"),
                            description=col.get("description"),
                            ordinalPosition=col.get("ordinalPosition"),
                            constraint=col.get("constraint"),
                            tags=[
                                TagModel(
                                    tagFQN=tag.get("tagFQN", ""),
                                    description=tag.get("description"),
                                    source=tag.get("source")
                                ) for tag in col.get("tags", [])
                            ]
                        ) for col in table.get("columns", [])
                    ]
                
                # Create table model
                table_model = TableModel(
                    id=table.get("id", ""),
                    name=table.get("name", ""),
                    fullyQualifiedName=table.get("fullyQualifiedName", ""),
                    tableType=table.get("tableType"),
                    description=table.get("description"),
                    displayName=table.get("displayName"),
                    owners=[
                        OwnerModel(
                            id=owner.get("id"),
                            name=owner.get("name", ""),
                            type=owner.get("type", ""),
                            fullyQualifiedName=owner.get("fullyQualifiedName")
                        ) for owner in table.get("owners", [])
                    ],
                    tags=[
                        TagModel(
                            tagFQN=tag.get("tagFQN", ""),
                            description=tag.get("description"),
                            source=tag.get("source")
                        ) for tag in table.get("tags", [])
                    ],
                    columns=columns_list,
                    column_count=len(table.get("columns", []))
                )
                
                tables_list.append(table_model)
        
        # Create response
        response = TablesResponse(
            service_name=service_name,
            filter=filter_info,
            tables=tables_list,
            count=len(tables_list)
        )
        
        logger.info("Table listing completed",
                   service_name=service_name,
                   table_count=len(tables_list))
        
        return response
        
    except requests.HTTPError as http_error:
        logger.error("OpenMetadata API error during table listing",
                    status_code=http_error.response.status_code if http_error.response else None,
                    service_name=service_name)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list tables: {str(http_error)}"
        )
    except Exception as e:
        logger.error("Error listing tables", exc_info=e, service_name=service_name)
        raise HTTPException(status_code=500, detail=f"Failed to list tables: {str(e)}")
 
 
@router.get("/health", response_model=dict, tags=["OpenMetadata API"])
def check_openmetadata_health(
    session: requests.Session = Depends(get_openmetadata_client_dependency)
) -> dict:
    """
    Check OpenMetadata connection health using direct API calls.
    
    Uses OpenMetadata REST API: GET /system/version
    """
    try:
        base_url = settings.openmetadata_url.rstrip('/') + '/api/v1'
        
        # Test connection health
        response = session.get(f"{base_url}/system/version", timeout=30)
        is_healthy = response.status_code == 200
        
        # Get server version if available
        server_info = {}
        if is_healthy:
            try:
                version_data = response.json()
                server_info = {
                    "version": version_data.get("version", "unknown"),
                    "connection_type": "Direct API",
                    "server_data": version_data
                }
            except Exception as version_error:
                logger.debug("Could not parse server version", exc_info=version_error)
                server_info = {
                    "version": "unknown",
                    "connection_type": "Direct API",
                    "version_error": str(version_error)
                }
        else:
            server_info = {
                "version": "unknown",
                "connection_type": "Direct API",
                "status_code": response.status_code,
                "response_text": response.text
            }
        
        return {
            "openmetadata_healthy": is_healthy,
            "api_loaded": True,
            "server_info": server_info,
            "message": "OpenMetadata API connection successful" if is_healthy else "OpenMetadata connection failed"
        }
        
    except Exception as e:
        logger.error("OpenMetadata health check failed", exc_info=e)
        return {
            "openmetadata_healthy": False,
            "api_loaded": True,
            "error": str(e),
            "message": "OpenMetadata API connection failed",
            "troubleshooting": {
                "check_server": "Ensure OpenMetadata server is running",
                "check_url": "Verify OPENMETADATA_URL environment variable",
                "check_token": "Verify OPENMETADATA_ACCESS_TOKEN is valid"
            }
        }