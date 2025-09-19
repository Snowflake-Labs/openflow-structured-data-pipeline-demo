UPDATE METADATA.SCHEMA_REGISTRY 
SET 
    SCHEMA_ANALYSIS = '${schema.analysis}',
    LAST_ANALYSIS_SOURCE = '${google.drive.file.path}',
    UPDATED_AT = CURRENT_TIMESTAMP()
WHERE 
    TABLE_NAME = '${table.name}' 
    AND TABLE_NAMESPACE = '${table.namespace}'