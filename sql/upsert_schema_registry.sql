-- Copyright 2025 Snowflake Inc.
-- SPDX-License-Identifier: Apache-2.0
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

MERGE INTO METADATA.SCHEMA_REGISTRY AS target
USING (
    SELECT 
        '${table.name}' AS table_name,
        '${table.namespace}' AS table_namespace,
        '${avro.schema.content}' AS avro_schema,
        '${schema.analysis}' AS schema_analysis,
        '${google.drive.file.path}' AS schema_source,
        CURRENT_TIMESTAMP() AS updated_at
) AS source
ON target.TABLE_NAME = source.table_name 
   AND target.TABLE_NAMESPACE = source.table_namespace

WHEN MATCHED THEN
    UPDATE SET
        AVRO_SCHEMA = source.avro_schema,
        SCHEMA_ANALYSIS = source.schema_analysis,
        SCHEMA_VERSION = METADATA.SEQ_SCHEMA_VERSION.NEXTVAL,
        IS_READY = FALSE,
        STATUS = 'ACTIVE',
        LAST_ANALYSIS_SOURCE = source.schema_source,
        UPDATED_AT = source.updated_at

WHEN NOT MATCHED THEN
    INSERT (
        TABLE_NAME,
        TABLE_NAMESPACE,
        AVRO_SCHEMA,
        SCHEMA_ANALYSIS,
        SCHEMA_VERSION,
        IS_READY,
        STATUS,
        BASELINE_SOURCE,
        LAST_ANALYSIS_SOURCE,
        CREATED_AT,
        UPDATED_AT
    )
    VALUES (
        source.table_name, -- table name    
        source.table_namespace, -- table namespace
        source.avro_schema, -- avro schema
        source.schema_analysis, -- schema analysis
        METADATA.SEQ_SCHEMA_VERSION.NEXTVAL, -- schema version
        FALSE, -- is ready
        'ACTIVE', -- status
        source.schema_source, -- baseline source
        source.schema_source, -- last analysis source
        CURRENT_TIMESTAMP(), -- created at
        source.updated_at -- updated at
    );