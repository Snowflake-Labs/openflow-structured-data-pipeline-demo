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

USE ROLE ACCOUNTADMIN;
CREATE WAREHOUSE IF NOT EXISTS <% SNOWFLAKE_WAREHOUSE %>  WAREHOUSE_SIZE = SMALL;
USE WAREHOUSE <% SNOWFLAKE_WAREHOUSE %>;

-- Create Role and Grant Ownership

CREATE ROLE IF NOT EXISTS <% SNOWFLAKE_ROLE %>;  

-- ability to execute tasks
GRANT EXECUTE TASK ON ACCOUNT TO ROLE <% SNOWFLAKE_ROLE %>;  
GRANT ROLE <% SNOWFLAKE_ROLE %> TO USER <% SNOWFLAKE_USER %>;
GRANT USAGE ON WAREHOUSE <% SNOWFLAKE_WAREHOUSE %> TO ROLE <% SNOWFLAKE_ROLE %>;

-- Demo Database and Schemas

CREATE DATABASE IF NOT EXISTS <% SNOWFLAKE_DATABASE %> 
  COMMENT = 'Snowflake Openflow Demos Database';

GRANT OWNERSHIP ON DATABASE <% SNOWFLAKE_DATABASE %> TO ROLE <% SNOWFLAKE_ROLE %>;

USE ROLE <% SNOWFLAKE_ROLE %>;

USE DATABASE <% SNOWFLAKE_DATABASE %> ;

CREATE SCHEMA IF NOT EXISTS METADATA;
CREATE SCHEMA IF NOT EXISTS DATA;
CREATE SCHEMA IF NOT EXISTS MODELS;

CREATE SCHEMA IF NOT EXISTS NETWORKS;
CREATE SCHEMA IF NOT EXISTS POLICIES;

USE SCHEMA METADATA;

CREATE SEQUENCE IF NOT EXISTS SEQ_SCHEMA_VERSION START = 1 INCREMENT = 1;

CREATE TABLE IF NOT EXISTS METADATA.SCHEMA_REGISTRY (
    TABLE_NAME VARCHAR(100) NOT NULL, -- Iceberg table name
    TABLE_NAMESPACE VARCHAR(100) NOT NULL, -- Iceberg table namespace
    
    AVRO_SCHEMA TEXT NOT NULL, -- avro schema
    SCHEMA_ANALYSIS TEXT NOT NULL, -- Cortex schema analysis
    SCHEMA_VERSION INTEGER NOT NULL DEFAULT METADATA.SEQ_SCHEMA_VERSION.NEXTVAL, -- schema version
    
    IS_READY BOOLEAN NOT NULL DEFAULT FALSE, -- is ready
    STATUS VARCHAR(20) NOT NULL DEFAULT 'DRAFT', -- 'DRAFT', 'ACTIVE', 'DEPRECATED'

    BASELINE_SOURCE STRING,        -- Original file that established this table
    LAST_ANALYSIS_SOURCE STRING,   -- Most recent file processed
    
    CREATED_AT TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    
    PRIMARY KEY (TABLE_NAME, TABLE_NAMESPACE, SCHEMA_VERSION)
);

-- Access and Grants 
grant usage on database <% SNOWFLAKE_DATABASE %> to role <% SNOWFLAKE_ROLE2 %>;
grant usage on schema <% SNOWFLAKE_DATABASE %>.metadata to role <% SNOWFLAKE_ROLE2 %>;
grant role <% SNOWFLAKE_ROLE %> to role <% SNOWFLAKE_ROLE2 %>;