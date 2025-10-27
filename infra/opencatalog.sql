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

use role <% SNOWFLAKE_ROLE %>;

-- External Volume
CREATE EXTERNAL VOLUME IF NOT EXISTS <%ctx.env.USER%>_openflow_music_flow
  STORAGE_LOCATIONS =
      (
        (
            NAME = '<%ctx.env.USER%>-openflow-music-flow-<%ctx.env.OPENCATALOG_STORAGE_AWS_REGION%>'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://<% ctx.env.OPENCATALOG_STORAGE_BUCKET_NAME %>'
            STORAGE_AWS_ROLE_ARN = '<% ctx.env.OPENCATALOG_STORAGE_AWS_ROLE_ARN %>'
            STORAGE_AWS_EXTERNAL_ID ='<% ctx.env.OPENCATALOG_STORAGE_AWS_EXTERNAL_ID %>'
        )
      )
  ALLOW_WRITES = TRUE;

CREATE CATALOG INTEGRATION IF NOT EXISTS music_flow_demo_catalog_rw
  CATALOG_SOURCE = POLARIS
  TABLE_FORMAT = ICEBERG
  CATALOG_NAMESPACE = '<% ctx.env.OPENCATALOG_CATALOG_NAMESPACE %>'
  REST_CONFIG = (
    CATALOG_URI = '<% ctx.env.OPENCATALOG_API_URL %>/polaris/api/catalog'
    CATALOG_NAME = '<% ctx.env.OPENCATALOG_CATALOG_NAME %>'
  )
  REST_AUTHENTICATION = (
    TYPE = OAUTH
    OAUTH_CLIENT_ID = '<% OPENCATALOG_CLIENT_ID %>'
    OAUTH_CLIENT_SECRET = '<% OPENCATALOG_CLIENT_SECRET %>'
    OAUTH_ALLOWED_SCOPES = ('<% ALLOWED_SCOPES %>')
  )
  ENABLED = TRUE;

-- create database with linked catalog to OpenCatalog
CREATE DATABASE IF NOT EXISTS <% ctx.env.SNOWFLAKE_MUSIC_FLOW_DATABASE %>
  LINKED_CATALOG = ( 
      CATALOG = 'music_flow_demo_catalog_rw' , 
      -- these are the namespaces that is used for the demo, add more if needed
      ALLOWED_NAMESPACES=('events','crm')
      )
  EXTERNAL_VOLUME=<%ctx.env.USER%>_openflow_music_flow;

-- grants and role assignments
grant usage on integration music_flow_demo_catalog_rw to role <% SNOWFLAKE_ROLE2 %>;
grant usage on external volume <%ctx.env.USER%>_openflow_music_flow to role <% SNOWFLAKE_ROLE2 %>;
grant usage on database <% ctx.env.SNOWFLAKE_MUSIC_FLOW_DATABASE %> to role <% SNOWFLAKE_ROLE2 %>;
-- add update other grants if needed
