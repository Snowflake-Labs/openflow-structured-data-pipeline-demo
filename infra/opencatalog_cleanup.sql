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

-- cleanup 
use role {{.SNOWFLAKE_ROLE}};
use database {{.SNOWFLAKE_MUSIC_FLOW_DATABASE}};


-- clear table and schema
use schema "events";
truncate table if exists "music_events";
drop table if exists "music_events";
drop schema if exists "events";

drop database {{.SNOWFLAKE_MUSIC_FLOW_DATABASE}};

drop external volume {{.USER}}_openflow_music_flow;
drop catalog integration music_flow_music_events_rw;

-- clean up stage
rm @{{.USER}}_openflow_demos.data.sources;
ls @{{.USER}}_openflow_demos.data.sources;
alter stage {{.USER}}_openflow_demos.data.sources refresh;