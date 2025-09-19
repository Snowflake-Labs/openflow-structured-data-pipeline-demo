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