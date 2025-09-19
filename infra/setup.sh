#!/usr/bin/env bash

set -euo pipefail 

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Push the script directory onto the directory stack
pushd "$SCRIPT_DIR" > /dev/null
SQL_DIR="${SCRIPT_DIR}"
SNOWFLAKE_ROLE2=$(snow sql -q 'select current_role() as CURRENT_ROLE' --format=json  | jq -r '.[0].CURRENT_ROLE')
snow sql --filename "${SQL_DIR}/setup.sql" \
  --variable SNOWFLAKE_DATABASE="${SNOWFLAKE_OPENFLOW_DEMO_DATABASE}" \
  --variable SNOWFLAKE_WAREHOUSE="${SNOWFLAKE_OPENFLOW_DEMO_WAREHOUSE}" \
  --variable SNOWFLAKE_ROLE="${SNOWFLAKE_OPENFLOW_DEMO_ROLE}" \
  --variable SNOWFLAKE_USER="${SNOWFLAKE_USER}" \
  --variable SNOWFLAKE_ROLE2="${SNOWFLAKE_ROLE2}"
