#!/usr/bin/env bash

# Copyright 2025 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
