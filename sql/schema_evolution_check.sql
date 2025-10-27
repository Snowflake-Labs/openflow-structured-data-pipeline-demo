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

WITH ai_analysis AS (
    SELECT 
        table_name,
        table_namespace,
        avro_schema::string as schema_json,
        AI_COMPLETE(
            model => 'claude-4-sonnet', 
            prompt => CONCAT(
                'You are a schema analysis expert. Analyze if the given Avro schema semantically matches CSV data.',
                '\n\n## Task',
                '\nDetermine if this Base64 Encoded Avro schema can accommodate the provided CSV structure.',
                '\n\n## Input Data',
                '\n**Avro Schema:**\n<schema>', AVRO_SCHEMA, '</schema>',
                '\n\n**CSV Headers:** ${csv.headers}',
                '\n**CSV Sample:** ${csv.sample.rows}',
                '\n\n## Analysis Rules',
                '\n1. **Semantic Matching**: If majority of CSV fields (>50%) can map to existing schema fields, consider it a match',
                '\n2. **Schema Evolution**: Required only if CSV has new fields that cannot map to existing schema fields',
                '\n3. **Match Priority**: If no semantic match exists, skip evolution analysis',
                '\n\n## Required Response Format',
                '\nRespond with ONLY a JSON object (no markdown, no explanations):',
                '\n{',
                '\n  "matched": "yes|no",',
                '\n  "schema_evolution_required": "yes|no",',
                '\n  "schemas_analysis": ["bullet point 1", "bullet point 2"]',
                '\n}',
                '\n\n## Analysis Guidelines',
                '\n- Focus on field purpose rather than exact naming',
                '\n- Consider common field variations (id/identifier, name/title, etc.)',
                '\n- Evolution needed only for genuinely new data concepts',
                '\n- Keep analysis points concise and technical'
            ),
           model_parameters => {
                'temperature': 0.1
            },
            response_format => {
                 'type': 'json',
                 'schema': {
                     'type': 'object',
                     'properties': {
                         'matched': {
                             'type': 'string',
                             'enum': ['yes', 'no']
                         },
                         'schema_evolution_required': {
                             'type': 'string',
                             'enum': ['yes', 'no']
                         },
                         'schemas_analysis': {
                             'type': 'array',
                             'items': {
                                 'type': 'string'
                             }
                         }
                     },
                     'required': ['matched', 'schema_evolution_required', 'schemas_analysis'],
                     'additionalProperties': false
                 }
            }
        ) as ai_response
    FROM METADATA.SCHEMA_REGISTRY
)
SELECT 
    table_name,
    table_namespace,
    schema_json,
    BASE64_ENCODE(ai_response) as ai_response
FROM ai_analysis
WHERE PARSE_JSON(ai_response):matched::string = 'yes'