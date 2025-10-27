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

WITH schema_mapping_analysis AS (
    SELECT 
        table_name,
        table_namespace,
        avro_schema,
        is_ready as ingestion_ok,
        status as schema_status,
        AI_COMPLETE(
            model => 'claude-4-sonnet', 
            prompt => CONCAT(
               'You are an Avro schema adaptation expert. Your task is to modify the given Avro schema to accommodate the provided CSV data structure.',
                '\n\n## Semantic Field Mapping Reference:\n',
                'Use this comprehensive table for semantic field equivalence detection:\n',
                '\n| Standard Field | Semantic Equivalents | Expected Type | Context |\n',
                '|---------------|---------------------|---------------|----------|\n',
                '| event_id | show_id, slot_number, performance_id, id | long | Primary identifier |\n',
                '| artist_name | dj_performer, performer, headliner, act_name, artist | string | Performer information |\n',
                '| stage | main_stage, venue_section, location, venue_area, stage_location, venue | string | Performance location |\n',
                '| event_datetime | start_time, time_block, date_time, time_slot, show_time, datetime | string | Event timing |\n',
                '| ticket_price | vip_price, admission_cost, ticket_cost, entry_fee, price_tier, price, cost | double | Pricing information |\n',
                '| festival_name | event_name, festival, venue_name | string | Event organization |\n',
                '| genre | music_genre, category, style, type | string | Classification |\n',
                '| capacity | max_attendance, venue_capacity, max_capacity | long | Venue limits |\n',
                '| city | location_city, venue_city | string | Geographic location |\n',
                '| sponsor | sponsorship, partner, brand | string | Commercial partnerships |\n',
                '| ticket_type | pass_type, admission_type, entry_type, ticket_category | string | Ticket classification |\n',
                '| quantity | qty, count, number_of_tickets, ticket_count | long | Purchase quantity |\n',
                '| unit_price | price_per_ticket, individual_price, single_price | double | Per-unit pricing |\n',
                '| total_amount | total_price, final_amount, grand_total, amount_paid | double | Total transaction value |\n',
                '| purchase_date | order_date, transaction_date, sale_date, bought_date | string | Transaction timestamp |\n',
                '| payment_method | payment_type, pay_method, payment_mode, transaction_type | string | Payment processing method |\n',
                '\n## Semantic Mapping Rules:\n',
                '1. **Semantic Matching**: Map based on field purpose using the comprehensive mapping table above\n',
                '2. **Exact Name Matching**: First check for exact case-insensitive name matches\n',
                '3. **Semantic Equivalence**: Use the mapping table to identify equivalent fields\n',
                '4. **Data Type Compatibility**: Ensure CSV data can convert to target Avro type\n',
                '5. **Required Fields**: Prioritize mapping to required schema fields\n',
                '6. **Perfect Match Optimization**: If CSV column names exactly match existing schema field names (same count, same names, compatible types), return the original schema unchanged\n',
                '\n## Schema Adaptation Instructions:\n',
                '- **FIRST**: Check if CSV column names exactly match schema field names with compatible types. If perfect name match, return original schema unchanged.\n',
                '- Analyze CSV headers and data types using semantic understanding\n',
                '- For CSV columns semantically equivalent to existing schema fields: ADD the CSV column name to the "aliases" array of the existing field (do NOT create separate fields)\n',
                '- Example: CSV "show_id" + schema "event_id" → keep "event_id", add "show_id" to aliases array\n',
                '- Use the comprehensive semantic mapping table above for all field equivalence detection\n',
                '- Common semantic mappings: artist_name/performer/headliner, stage/venue/location, ticket_price/cost/fee, event_datetime/time/schedule\n',
                '- ADD completely new fields only for CSV columns with no semantic equivalent in existing schema\n',
                '- KEEP existing schema fields that don''t appear in CSV but make them optional (union with null)\n',
                '- Infer appropriate Avro data types from CSV sample data with type compatibility validation\n',
                '- **IMPORTANT**: Do NOT include "doc" fields in the Avro schema as this schema is used only for CSV parsing, not code generation\n',
                '- Return ONLY the adapted Avro schema in JSON format\n',
                '- DO NOT wrap the response in markdown code blocks\n',
                '- DO NOT use triple backticks (```) in your response\n',
                '- Return raw JSON only, starting with { and ending with }\n\n',
                '## Input Data:\n',
                '**Current Avro Schema:**\n',
                '<schema>', avro_schema, '</schema>\n\n',
                '**CSV Data:**\n',
                '\n\n**CSV Headers:** ${csv.headers}',
                '\n**CSV Sample:** ${csv.sample.rows}',
                '\n\n',
                '## Output:\n',
                'Return ONLY raw JSON without any markdown formatting, code blocks, or explanatory text. Start directly with the opening brace { of the JSON schema.'
            ),
            model_parameters => {
                'temperature': 0.1
            }
        ) as adapted_avro_schema
    FROM SCHEMA_REGISTRY
)
SELECT 
   avro_schema,
   BASE64_ENCODE(PARSE_JSON(adapted_avro_schema)) as adapted_avro_schema,
   schema_status,
   ingestion_ok as is_ready
FROM schema_mapping_analysis
where
    table_name = '${table.name}'
AND
    table_namespace = '${table.namespace}';