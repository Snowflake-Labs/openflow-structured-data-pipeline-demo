## User Message

Please analyze the following CSV file fetched from Google Drive and generate metadata, Avro schema, and Snowflake SQL DDL with Jinja2 templating support:

### File Information

- **Google Drive File:** `${filename}`
- **File size:** `${file.size}` bytes
- **Source:** Google Drive via FetchGoogleDriveFile processor

These NiFi variables will be substituted before sending to the LLM.

### CSV Structure

#### Headers

```csv
${csv.headers}
```

#### Sample Data (first 5 rows)

```csv
${csv.sample.data}
```

Note: These variables (`${csv.headers}` and `${csv.sample.data}`) will be populated by NiFi before sending to the LLM.

### Schema State Context

- **Is first time processing:** `${is.first.time}`
- **Schema evolution required:** `${schema.evolution.required}`
- **Existing schema (for evolution):** `${existing.schema}`
- **Schema Analysis Extra Info (for evolution):** `${schema.analysis}`
- **Table name (for evolution):** `${table.name}`
- **Table namespace (for evolution):** `${table.namespace}`

These variables should be used directly in the generated Snowflake SQL DDL with Jinja2 templating for dynamic behavior across different environments.

### Schema Evolution Requirements

When `${schema.evolution.required}` is "yes":

1. **Semantic Field Matching**: Use the semantic field mapping table to identify equivalent fields between existing schema and new CSV headers
2. **Evolution Analysis**: Generate detailed field mapping analysis including:
   - Exact name matches (case-insensitive)
   - Semantic equivalent matches (e.g., "artist_name" ↔ "performer")
   - New fields requiring addition
   - Type changes requiring evolution
3. **Evolved Schema Generation**: Create complete Avro schema containing ALL fields (existing + new/changed fields) with all new fields marked as nullable for backward compatibility
4. **Compatibility Preservation**: Ensure all evolution changes maintain backward compatibility

### Semantic Field Mapping Reference

Use this table for semantic field equivalence detection:

| Standard Field | Semantic Equivalents | Expected Type | Context |
|---------------|---------------------|---------------|---------|
| event_id | show_id, slot_number, performance_id, id | long | Primary identifier |
| artist_name | dj_performer, performer, headliner, act_name, artist | string | Performer information |
| stage | main_stage, venue_section, location, venue_area, stage_location, venue | string | Performance location |
| event_datetime | start_time, time_block, date_time, time_slot, show_time, datetime | string | Event timing |
| ticket_price | vip_price, admission_cost, ticket_cost, entry_fee, price_tier, price, cost | double | Pricing information |
| festival_name | event_name, festival, venue_name | string | Event organization |
| genre | music_genre, category, style, type | string | Classification |
| capacity | max_attendance, venue_capacity, max_capacity | long | Venue limits |
| city | location_city, venue_city | string | Geographic location |
| sponsor | sponsorship, partner, brand | string | Commercial partnerships |
| ticket_type | pass_type, admission_type, entry_type, ticket_category | string | Ticket classification |
| quantity | qty, count, number_of_tickets, ticket_count | long | Purchase quantity |
| unit_price | price_per_ticket, individual_price, single_price | double | Per-unit pricing |
| total_amount | total_price, final_amount, grand_total, amount_paid | double | Total transaction value |
| purchase_date | order_date, transaction_date, sale_date, bought_date | string | Transaction timestamp |
| payment_method | payment_type, pay_method, payment_mode, transaction_type | string | Payment processing method |

### Pipeline Context

- This CSV will be processed through Apache NiFi data pipeline
- **Target:** Apache Iceberg table for analytics
- **Processing pattern:** Real-time ingestion with schema evolution
- **Evolution strategy:** Additive changes only (maintain backward compatibility)

### Processing Logic

#### For First-Time Processing (`${is.first.time}` = "yes")

1. Generate complete Avro schema for all CSV fields
2. Create Snowflake SQL DDL for schema and table creation with error handling
3. Include comprehensive field analysis in `schema_analysis`
4. Set `evolution_required: false` in schema analysis
5. Generate `CREATE SCHEMA IF NOT EXISTS` statement
6. Generate `CREATE ICEBERG TABLE IF NOT EXISTS` statement

#### For Schema Evolution (`${schema.evolution.required}` = "yes")

1. Parse `${existing.schema}` to understand current table structure and use `${table.name}` and `${table.namespace}` for target table identification
2. **CRITICAL**: Perform semantic field matching between CSV headers and existing table columns using the semantic mapping table
3. **CRITICAL**: Identify which CSV fields are semantic equivalents of existing columns (DO NOT add these as new columns)
4. **CRITICAL**: Identify which CSV fields have NO semantic equivalent in existing schema (ADD ONLY these as new columns)
5. Generate complete Avro schema containing ALL fields (existing + truly new fields) with all new fields marked as nullable
6. Create Snowflake SQL DDL for `ALTER ICEBERG TABLE` operations that add ONLY the truly new fields (not semantic equivalents)
7. Include detailed field mapping analysis in `schema_analysis` showing which fields were matched vs. which were added

#### For Standard Processing (`${schema.evolution.required}` = "no")

1. Generate table validation queries only
2. Set `evolution_required: false` in schema analysis

### Snowflake Iceberg SQL Requirements

- Use `ALTER ICEBERG TABLE ... ADD COLUMN` for schema evolution with Snowflake data types (BIGINT, DOUBLE, STRING, etc.)
- Chain multiple `ADD COLUMN` clauses for multiple new fields
- Always make new fields nullable by default (no `NOT NULL` constraint)
- Use proper Snowflake data types for all schema operations
- **CRITICAL**: Use Snowflake data types (BIGINT, DOUBLE, STRING, etc.) for all schema operations

### Snowflake Iceberg Schema Requirements

**MANDATORY**: All table creation and schema evolution must use Snowflake Iceberg syntax with Jinja2 templating:

- **Jinja2 Directive**: Always start with `--!jinja` for dynamic SQL execution
- **Environment Variables**: Use `{{ variable_name }}` syntax ONLY for database context switching (USE DATABASE statements)
- **Schema Creation**: Use `CREATE SCHEMA IF NOT EXISTS` with inferred schema names
- **External REST Catalog**: ALWAYS use double quotes around schema, table, and column names for external REST catalog compatibility
- **Native Snowflake Tables**: Do NOT use double quotes for native Snowflake tables (like metadata.schema_registry)
- **Table Type Identification**:
  - **User-created tables/schemas** → External REST Catalog → Use double quotes
  - **System tables** (metadata.schema_registry) → Native Snowflake → No double quotes
- **Table Creation**: Use `CREATE ICEBERG TABLE IF NOT EXISTS` with inferred table names and schemas
- **Schema Evolution**: Use `ALTER ICEBERG TABLE` with inferred table names and schemas
- **Database Context**: Use `USE DATABASE {{ music_flow_demo_db }}` and `USE DATABASE {{ music_flow_system_db }}` for environment switching
- **Type Mapping**:
  - Text fields → `STRING`
  - Integer/ID fields → `BIGINT`
  - Decimal fields → `DOUBLE`
  - Boolean fields → `BOOLEAN`
  - Date fields → `DATE`
  - Timestamp fields → `TIMESTAMP`

**Example Schema and Table Creation with Jinja2 Templating**:

```sql
--!jinja
USE DATABASE {{ music_flow_demo_db }};

CREATE SCHEMA IF NOT EXISTS "events";

CREATE ICEBERG TABLE IF NOT EXISTS "events"."music_events" (
    "event_id" BIGINT NOT NULL,
    "artist_name" STRING,
    "ticket_price" DOUBLE
);

USE DATABASE {{ music_flow_system_db }};

-- Update schema registry to mark table as ready for ingestion
UPDATE metadata.schema_registry
SET IS_READY = TRUE
WHERE table_name = 'music_events'
  AND table_namespace = 'events';
```

**Example Schema Evolution with Jinja2 Templating**:

```sql
--!jinja
USE DATABASE {{ music_flow_demo_db }};

ALTER ICEBERG TABLE "events"."music_events" 
ADD COLUMN "genre" STRING,
    COLUMN "sponsor" STRING;

USE DATABASE {{ music_flow_system_db }};

-- Update schema registry to mark table as ready for ingestion
UPDATE metadata.schema_registry
SET IS_READY = TRUE
WHERE table_name = 'music_events'
  AND table_namespace = 'events';
```

### Snowflake SQL Requirements

**MANDATORY**: Always use proper Snowflake Iceberg syntax with Jinja2 templating for all table operations:

- **Jinja2 Directive**: Always start with `--!jinja` for dynamic SQL execution
- **Environment Variables**: Use `{{ variable_name }}` syntax ONLY for database context switching (USE DATABASE statements)
- **Database Context**: Use `USE DATABASE {{ music_flow_demo_db }}` and `USE DATABASE {{ music_flow_system_db }}` for environment switching
- **External REST Catalog**: ALWAYS use double quotes around schema, table, and column names for external REST catalog compatibility
- **Native Snowflake Tables**: Do NOT use double quotes for native Snowflake tables (like metadata.schema_registry)
- **Table Type Identification**:
  - **User-created tables/schemas** → External REST Catalog → Use double quotes
  - **System tables** (metadata.schema_registry) → Native Snowflake → No double quotes
- Use `CREATE SCHEMA IF NOT EXISTS` with inferred schema names before table creation
- Use `CREATE ICEBERG TABLE IF NOT EXISTS` with inferred table names and schemas for table creation
- Use `ALTER ICEBERG TABLE` with inferred table names and schemas for schema evolution
- Use proper Snowflake data types (BIGINT, DOUBLE, STRING, etc.)
- Include proper error handling with `IF NOT EXISTS` clauses

### SQL Documentation Requirements

**MANDATORY**: Include comprehensive documentation in generated SQL based on Avro schema:

- **Table Comments**: Use `COMMENT 'text'` clause on table creation to describe the table's purpose
- **Column Comments**: Use `COMMENT 'text'` clause on each column to describe its business meaning  
- **Inline Comments**: Use `--` comments to explain complex logic or business rules
- **Schema Comments**: Include comments explaining the data source and processing context
- **Snowflake Syntax**: Use proper Snowflake COMMENT syntax with single quotes around comment text
- **SQL Keywords**: Use uppercase for ALL SQL keywords (CREATE, TABLE, SCHEMA, COLUMN, ALTER, ADD, COMMENT, SELECT, FROM, WHERE, etc.)

**Documentation Mapping from Avro Schema**:

- Extract `doc` field from Avro schema fields and use as column comments
- Use Avro schema `name` and `namespace` to generate table description
- Include business context from semantic field mapping table
- Add processing metadata (source file, ingestion date, etc.)

**Example Documentation Pattern with Jinja2 Templating**:

```sql
--!jinja
USE DATABASE {{ music_flow_demo_db }};

-- Create schema for music events data
CREATE SCHEMA IF NOT EXISTS {{ table_namespace }};

-- Music events table containing festival and concert information
-- Source: CSV files from various music festivals
CREATE ICEBERG TABLE IF NOT EXISTS {{ table_namespace }}.{{ table_name }} (
    event_id BIGINT NOT NULL COMMENT 'Primary identifier for the event',
    artist_name STRING COMMENT 'Name of the performing artist or DJ',
    stage STRING COMMENT 'Stage or venue area where performance occurs',
    event_datetime STRING COMMENT 'Date and time of the performance',
    ticket_price DOUBLE COMMENT 'Price of the ticket in USD'
) COMMENT = 'Music events table containing festival and concert information from multiple sources';

USE DATABASE {{ music_flow_system_db }};

-- Update schema registry to mark table as ready for ingestion
UPDATE metadata.schema_registry
SET IS_READY = TRUE
WHERE table_name = '{{ table_name }}'
  AND table_namespace = '{{ table_namespace }}';
```

**CRITICAL**: Always use proper Snowflake COMMENT syntax:

- Column comments: `column_name DATA_TYPE COMMENT 'comment text'`
- Table comments: `) COMMENT = 'table comment text';`
- Use single quotes around all comment text
- Comments are stored as metadata and visible in DESCRIBE TABLE

### Requirements

#### Schema Generation

1. Generate a complete Avro schema suitable for Apache NiFi Record processors
2. Use "long" for all integer/ID fields (for scalability)
3. Use "double" for all decimal/numeric fields (for precision)
4. Make fields nullable unless clearly required (primary keys)
5. Use descriptive field names in snake_case format
6. Include field documentation for business context
7. **CRITICAL**: When evolution required, generate complete schema with ALL fields (existing + new), ensuring all new fields are nullable
8. Include semantic field mapping in schema documentation

#### Code Generation

1. Generate clean Snowflake SQL DDL that handles CREATE, ALTER, and LOAD scenarios based on context
2. SQL code should include proper schema creation and table creation statements
3. **MANDATORY**: Use proper Snowflake Iceberg syntax throughout
4. **CRITICAL**: Use proper Snowflake data types (BIGINT, DOUBLE, STRING, etc.)
5. Generate code that's ready to execute in Snowflake with proper formatting
6. Handle CREATE (first time), ALTER (schema evolution), and LOAD (standard) scenarios dynamically
7. **MANDATORY**: Include table and column comments based on Avro schema documentation using proper Snowflake COMMENT syntax
8. Include proper error handling with `IF NOT EXISTS` clauses
9. **CRITICAL**: Use the inferred metadata values directly in the generated SQL
10. Include schema comparison logic when existing schema context is provided
11. Generate appropriate Snowflake Iceberg operations using proper data types based on detected schema differences
12. Include semantic field matching logic for evolution detection
13. **CRITICAL**: When schema evolution is required, use semantic matching to identify equivalent fields between CSV and existing table
14. **CRITICAL**: DO NOT add new columns for CSV fields that are semantic equivalents of existing table columns
15. **CRITICAL**: Only add columns for CSV fields that have NO semantic match in the existing schema
16. **CRITICAL**: The ingestion layer (Avro schema with aliases) handles mapping semantic equivalents, NOT the schema evolution
17. **CRITICAL**: Example Evolution Scenario:
    - Existing table columns: event_id, artist_name, main_stage, start_time, vip_price
    - CSV columns: event_id, artist_name, stage, start_time, ticket_price, genre, sponsor
    - Semantic analysis using mapping table:
      - "stage" ↔ "main_stage" (semantic equivalent per mapping table - DO NOT ADD)
      - "ticket_price" ↔ "vip_price" (semantic equivalent per mapping table - DO NOT ADD)
      - "genre" (no equivalent in existing schema - ADD THIS)
      - "sponsor" (no equivalent in existing schema - ADD THIS)
    - Generated SQL: ALTER ICEBERG TABLE ADD COLUMN "genre" STRING, COLUMN "sponsor" STRING
    - Avro adapter will map "stage" → "main_stage" and "ticket_price" → "vip_price" during ingestion
18. **CRITICAL**: When generating ALTER TABLE logic, only add truly new fields to existing table - do not recreate or modify existing fields
19. **MANDATORY**: Always include `CREATE SCHEMA IF NOT EXISTS` before table creation
20. **MANDATORY**: Include simple schema registry updates to mark tables as ready for ingestion (only set IS_READY = TRUE)

#### Schema Analysis Generation

1. Generate comprehensive `schema_analysis` with evolution details
2. Include field mapping analysis with exact and semantic matches
3. **CRITICAL**: In semantic_matches, show CSV fields that map to existing table columns (these will NOT be added as new columns)
4. **CRITICAL**: In new_fields, show ONLY CSV fields that have NO semantic equivalent in existing schema (these WILL be added as new columns)
5. Identify new fields requiring addition to existing schema (only truly new fields without semantic equivalents)
6. Detect type changes that require schema evolution
7. Provide evolution strategy recommendations
8. Include compatibility notes for backward compatibility assurance
9. **CRITICAL**: Example schema_analysis for evolution:

   ```json
   {
     "evolution_required": true,
     "field_mappings": {
       "exact_matches": ["event_id", "artist_name", "start_time"],
       "semantic_matches": {
         "stage": "main_stage",
         "ticket_price": "vip_price"
       },
       "new_fields": ["genre", "sponsor"]
     },
     "evolution_strategy": "ADD_FIELDS",
     "compatibility_notes": ["Only genre and sponsor will be added as new columns", "stage maps to existing main_stage", "ticket_price maps to existing vip_price"]
   }
   ```

### Critical Instructions

- **MANDATORY**: Provide ONLY a JSON object with `inferred_metadata`, `avro_schema`, `code`, and `schema_analysis` properties
- **MANDATORY**: Use proper JSON string escaping for newlines (`\n`) and quotes (`\"`)
- **MANDATORY**: Ensure the response is valid JSON that can be parsed directly with JSON.loads()
- **MANDATORY**: Both schema and code must be immediately usable after extraction from JSON with proper formatting preserved
- **MANDATORY**: No markdown formatting, explanatory text, or conversational language anywhere
- **MANDATORY**: Start response immediately with `{` - no introductory text
- **MANDATORY**: End response immediately with `}` - no concluding text
- **MANDATORY**: Use proper JSON syntax with double quotes for all keys and string values
- **MANDATORY**: The avro_schema value must be JSON format (not YAML) converted to a string
- **MANDATORY**: Preserve SQL indentation using `\n` patterns in the JSON string
- When `${schema.evolution.required}` = "yes", generate complete schema with ALL fields (existing + new), ensuring all new fields are nullable
- Include comprehensive field mapping analysis in `schema_analysis` output
- Generate evolution-specific Snowflake SQL DDL with ALTER TABLE logic when required
- **CRITICAL**: When `${schema.evolution.required}` = "yes", generate complete Avro schema with ALL fields (existing + new), making all new fields nullable
- **CRITICAL**: SQL code should use ALTER ICEBERG TABLE operations that add only new fields, preserving existing table structure and data
- **CRITICAL**: Always use Snowflake data types (BIGINT, DOUBLE, STRING, etc.) for all schema operations in generated SQL
- **CRITICAL**: Always use proper Snowflake COMMENT syntax with single quotes around comment text
- **CRITICAL**: Always use uppercase for ALL SQL keywords (CREATE, TABLE, SCHEMA, COLUMN, ALTER, ADD, COMMENT, SELECT, FROM, WHERE, etc.)
- **CRITICAL**: For ALTER ICEBERG TABLE with multiple columns: use `ADD COLUMN col1 ..., COLUMN col2 ...` NOT `ADD COLUMN col1 ..., ADD COLUMN col2 ...`

### Context Variable Usage

Use these NiFi flow variables exactly as provided:

- `${is.first.time}` - Boolean indicating first-time table creation
- `${schema.evolution.required}` - Boolean indicating if schema evolution is needed
- `${existing.schema}` - JSON string of existing Avro schema (empty if first time)
- `${csv.headers}` - Comma-separated list of CSV column headers
- `${csv.sample.data}` - Sample CSV rows for data type inference
- `${table.name}` - Target table name (used during evolution)
- `${table.namespace}` - Target table namespace (used during evolution)

### Evolution Decision Matrix

| is.first.time | schema.evolution.required | Action | Schema Content | SQL Logic | Table Naming | Success Logging |
|---------------|---------------------------|--------|----------------|-----------|----------------|------------------|
| true | false | CREATE | Complete schema | CREATE SCHEMA + CREATE ICEBERG TABLE | Infer from CSV | Include success logging |
| false | true | EVOLVE | Complete schema (existing + new nullable fields) | ALTER ICEBERG TABLE | Use ${table.name}/${table.namespace} | Include success logging |
| false | false | LOAD | N/A (use existing) | Table validation queries | Use ${table.name}/${table.namespace} | Not needed |

### Forbidden Content and Formatting

- **FORBIDDEN**: Any references to YAML format anywhere
- **FORBIDDEN**: Any markdown syntax or code blocks
- **FORBIDDEN**: Any explanatory text outside the JSON structure
- **FORBIDDEN**: Any conversational language
- **FORBIDDEN**: Any text before `{` or after `}`
- **FORBIDDEN**: Invalid JSON syntax
- **FORBIDDEN**: Do not use single quotes in JSON - must use double quotes only
- **FORBIDDEN**: Do not exceed 120 characters per line in SQL code (when extracted)
- **FORBIDDEN**: Do not use double quotes for string literals - use single quotes
- **FORBIDDEN**: Using NiFi variable placeholders in generated code - only use actual inferred values
- **FORBIDDEN**: Using any non-SQL syntax anywhere - always use Snowflake SQL syntax
- **FORBIDDEN**: Using double quotes in COMMENT clauses - always use single quotes: `COMMENT 'text'` not `COMMENT "text"`
- **FORBIDDEN**: Using lowercase SQL keywords - always use uppercase: `CREATE` not `create`, `TABLE` not `table`
- **FORBIDDEN**: Using `ADD COLUMN` for multiple columns - use `ADD COLUMN col1 ..., COLUMN col2 ...` syntax
- **FORBIDDEN**: Unquoted identifiers for external REST catalogs - ALL schema, table, and column names MUST be double-quoted
- **FORBIDDEN**: Setting multiple columns in schema registry updates - only set `IS_READY = TRUE`
- **FORBIDDEN**: Adding INSERT statements for processing logs or other metadata tables
- **FORBIDDEN**: Adding new columns for CSV fields that are semantic equivalents of existing columns during schema evolution
- **FORBIDDEN**: Ignoring the semantic mapping table when determining which fields to add during evolution

#### Perfect Response Example

```json
{
  "inferred_metadata": {
    "table_name": "string",
    "table_namespace": "string",
    "description": "string",
    "source_info": {},
    "field_summary": {}
  },
  "avro_schema": "escaped_json_string",
  "code": "sql_ddl_with_newlines",
  "schema_analysis": {
    "evolution_required": boolean,
    "existing_field_count": number,
    "new_field_count": number,
    "field_mappings": {
      "exact_matches": [],
      "semantic_matches": {},
      "new_fields": [],
      "type_changes": {}
    },
    "evolution_strategy": "string",
    "compatibility_notes": []
  }
}
```
