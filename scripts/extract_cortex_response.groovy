/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import groovy.json.JsonSlurper
import groovy.json.JsonBuilder
import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets

// Get the flowfile from the session
def flowFile = session.get()
if (!flowFile) return

// Read the flowfile content (Cortex response)
def inputStream = session.read(flowFile)
def jsonContent = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
inputStream.close()

try {
    // Parse the Cortex JSON response
    def jsonSlurper = new JsonSlurper()
    def cortexResponse = jsonSlurper.parseText(jsonContent)
    
    // Extract the messages content from Cortex structure
    def messagesContent = cortexResponse.choices[0].messages
    
    // Handle the case where messages might be a string or object
    def parsedMessages
    if (messagesContent instanceof String) {
        // If messages is a JSON string, parse it
        parsedMessages = jsonSlurper.parseText(messagesContent)
    } else {
        // If messages is already an object, use it directly
        parsedMessages = messagesContent
    }
    
    // Both are JSON strings, so parse them
    def inferredMetadata = parsedMessages.inferred_metadata
    def avroSchemaObject = jsonSlurper.parseText(parsedMessages.avro_schema)
    def codeJson = parsedMessages.code

    // Get table metadata
    // TODO:extract others as needed
    def tableName = inferredMetadata.table_name
    def tableNamespace = inferredMetadata.table_namespace
    def tableDescription = inferredMetadata.description

    //TODO: add this if needed
    def inferredMetadataJson = new JsonBuilder(inferredMetadata).toString()
    def avroSchemaJson = new JsonBuilder(avroSchemaObject).toString()
    def avroSchemaBase64 = Base64.getEncoder().encodeToString(avroSchemaJson.getBytes(StandardCharsets.UTF_8))

    // Set attributes
    flowFile = session.putAttribute(flowFile, 'avro.schema.content', avroSchemaBase64)
    flowFile = session.putAttribute(flowFile, 'code.content', codeJson)
    flowFile = session.putAttribute(flowFile, 'inferred.metadata', inferredMetadataJson)
    flowFile = session.putAttribute(flowFile, 'table.name', tableName)
    flowFile = session.putAttribute(flowFile, 'table.namespace', tableNamespace)
    flowFile = session.putAttribute(flowFile, 'table.description', tableDescription)
    flowFile = session.putAttribute(flowFile, 'extraction.status', 'success')
    
    // Optional: Log for debugging
    log.info("Successfully extracted Avro schema and code from Cortex response")
    
    session.transfer(flowFile, REL_SUCCESS)
    
} catch (Exception e) {
    // Log error and set failure attributes
    log.error("Failed to extract from Cortex response: " + e.getMessage())
    flowFile = session.putAttribute(flowFile, 'extraction.status', 'failed')
    flowFile = session.putAttribute(flowFile, 'extraction.error', e.getMessage())
    
    session.transfer(flowFile, REL_FAILURE)
}