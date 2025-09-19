import groovy.json.JsonSlurper
import java.nio.charset.StandardCharsets

// Get the flowfile
def flowFile = session.get()
if (!flowFile) return

try {
    // Read the JSON content from the flowfile
    def jsonContent = ''
    session.read(flowFile) { inputStream ->
        jsonContent = inputStream.getText(StandardCharsets.UTF_8.name())
    }
    
    // Parse the JSON array
    def jsonSlurper = new JsonSlurper()
    def results = jsonSlurper.parseText(jsonContent)
    
    // Check if results array is not empty
    if (results && results.size() > 0) {
        // Assuming we only need the first result
        matchedResult = results[0] 
        // Extract values with null safety
        def tableName = matchedResult.TABLE_NAME ?: ''
        def tableNamespace = matchedResult.TABLE_NAMESPACE ?: ''

        def schemaJson = matchedResult.SCHEMA_JSON ?: ''
        if (schemaJson) {
            def decodedSchemaJson = null
            try {
                def decodedBytes = Base64.getDecoder().decode(schemaJson)
                decodedSchemaJson = new String(decodedBytes, StandardCharsets.UTF_8)
            } catch (Exception e) {
                log.warn("Failed to decode schema JSON: ${e.getMessage()}")
                decodedSchemaJson = schemaJson // fallback to original if decode fails
            }
            flowFile = session.putAttribute(flowFile, 'existing.schema', decodedSchemaJson)
        }else{
            flowFile = session.putAttribute(flowFile, 'existing.schema', '')
        }

        // Base64 decode schemaAnalysis if not null and parse as JSON
        def schemaAnalysis = matchedResult.AI_RESPONSE ?: ''
        def analysisJsonObject = null
        if (schemaAnalysis) {
            try {
                // set it as base64 encoded string for database storage
                flowFile = session.putAttribute(flowFile, 'schema.analysis', schemaAnalysis)
                // encode it to base64 string to check if schema evolution is required
                def decodedBytes = Base64.getDecoder().decode(schemaAnalysis)
                def decodedString = new String(decodedBytes, StandardCharsets.UTF_8)
                analysisJsonObject = jsonSlurper.parseText(decodedString)
                schemaEvolutionRequired = analysisJsonObject?.schema_evolution_required ?: "no"
                flowFile = session.putAttribute(flowFile, 'schema.evolution.required', schemaEvolutionRequired)
            } catch (Exception e) {
                log.warn("Failed to decode/parse schema analysis: ${e.getMessage()}")
                flowFile = session.putAttribute(flowFile, 'schema.analysis', '')
            }
        }
        

        // Update flowfile attributes
        flowFile = session.putAttribute(flowFile, 'table.name', tableName)
        flowFile = session.putAttribute(flowFile, 'table.namespace', tableNamespace)
        
        flowFile = session.putAttribute(flowFile, 'is.first.time', 'no')
        
        // Log the extraction
        log.info("Successfully extracted matched result: table=${tableName}, namespace=${tableNamespace}")
        
        // Transfer to success
        session.transfer(flowFile, REL_SUCCESS)
       
    } else {
        // Handle empty results array
        flowFile = session.putAttribute(flowFile, 'table.name', '')
        flowFile = session.putAttribute(flowFile, 'table.namespace', '')
        flowFile = session.putAttribute(flowFile, 'existing.schema', '')
        flowFile = session.putAttribute(flowFile, 'is.first.time', 'yes')

        log.warn("No results found in SQL output array")
        session.transfer(flowFile, REL_SUCCESS)
    }
    
} catch (Exception e) {
    // Handle any parsing or processing errors
    flowFile = session.putAttribute(flowFile, 'sql.extraction.error', e.getMessage())
    flowFile = session.putAttribute(flowFile, 'sql.extraction.status', 'error')
    
    log.error("Error processing SQL results: ${e.getMessage()}", e)
    session.transfer(flowFile, REL_FAILURE)
}