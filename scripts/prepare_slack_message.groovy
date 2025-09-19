import groovy.json.JsonBuilder
import groovy.json.JsonSlurper

def flowFile = session.get()
def jsonSlurper = new JsonSlurper()
try {
    // Get common attributes for both fallback and success messages
    def filename = flowFile.getAttribute('source.filename') ?: "N/A"
    def ingestionBucket = flowFile.getAttribute('ingestion.s3.bucket')
  
    def tableNamespace = flowFile.getAttribute('table.namespace') ?: 'N/A'
    def tableName = flowFile.getAttribute('table.name') ?: 'N/A'
    def schemaEvolutionRequired = flowFile.getAttribute('schema.evolution.required') ?: 'no'
    def isFirstTime = flowFile.getAttribute('is.first.time') ?: 'no'
    def newOrNeedsEvolution = (schemaEvolutionRequired == 'yes' || isFirstTime == 'yes') ? 'yes' : 'no'
    
    // Common ingestion status section for both messages
    def ingestionStatusSection = """
---

📊 *Processing Status Report:*

- *Ingestion Ready Status:* Successfully processed and moved to ingestion bucket

- *Destination s3 bucket:* `${ingestionBucket}`

${isFirstTime == 'yes' ? "- *Next Steps:* Please run the schema creation script: `${tableNamespace}_${tableName}_create.sql`" : 
  schemaEvolutionRequired == 'yes' ? "- *Next Steps:* Please run the schema evolution script: `${tableNamespace}_${tableName}_evolve.sql`" : 
  '✅ *Schema Status:* No evolution required - ready to use!'}

---
🎵 MusicFlow Pipeline | Schema Intelligence
"""
    
    // Parse schema analysis from FlowFile attribute
    def schemaAnalysisRaw = flowFile.getAttribute('schema.analysis')
    if (schemaAnalysisRaw == null) {
        log.warn("Missing 'schema.analysis' attribute, creating fallback message")
        
        // Create fallback message when schema analysis is not available
        def fallbackMessage = """🔄 *Schema Analysis Status*

*File:* `${filename}`
*Status:* ⚠️ Analysis data not available

📋 *Analysis Details:*

Schema analysis attribute was not found in the FlowFile. This may indicate:
- The analysis step was skipped
- An error occurred during schema analysis
- The FlowFile was processed without analysis

${ingestionStatusSection}"""

        flowFile = session.putAttribute(flowFile, "slack.message", fallbackMessage)
        session.transfer(flowFile, REL_SUCCESS)
        return
    }

    def schemaAnalysisDecoded = new String(schemaAnalysisRaw.decodeBase64())
    def schemaAnalysis = jsonSlurper.parseText(schemaAnalysisDecoded)

    // Build dynamic content for successful analysis
    def matchStatus = schemaAnalysis.matched == 'yes' ? '✅ Matched' : '❌ No Match'
    def strEvolutionRequired = schemaAnalysis.schema_evolution_required
    def evolutionRequired = strEvolutionRequired == 'yes' ? '⚠️ Yes' : '✅ No'
    def analysisDetails = (schemaAnalysis.schemas_analysis ?: [])
        .withIndex(1)
        .collect { item, index -> "${index}. ${item}" }
        .join('\n\n')
    
    // Create comprehensive Slack message combining both schema analysis and processing status
    def slackMessage = """🔄 *Schema Analysis and Generation Complete* 🎉

*File:* `${filename}`
*Status:* ${strEvolutionRequired == 'yes' ? '⚠️ Schema evolution required' : '✅ Schema is adaptable'}

*Match Status:* ${matchStatus}
*Evolution Required:* ${evolutionRequired}

📋 *Analysis Details:*

${analysisDetails}
${ingestionStatusSection}"""

    // Also write plain text message to attribute for direct use
    flowFile = session.putAttribute(flowFile, "slack.message", slackMessage)

    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    log.error("Failed to prepare Slack message: ${e.message}", e)
    flowFile = session.putAttribute(flowFile, "slack.message.error", e.message ?: e.toString())
    session.transfer(flowFile, REL_FAILURE)
}