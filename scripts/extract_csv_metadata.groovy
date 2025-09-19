// ExecuteScript processor (Groovy)
import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets

def flowFile = session.get()
if (!flowFile) return

try {
    def lines = []
    session.read(flowFile) { inputStream ->
        def content = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        lines = content.split('\n')
    }
    
    // Extract header
    def header = lines[0]
    
    // Extract sample rows (first 5 data rows)
    def sampleRows = []
    for (int i = 1; i <= Math.min(5, lines.length - 1); i++) {
        sampleRows.add(lines[i])
    }
    
    // Add attributes
    flowFile = session.putAttribute(flowFile, 'csv.headers', header)
    flowFile = session.putAttribute(flowFile, 'csv.sample.rows', sampleRows.join('\n'))
    
    session.transfer(flowFile, REL_SUCCESS)
    
} catch (Exception e) {
    log.error('Error processing CSV: {}', e.getMessage())
    session.transfer(flowFile, REL_FAILURE)
}