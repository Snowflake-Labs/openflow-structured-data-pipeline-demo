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