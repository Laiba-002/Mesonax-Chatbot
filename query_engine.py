
"""
AI-Powered Query Engine using OpenAI API
"""

from openai import OpenAI
import json
import re
from typing import Dict, List, Any, Optional
from config import OPENAI_CONFIG
from schema import SCHEMA


class AIQueryEngine:
    def __init__(self):
        self.schema = SCHEMA
        # self.sample_queries = SAMPLE_QUERIES
        self.client = OpenAI(api_key=OPENAI_CONFIG['api_key'])
        self.model = OPENAI_CONFIG['model']
        self.temperature = OPENAI_CONFIG['temperature']
        self.max_tokens = OPENAI_CONFIG['max_tokens']
        self.conversation_history = []

    def _build_system_prompt(self, plant_code: Optional[str] = None) -> str:
        """Build comprehensive system prompt for AI"""

        # Node descriptions
        nodes_desc = "## NODES IN KNOWLEDGE GRAPH:\n\n"
        for node_name, node_config in self.schema['nodes'].items():
            nodes_desc += f"### {node_name}\n"
            # if 'description' in node_config:
            #     nodes_desc += f"- Description: {node_config['description']}\n"
            nodes_desc += f"- Primary Key: {node_config['primary_key']}\n"
            nodes_desc += f"- Properties: {', '.join(node_config['properties'])}\n"
            nodes_desc += f"- Hierarchy Level: {node_config['level']}\n\n"

        # Relationship descriptions
        rels_desc = "## RELATIONSHIPS IN KNOWLEDGE GRAPH:\n\n"
        for rel in self.schema['relationships']:
            rels_desc += f"### {rel['name']}\n"
            rels_desc += f"- From: {rel['from_node']} → To: {rel['to_node']}\n"
            if 'description' in rel:
                rels_desc += f"- Description: {rel['description']}\n"
            rels_desc += f"- Connection: {rel['from_node']}.{rel['from_property']} = {rel['to_node']}.{rel['to_property']}\n\n"

        # # Sample queries
        # samples_desc = "## SAMPLE QUERIES:\n\n"
        # for category, category_data in self.sample_queries.items():
        #     samples_desc += f"### {category.upper()}\n"
        #     if isinstance(category_data, dict):
        #         if 'description' in category_data:
        #             samples_desc += f"{category_data['description']}\n\n"
        #         if 'examples' in category_data:
        #             for example in category_data['examples']:
        #                 samples_desc += f"**Q: {example['question']}**\n"
        #                 samples_desc += f"```cypher\n{example['cypher']}\n```\n\n"
        #         else:
        #             # Handle case where category_data contains query templates directly
        #             for query_name, query_template in category_data.items():
        #                 if isinstance(query_template, str):
        #                     samples_desc += f"- {query_name}: {query_template[:100]}...\n"

        system_prompt = f"""You are an expert Cypher query generator for a Neo4j manufacturing knowledge graph database.

**IMPORTANT: Conversation Context Awareness**
You have access to full conversation history. Use this to:
- Understand follow-up questions and references like "that machine", "same line", "those products if any  otherwise extract values from current question"
- Resolve pronouns and implicit references from earlier queries
- Maintain continuity across related questions
- If a question lacks details but refers to previous context, extract values from history
The knowledge graph schema is as follows:
{nodes_desc}

{rels_desc}



## HIERARCHY STRUCTURE:
Group → Plant → Line → Machine → ProductionPlan → ProductionPlanDetail → POOperation → OperationConsumption → ProductionBatch

## IMPORTANT RULES:


1. **Return Format**: Always return a JSON object with this structure:
{{
    "cypher": "your cypher query here",
    "explanation": "brief explanation of what the query does",
    "parameters": {{"key": "value"}},
    "expected_results": "description of expected results"
}}

2. **Query Guidelines**:
   - Use MATCH for pattern matching
   - Use WHERE for filtering 
   - Use RETURN to specify what to return
   - Add LIMIT to prevent huge result sets (default 100)
   - Use meaningful variable names
   - Handle NULL values appropriately

3. **Common Patterns**:
   - Hierarchy: MATCH (parent)-[:RELATIONSHIP]->(child)
   - Filtering: WHERE node.property = 'value'
   - Aggregation: RETURN count(n), sum(n.property), etc.
   - Optional: OPTIONAL MATCH for nullable relationships

4. **Property Usage**:
   - For codes: {'nodeType'}Code (e.g., plantCode, machineCode)
   - For names: {'nodeType'}Name (e.g., plantName, machineName)
   - For status: status, isActive
   - For quantities: plannedQuantity, quantity, consumedQty
   - For time: startTime, endTime, planStartDate, planEndDate

5. **Error Prevention**:
   - Always validate that properties exist on nodes
   - Use property names exactly as defined in schema
   - Don't assume relationships that aren't defined
   - Handle case sensitivity properly

5.1. **CRITICAL: Date/DateTime Handling** (MANDATORY):
   Manufacturing data contains time-based fields that MUST be handled correctly:
   
   **Field Types:**
   - StartTime, EndTime: DATETIME strings (e.g., "2024-09-03T09:35:01")
   - StartDate, EndDate: DATE strings (e.g., "2024-09-03")
   - Duration: Numeric or ISO duration
   
   **Correct Usage:**
   ✅ For DATETIME fields: datetime(d.StartTime) >= datetime() - duration('P7D')
   ✅ For DATE fields: date(p.StartDate) >= date() - duration('P7D')
   ❌ NEVER use date() on DATETIME fields - causes "Text cannot be parsed to a Date" error
   
   
   **Time Period Patterns:**
   - Last 7 days: datetime(field) >= datetime() - duration('P7D')
   - Last 30 days: datetime(field) >= datetime() - duration('P30D')
   - This month: datetime(field) >= datetime(date.truncate('month'))
   - Today: date(datetime(field)) = date()
   - Yesterday: date(datetime(field)) = date() - duration('P1D')
   
   **Always identify if the field contains time (with T) before applying date/datetime functions.**

6. **Best Practices**:
   - Keep queries simple and readable
   - Use aliases that make sense (p for Plant, l for Line, etc.)
   - Add WHERE clauses to narrow results when appropriate
   - Always consider performance (use LIMIT)

7. **CRITICAL: Use ACTUAL VALUES, Not Parameters** (MUST FOLLOW):
   ❌ WRONG - Don't use parameters for filter values:
   ```
   WHERE toLower(p.PlantCode) = toLower($plant_code) AND toLower(l.LineName) = toLower($line_name)
   ```
   
   ✅ CORRECT - Embed actual values directly in the query:
   ```
   WHERE toLower(p.PlantCode) = toLower('PLANT001') AND toLower(l.LineName) = toLower('Assembly Line 1')
   ```
   
   **IMPORTANT:**
   - Extract actual values from the user's natural language question 
   - Embed these values DIRECTLY in the WHERE clauses as string literals
   - DO NOT use parameters like $plant_code, $line_name, $machine_name, etc.
   - The PlantCode will be provided to you - use it as a literal value
   - All other filter values (names, codes) must be extracted from the question and embedded directly
   - Never display any type of internal IDs, UserCode, OrderCode or technical identifiers etc in either tabular or natural language responses; only show user-friendly names and descriptive fields.
   - Always enrich the response by including relevant additional details (e.g., status, performance metrics, insights) even if not explicitly asked, especially when user ask about some specific entity.

8. **Query Pattern for Filtering** (MUST FOLLOW):
   ❌ WRONG - Don't do this:
   ```
   MATCH (p:Plant)-[:HAS_LINE]->(l:Line)-[:HAS_MACHINE]->(m:Machine)
   WHERE toLower(p.PlantCode) = toLower('PLANT001') AND toLower(l.LineName) = toLower('Line 1')
   ```
   
   ✅ CORRECT - Match nodes first, then traverse:
   ```
   MATCH (p:Plant)
   WHERE toLower(p.PlantCode) = toLower('PLANT001')
   WITH p
   MATCH (l:Line)
   WHERE toLower(l.LineName) = toLower('Line 1') AND l.PlantCode = p.PlantCode
   WITH l
   MATCH (l)-[:HAS_MACHINE]->(m:Machine)
   ```
   
   **Why this matters:**
   - Match nodes by their properties FIRST with actual values
   - Only traverse relationships AFTER nodes are identified
   - This prevents "property not defined" errors
   - Much more efficient and provides clearer error messages

9. **Node Matching Strategy**:
   - For nodes with known property values: Match the node first, then traverse relationships
   - For hierarchical queries: Match from specific to general using WITH clauses
   - Always validate property existence before comparison
   - Use OPTIONAL MATCH only when the relationship truly might not exist

Generate ONLY valid Cypher queries based on the schema provided. Do not make assumptions about data that doesn't exist in the schema.
"""

        # Add plant-specific filtering instruction if plant_code is provided
        if plant_code:
            system_prompt += f"""

## PLANT-SPECIFIC FILTERING (MANDATORY):
**Important:** All queries MUST be filtered for Plant Code: {plant_code}

- Always include WHERE clauses filtering to PlantCode = '{plant_code}' for all relevant nodes
- Use the ACTUAL plant code value '{plant_code}' directly in the query as a string literal
- DO NOT use $plant_code parameter - use the literal value '{plant_code}'
- When joining nodes, ensure the plant code filter is applied
- Never return data from other plants

Example: WHERE toLower(p.PlantCode) = toLower('{plant_code}')

SECURITY: Queries must respect plant code boundaries to ensure data isolation.
"""

        return system_prompt

    def generate_cypher(self, natural_query: str, context: Optional[Dict] = None, plant_code: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate Cypher query using OpenAI API

        Args:
            natural_query: User's question in natural language
            context: Optional context from previous queries
            plant_code: Optional plant code for filtering results to user's plant

        Returns:
            Dictionary with cypher query and metadata
        """

        print("\n🤖 AI is analyzing your question...")

        # Build messages
        messages = [
            {"role": "system",
                "content": self._build_system_prompt(plant_code)}
        ]

        # Add conversation history for context (full history)
        if self.conversation_history:
            messages.extend(self.conversation_history)

        # Add current query
        conversation_context = ""
        if self.conversation_history:
            conversation_context = "\n\n**CONVERSATION CONTEXT:** You have access to previous conversation history. Use it to understand follow-up questions, references to 'that machine', 'same line', 'those products', etc. If the user asks a follow-up question without specifying details, infer from previous queries."

        user_message = f"""Generate a Cypher query for this question: "{natural_query}"{conversation_context}

Remember to:
1. Use the exact property names from the schema
2. Return results in accurate and valid JSON format
3. Keep the query efficient with LIMIT clauses
4. Handle potential NULL values
5. CRITICAL: Extract actual values (names, codes, etc.) from the question and embed them DIRECTLY as string literals in WHERE clauses
6. DO NOT use parameters like $plant_code, $line_name - use actual values like 'PLANT001', 'Line 1'
7. For follow-up questions, use context from previous conversation to identify referenced entities if any followup questions otherwise extract values from the current question only."""

        user_message += f"""
Important for Neo4j compatible syntax:

 1. CRITICAL - DATE/DATETIME HANDLING (MANDATORY FOR ALL TIME-BASED QUERIES):
    
    **Know Your Time Fields:**
    - StartTime, EndTime: DATETIME format (e.g., "2024-09-03T09:35:01")
    - StartDate, EndDate: DATETIME format (e.g., "2024-09-03T09:35:01")
    - Duration: Usually numeric (minutes, seconds, or ISO duration)
    
    **RULES:**
    a) For DATETIME fields (StartTime, EndTime):
       ✅ CORRECT: datetime(d.StartTime) >= datetime() - duration('P7D')
       ✅ CORRECT: datetime(d.StartTime) >= datetime('2024-01-01T00:00:00')
       ❌ WRONG: date(d.StartTime) - will fail with "Text cannot be parsed to a Date"
    
    b) For DATE fields (StartDate, EndDate):
       ✅ CORRECT: date(datetime(p.StartDate)) >= date() - duration('P7D')
       ✅ CORRECT: date(datetime(p.StartDate)) = date('2024-09-03')
    
    c) For current time comparisons:
       - datetime() = current datetime
       - date(datetime()) = current date
         - Use date(datetime(field)) to  extract DATE from the current datetime field because date(field) on DATETIME causes errors
         -Use datetime function first and then apply Date function on top of it.
    d) For duration calculations:
       - duration('P7D') = 7 days
       - duration('P1M') = 1 month
       - duration('PT24H') = 24 hours
       - duration.between(datetime(start), datetime(end))
    
    **Common Time-Based Query Patterns:**
    
    Example 1 - Last 7 days downtime:
    ```
    WHERE datetime(d.StartTime) >= datetime() - duration('P7D')
    ```
    
    Example 2 - Between dates:
    ```
    WHERE datetime(d.StartTime) >= datetime('2024-01-01T00:00:00')
      AND datetime(d.StartTime) <= datetime('2024-12-31T23:59:59')
    ```
    
    Example 3 - Today's data:
    ```
    WHERE date(datetime(d.StartTime)) = date()
    ```
    
    Example 4 - Duration calculation:
    ```
    WITH duration.between(datetime(d.StartTime), datetime(d.EndTime)) AS timeDiff
    RETURN duration.inSeconds(timeDiff) AS DurationSeconds
    ```
 
 2. Check Neo4j Cypher query for syntax, variable scoping, WITH/MATCH/RETURN issues, and aggregation errors
 
 3. MANDATORY PlantCode Filter: Always filter by PlantCode = '{plant_code}' (use this EXACT value as a string literal, not as $plant_code parameter)
    Example: WHERE toLower(p.PlantCode) = toLower('{plant_code}')
 
 4. Assume string comparisons may be case-sensitive and automatically rewrite all string equality filters to be case-insensitive using toLower(property) = toLower('literal_value') like comparison of values: toLower(property)=toLower('value')
 
 5. CRITICAL - Extract Values from Question:
    - Look for line names, machine names, product names, etc. in the natural language question
    - Embed these values DIRECTLY in the query as string literals
    - Example: If question mentions "Line 1", use WHERE toLower(l.LineName) = toLower('Line 1')
    - DO NOT use parameters ($line_name, $machine_name, etc.) - use actual extracted values
 
 6. CRITICAL PATTERN - Match nodes FIRST by properties, then traverse relationships:
    - Match specific nodes using WHERE clauses with ACTUAL literal values
    - Use WITH to pass matched nodes to next step
    - Then traverse relationships from those identified nodes
    - Example: MATCH (p:Plant) WHERE toLower(p.PlantCode) = toLower('{plant_code}') WITH p MATCH (l:Line) WHERE toLower(l.LineName) = toLower('Assembly Line') AND l.PlantCode = p.PlantCode WITH l MATCH (l)-[:HAS_MACHINE]->(m:Machine)
    - This prevents "property not defined" errors that occur when pattern matching before filtering
 7. CRITICAL For Boolean Aggregations:
    - When counting based on boolean conditions, use CASE statements
    - NEVER apply aggregation functions (SUM, AVG, MIN, MAX) on BOOLEAN properties.
    - If a BOOLEAN must be aggregated, ALWAYS convert it using
      CASE WHEN <property> = true THEN 1 ELSE 0 END or toInteger(<property>).   
 Important Note:
    - Don't show index value or Codes or Ids in the final output or tabular response.
    - Don't display empty data table to user if query return no results, instead show message "No data found for the given criteria."
    - Use appropriate and meaningful aliases for returned fields to avoid exposing raw Ids or Codes or raw/vague field names.
Respond ONLY with a valid JSON object."""

        messages.append({"role": "user", "content": user_message})

        try:
            # Call OpenAI API
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=self.temperature,
                max_tokens=self.max_tokens
            )

            # Extract response
            ai_response = response.choices[0].message.content.strip()

            # Parse JSON response
            query_data = self._parse_ai_response(ai_response)

            # Update conversation history (keep full history)
            self.conversation_history.append(
                {"role": "user", "content": natural_query})
            self.conversation_history.append(
                {"role": "assistant", "content": ai_response})

            return {
                'success': True,
                'natural_query': natural_query,
                'cypher': query_data['cypher'],
                'explanation': query_data.get('explanation', ''),
                'parameters': query_data.get('parameters', {}),
                'expected_results': query_data.get('expected_results', ''),
                'raw_response': ai_response
            }

        except Exception as e:
            print(f"\n❌ AI Error: {e}")
            return {
                'success': False,
                'natural_query': natural_query,
                'error': str(e),
                'cypher': None
            }

    def _parse_ai_response(self, ai_response: str) -> Dict:
        """Parse AI response to extract query data"""

        # Try to extract JSON from markdown code blocks
        json_pattern = r'```json\s*(.*?)\s*```'
        json_match = re.search(json_pattern, ai_response, re.DOTALL)

        if json_match:
            json_str = json_match.group(1)
        else:
            # Try to find JSON object directly
            json_pattern = r'\{.*\}'
            json_match = re.search(json_pattern, ai_response, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
            else:
                json_str = ai_response

        try:
            query_data = json.loads(json_str)
            return query_data
        except json.JSONDecodeError:
            # Fallback: try to extract cypher query manually
            cypher_pattern = r'```cypher\s*(.*?)\s*```'
            cypher_match = re.search(cypher_pattern, ai_response, re.DOTALL)

            if cypher_match:
                return {
                    'cypher': cypher_match.group(1).strip(),
                    'explanation': 'Query extracted from response',
                    'parameters': {},
                    'expected_results': ''
                }
            else:
                raise ValueError(
                    "Could not parse AI response into valid query format")

    def validate_cypher(self, cypher: str) -> Dict[str, Any]:
        """
        Validate Cypher query syntax (basic validation)
        """
        validation = {
            'valid': True,
            'issues': [],
            'warnings': []
        }

        # Check for required keywords
        if 'MATCH' not in cypher.upper() and 'CREATE' not in cypher.upper():
            validation['valid'] = False
            validation['issues'].append(
                "Query must contain MATCH or CREATE clause")

        if 'RETURN' not in cypher.upper():
            validation['valid'] = False
            validation['issues'].append("Query must contain RETURN clause")

        # Check for potential issues
        if 'LIMIT' not in cypher.upper():
            validation['warnings'].append(
                "Consider adding LIMIT clause to prevent large result sets")

        # # Check for ID usage (should use codes)
        # if re.search(r'\b(PlantCode|MachineCode|GroupCode|lineId)\b', cypher, re.IGNORECASE):
        #     validation['issues'].append(
        #         "Use codes for (plantCode, machineCode,GroupCode) and Ids for others")
        #     validation['valid'] = False

        return validation

    def refine_query(self, original_query: str, feedback: str) -> Dict[str, Any]:
        """
        Refine a query based on user feedback or error messages
        """

        refinement_prompt = f"""The previous query needs refinement.

Original Question: {original_query}
Feedback/Error: {feedback}

Please generate an improved Cypher query that addresses the feedback.
Return in the same JSON format as before."""

        messages = [
            {"role": "system", "content": self._build_system_prompt()},
            {"role": "user", "content": refinement_prompt}
        ]

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=self.temperature,
                max_tokens=self.max_tokens
            )

            ai_response = response.choices[0].message.content.strip()
            query_data = self._parse_ai_response(ai_response)

            return {
                'success': True,
                'cypher': query_data['cypher'],
                'explanation': query_data.get('explanation', ''),
                'parameters': query_data.get('parameters', {}),
                'refined': True
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'refined': False
            }

    def _get_error_specific_guidance(self, error_message: str, failed_query: str) -> str:
        """
        Provide specific guidance based on error type to help AI avoid repeating mistakes
        """
        error_lower = error_message.lower()

        # Property does not exist error
        if "property" in error_lower and ("does not exist" in error_lower or "not defined" in error_lower):
            # Extract property name from error
            import re
            prop_match = re.search(
                r"property[\s'`]*([\w]+)[\s'`]*", error_message, re.IGNORECASE)
            wrong_prop = prop_match.group(1) if prop_match else "unknown"

            return f"""**ERROR TYPE: Property Name Error**

**Problem:** The property '{wrong_prop}' does not exist in the database schema.

**Solution Steps:**
1. Check the schema for the correct property name (case-sensitive)
2. Common mistakes:
   - Wrong case: 'lineName' should be 'LineName'
   - Wrong format: 'machine_name' should be 'MachineName'
   - Wrong property: 'name' should be 'MachineName' or 'LineName' etc.

**Example Fix:**
❌ WRONG: WHERE l.lineName = 'L001'
✅ CORRECT: WHERE l.LineName = 'L001'

**Action Required:** Replace '{wrong_prop}' with the correct property name from the schema."""

        # Variable not defined error
        elif "variable" in error_lower and "not defined" in error_lower:
            var_match = re.search(
                r"variable[\s'`]*([\w]+)[\s'`]*", error_message, re.IGNORECASE)
            wrong_var = var_match.group(1) if var_match else "unknown"

            return f"""**ERROR TYPE: Variable Scope Error**

**Problem:** Variable '{wrong_var}' is used but not defined in the query scope.

**Solution Steps:**
1. Ensure the variable is defined in a MATCH clause before use
2. Use WITH clause to pass variables between query parts
3. Check that the variable is available in the current scope

**Example Fix:**
❌ WRONG:
MATCH (p:Plant)-[:HAS_LINE]->(l:Line)
WHERE l.LineName = 'Assembly'
RETURN m.MachineName  // 'm' not defined!

✅ CORRECT:
MATCH (p:Plant)-[:HAS_LINE]->(l:Line)
WHERE l.LineName = 'Assembly'
WITH l
MATCH (l)-[:HAS_MACHINE]->(m:Machine)
RETURN m.MachineName

**Action Required:** Define '{wrong_var}' in a MATCH clause or use WITH to bring it into scope."""

        # Date/DateTime parsing error
        elif "cannot be parsed" in error_lower and "date" in error_lower:
            return """**ERROR TYPE: Date/DateTime Handling Error**

**Problem:** Using wrong date function for the field type.

**Solution Steps:**
1. DATETIME fields (with time like "2024-09-03T09:35:01"):
   ✅ Use: datetime(field)
   ❌ Don't use: date(field)

2. DATE fields (date only like "2024-09-03"):
   ✅ Use: date(datetime(field))
   ❌ Don't use: date(field) directly

3. Current time comparisons:
   - datetime() for current datetime
   - date(datetime()) for current date

**Example Fix:**
❌ WRONG: WHERE date(d.StartTime) >= date() - duration('P7D')
✅ CORRECT: WHERE datetime(d.StartTime) >= datetime() - duration('P7D')

**Action Required:** Replace date() with datetime() for DATETIME fields."""

        # Type mismatch error
        elif "type mismatch" in error_lower or "expected" in error_lower:
            return """**ERROR TYPE: Type Mismatch Error**

**Problem:** Using wrong data type in comparison or operation.

**Solution Steps:**
1. Convert strings to numbers: toInteger(), toFloat()
2. Convert numbers to strings: toString()
3. Handle boolean values: Use CASE WHEN or toInteger()

**Example Fix:**
❌ WRONG: WHERE m.status = true (if status is string)
✅ CORRECT: WHERE toLower(m.status) = 'active'

❌ WRONG: SUM(m.isActive) (boolean aggregation)
✅ CORRECT: SUM(CASE WHEN m.isActive = true THEN 1 ELSE 0 END)

**Action Required:** Add appropriate type conversion function."""

        # Syntax error
        elif "syntax" in error_lower or "invalid" in error_lower:
            return """**ERROR TYPE: Cypher Syntax Error**

**Problem:** Query has invalid Cypher syntax structure.

**Solution Steps:**
1. Check clause order: MATCH → WHERE → WITH/RETURN
2. Verify parentheses and brackets are balanced
3. Ensure relationship syntax: -[:REL_TYPE]->
4. Check property access: node.property

**Common Syntax Issues:**
- Missing RETURN clause
- Invalid relationship pattern
- Incorrect property access
- Missing or extra parentheses

**Action Required:** Restructure query with correct Cypher syntax."""

        # Generic guidance for unknown errors
        else:
            return f"""**ERROR TYPE: General Query Error**

**Problem:** {error_message}

**Solution Steps:**
1. Carefully read the error message above
2. Identify which part of the query caused the error
3. Apply the appropriate fix based on the error description
4. Ensure the query follows Neo4j Cypher syntax rules

**Common Fixes:**
- Check property names match schema exactly (case-sensitive)
- Verify all variables are defined before use
- Use correct date/datetime functions
- Apply proper type conversions
- Follow correct Cypher syntax structure

**Action Required:** Analyze the error message and generate a corrected query."""

    def generate_cypher_with_error_correction(self,
                                              natural_query: str,
                                              failed_cypher: str,
                                              error_message: str,
                                              attempt_number: int,
                                              plant_code: Optional[str] = None) -> Dict[str, Any]:
        """
        Regenerate Cypher query with error correction feedback loop.

        This method analyzes the execution error and uses it to improve the query.
        The error message and failed query are fed back to the AI along with the
        original user question to maintain context.

        Args:
            natural_query: Original user's question in natural language
            failed_cypher: The Cypher query that failed to execute
            error_message: The error message from query execution
            attempt_number: Current retry attempt number
            plant_code: Optional plant code for filtering

        Returns:
            Dictionary with corrected cypher query and metadata
        """

        print(
            f"\n🔄 Attempt {attempt_number}: Analyzing error and regenerating query...")

        # Analyze error type for specific guidance
        error_guidance = self._get_error_specific_guidance(
            error_message, failed_cypher)

        # Build error correction prompt with stronger emphasis on correction
        error_correction_prompt = f"""⚠️ QUERY EXECUTION FAILED - GENERATE CORRECTED QUERY ⚠️

**Original User Question:** "{natural_query}"

**Failed Cypher Query (Attempt {attempt_number - 1}):**
```cypher
{failed_cypher}
```

**Database Error:**
{error_message}

**CRITICAL: Your Previous Query FAILED. You MUST Generate a DIFFERENT Query!**

{error_guidance}

**Your Task - Generate a CORRECTED Query:**
1. ❌ DO NOT repeat the same query structure that failed
2. ✅ IDENTIFY the specific cause of the error from the message above
3. ✅ APPLY the appropriate fix from the guidance provided
4. ✅ VERIFY your corrected query addresses the error
5. ✅ MAINTAIN the user's original question intent

**Mandatory Requirements:**
- Your corrected query MUST be structurally different from the failed query
- Address the EXACT error mentioned in the database error message
- Use correct property names from schema (case-sensitive)
- Use correct function names (datetime(), date(), toLower(), etc.)
- Ensure all variables are in scope before use
- Apply plant code filter: PlantCode = '{plant_code}' if provided

**Verification Checklist Before Responding:**
□ Is my corrected query different from the failed query?
□ Does it fix the specific error mentioned?
□ Are all property names correct and from the schema?
□ Are all variables properly scoped?
□ Does it maintain the user's intent?

Respond ONLY with a valid JSON object containing the CORRECTED query.
"""

        # Build messages with system prompt and conversation history
        messages = [
            {"role": "system",
                "content": self._build_system_prompt(plant_code)}
        ]

        # Add conversation history for full context
        if self.conversation_history:
            messages.extend(self.conversation_history)

        # Add error correction request
        messages.append({"role": "user", "content": error_correction_prompt})

        try:
            # Call OpenAI API
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=self.temperature,
                max_tokens=self.max_tokens
            )

            # Extract response
            ai_response = response.choices[0].message.content.strip()

            # Parse JSON response
            query_data = self._parse_ai_response(ai_response)

            print(f"✅ Generated corrected query (Attempt {attempt_number})")

            return {
                'success': True,
                'natural_query': natural_query,
                'cypher': query_data['cypher'],
                'explanation': query_data.get('explanation', ''),
                'parameters': query_data.get('parameters', {}),
                'expected_results': query_data.get('expected_results', ''),
                'raw_response': ai_response,
                'error_corrected': True,
                'attempt_number': attempt_number,
                'previous_error': error_message
            }

        except Exception as e:
            print(
                f"\n❌ Error Correction Failed (Attempt {attempt_number}): {e}")
            return {
                'success': False,
                'natural_query': natural_query,
                'error': str(e),
                'cypher': None,
                'attempt_number': attempt_number
            }

    def explain_results(self, query: str, results: List[Dict]) -> str:
        """
        Generate natural language explanation of query results using AI
        """

        summary_prompt = f"""Based on this Cypher query and its results, provide a concise natural language summary.
        Important: 
          1. Focus on key insights and findings.
          2. Avoid technical jargon.
          3. Be concise and clear.
          4. If no results, state that clearly.
          5. Don't show any Ids or Codes in the summary.
          6. Don't start summary from this sentence "The query retrieved information..." instead make it natural.


Query: {query}

Number of results: {len(results)}

Sample results (first 3):
{json.dumps(results[:3], indent=2, default=str)}

Provide a brief, user-friendly summary of what was found."""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a helpful assistant that explains knowledge graph database query results in plain English."},
                    {"role": "user", "content": summary_prompt}
                ],
                temperature=0.1,
                max_tokens=500
            )

            return response.choices[0].message.content.strip()

        except Exception as e:
            return f"Found {len(results)} results. Unable to generate summary: {e}"

    def clear_history(self):
        """Clear conversation history"""
        self.conversation_history = []
        print("💬 Conversation history cleared")

    def get_conversation_count(self) -> int:
        """Get the number of exchanges in conversation history"""
        return len(self.conversation_history) // 2

    def get_conversation_summary(self) -> str:
        """Get a summary of the conversation history"""
        if not self.conversation_history:
            return "No conversation history"

        exchanges = len(self.conversation_history) // 2
        return f"Conversation history: {exchanges} exchanges (all available for context)"
