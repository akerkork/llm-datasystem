import re
from typing import Dict
from google import genai

class LLMAdapter:
    """
    Translates natural language queries into SQL using an LLM.
    Treats all LLM output as untrusted input and never executes SQL directly.
    """

    def __init__(self, api_key: str, model_name: str = "gemini-1.5-flash"):
        """
        Initializes the LLM Adapter with the necessary credentials and model configuration.
        """
        self.api_key = api_key
        self.model_name = model_name
        self.client = genai.Client(api_key=self.api_key)

    def generate_sql(self, user_query: str, db_schema_context: str) -> Dict[str, str]:
        """
        Takes the user's natural language request and the database schema context,
        builds the prompt, and calls the LLM to generate the SQL.

        Returns a dictionary containing the generated SQL and the AI's explanation.
        """
        prompt = self._build_prompt(user_query, db_schema_context)

        try:
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=prompt)
            response_text = getattr(response, "text", "") or ""
            return self._parse_llm_response(response_text)
        except Exception as e:
            return {
                "sql": "",
                "explanation": f"Failed to connect to the LLM or generate content: {str(e)}",
            }

    def _build_prompt(self, user_query: str, schema_context: str) -> str:
        """
        Constructs the prompt engineering string.
        Instructs the AI to generate a SQL query compatible with SQLite 
        and provide a short comment explaining what the query does.
        """
        return f"""
        You are an assistant that converts natural language into safe SQLite SELECT queries.

        Database schema context:
        {schema_context}

        User request:
        {user_query}

        Rules:
        1. Generate exactly one SQL query that answers the user's request.
        2. The SQL must be valid SQLite syntax.
        3. Only generate a read-only SELECT query.
        4. Do not generate INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, PRAGMA, ATTACH, or other non-SELECT statements.
        5. Use only tables and columns present in the schema context.
        6. If the request is ambiguous, make the most reasonable read-only interpretation using the available schema.
        7. Do not add markdown commentary outside the required format.

        Return your answer in exactly this format:
        SQL:
        'single SQLite SELECT query'

        Explanation:
        'short explanation of what the query does'
        """.strip()

    def _parse_llm_response(self, response_text: str) -> Dict[str, str]:
        """
        Extracts the raw SQL query and the explanation from the LLM's response format.
        Strips out markdown formatting or conversational filler.
        """
        cleaned = (response_text or "").strip()
        result = {"sql": "", "explanation": ""}

        if not cleaned:
            result["explanation"] = "The LLM returned an empty response."
            return result

        # Prefer fenced SQL blocks if present.
        fenced_sql_match = re.search(r"```(?:sql)?\s*(.*?)```", cleaned, re.IGNORECASE | re.DOTALL)
        if fenced_sql_match:
            sql = fenced_sql_match.group(1).strip()
        else:
            # Prefer the declared SQL section.
            labeled_sql_match = re.search(
                r"SQL\s*:\s*(.*?)(?:\n\s*Explanation\s*:|$)",
                cleaned,
                re.IGNORECASE | re.DOTALL,
            )
            if labeled_sql_match:
                sql = labeled_sql_match.group(1).strip()
            else:
                # Fallback: first SELECT statement ending with a semicolon or end of text
                select_match = re.search(r"(SELECT\b.*?)(?:;\s*$|$)", cleaned, re.IGNORECASE | re.DOTALL)
                sql = select_match.group(1).strip() if select_match else ""

        # Remove leading labels and markdown fences if they slipped through
        sql = re.sub(r"^```(?:sql)?\s*|\s*```$", "", sql, flags=re.IGNORECASE | re.DOTALL).strip()
        sql = re.sub(r"^SQL\s*:\s*", "", sql, flags=re.IGNORECASE).strip()

        # Normalize to a single statement and strip trailing explanation if the model blended them
        if ";" in sql:
            sql = sql.split(";", 1)[0].strip() + ";"
        sql = re.split(r"\n\s*Explanation\s*:", sql, maxsplit=1, flags=re.IGNORECASE)[0].strip()

        # Extract explanation.
        explanation_match = re.search(r"Explanation\s*:\s*(.*)$", cleaned, re.IGNORECASE | re.DOTALL)
        explanation = explanation_match.group(1).strip() if explanation_match else ""

        # If explanation is missing, use the trailing non-SQL text when possible.
        if not explanation:
            if fenced_sql_match:
                explanation = cleaned[fenced_sql_match.end():].strip("\n -:")
            elif labeled_sql_match:
                remainder = cleaned[labeled_sql_match.end():].strip()
                explanation = re.sub(r"^Explanation\s*:\s*", "", remainder, flags=re.IGNORECASE).strip()

        # Final cleanup
        explanation = re.sub(r"^```|```$", "", explanation, flags=re.DOTALL).strip()
        explanation = re.sub(r"\s+", " ", explanation).strip()

        result["sql"] = sql
        result["explanation"] = explanation or "Generated a SQLite SELECT query based on the user request."
        return result