import re
from typing import Dict
import google.generativeai as genai

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

        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel(self.model_name)

    def generate_sql(self, user_query: str, db_schema_context: str) -> Dict[str, str]:
        """
        Takes the user's natural language request and the database schema context,
        builds the prompt, and calls the LLM to generate the SQL.
        
        Returns a dictionary containing the generated SQL and the AI's explanation.
        """
        pass

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
        pass