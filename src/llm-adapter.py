from typing import Dict

class LLMAdapter:
    """
    Translates natural language queries into SQL using an LLM.
    Treats all LLM output as untrusted input and never executes SQL directly.
    """

    def __init__(self, api_key: str, model_name: str = "gpt-3.5-turbo"):
        """
        Initializes the LLM Adapter with the necessary credentials and model configuration.
        """
        self.api_key = api_key
        self.model_name = model_name

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
        pass

    def _parse_llm_response(self, response_text: str) -> Dict[str, str]:
        """
        Extracts the raw SQL query and the explanation from the LLM's response format.
        Strips out markdown formatting or conversational filler.
        """
        pass