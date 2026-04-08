import importlib
import sys
import types
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

def _load_llm_adapter_class():
    fake_genai = types.SimpleNamespace()
    fake_genai.configure = MagicMock()
    fake_genai.GenerativeModel = MagicMock()

    google_module = sys.modules.setdefault("google", types.ModuleType("google"))
    sys.modules["google.generativeai"] = fake_genai
    setattr(google_module, "generativeai", fake_genai)

    try:
        module = importlib.import_module("src.llm_adapter")
    except ModuleNotFoundError:
        module = importlib.import_module("llm_adapter")
    return module.LLMAdapter

LLMAdapter = _load_llm_adapter_class()

def make_adapter():
    with patch("llm_adapter.genai.configure"), patch("llm_adapter.genai.GenerativeModel") as mock_model:
        adapter = LLMAdapter("fake-key", "gemini-test")
        return adapter, mock_model

def test_build_prompt_contains_schema_and_user_query():
    adapter, _ = make_adapter()
    prompt = adapter._build_prompt("show all users", "- users (id INTEGER, name TEXT)")
    assert "show all users" in prompt
    assert "users" in prompt
    assert "Only generate a read-only SELECT query" in prompt

def test_parse_llm_response_with_fenced_sql():
    adapter, _ = make_adapter()
    parsed = adapter._parse_llm_response(
        """```sql\nSELECT name FROM people;\n```\nExplanation: Lists names."""
    )
    assert parsed["sql"] == "SELECT name FROM people;"
    assert parsed["explanation"] == "Lists names."

def test_parse_llm_response_with_labeled_sections():
    adapter, _ = make_adapter()
    parsed = adapter._parse_llm_response(
        "SQL:\nSELECT age FROM people\n\nExplanation:\nReturns all ages."
    )
    assert parsed["sql"] == "SELECT age FROM people"
    assert parsed["explanation"] == "Returns all ages."

def test_generate_sql_successfully_calls_model_and_parses_response():
    with patch("llm_adapter.genai.configure"), patch("llm_adapter.genai.GenerativeModel") as mock_model_cls:
        mock_model = MagicMock()
        mock_model.generate_content.return_value = SimpleNamespace(
            text="SQL:\nSELECT name FROM people;\n\nExplanation:\nLists names."
        )
        mock_model_cls.return_value = mock_model

        adapter = LLMAdapter("fake-key", "gemini-test")
        result = adapter.generate_sql("show names", "- people (name TEXT)")

        assert result == {"sql": "SELECT name FROM people;", "explanation": "Lists names."}
        mock_model.generate_content.assert_called_once()

def test_generate_sql_returns_error_payload_when_model_call_fails():
    with patch("llm_adapter.genai.configure"), patch("llm_adapter.genai.GenerativeModel") as mock_model_cls:
        mock_model = MagicMock()
        mock_model.generate_content.side_effect = RuntimeError("boom")
        mock_model_cls.return_value = mock_model

        adapter = LLMAdapter("fake-key", "gemini-test")
        result = adapter.generate_sql("show names", "schema")

        assert result["sql"] == ""
        assert "Failed to connect" in result["explanation"]
