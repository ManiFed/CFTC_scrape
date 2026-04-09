from unittest.mock import patch, MagicMock

from cftc_pipeline.config import Settings


def test_openai_auth_token_prefers_api_key():
    settings = Settings(openai_api_key="sk-api", codex_cli_auth_token="codex-token")
    assert settings.openai_auth_token() == "sk-api"


def test_openai_auth_token_uses_codex_token_when_api_key_missing():
    settings = Settings(openai_api_key="", codex_cli_auth_token="codex-token")
    assert settings.openai_auth_token() == "codex-token"


def test_get_client_uses_auth_token_fallback():
    """get_client() must use openai_auth_token() so CODEX_CLI_AUTH_TOKEN is honoured."""
    import cftc_pipeline.analysis.llm_analyzer as mod

    fake_settings = Settings(
        openai_api_key="",
        openrouter_api_key="",
        codex_cli_auth_token="codex-token",
    )
    captured = {}

    def fake_openai(api_key=None, **kwargs):
        captured["api_key"] = api_key
        return MagicMock()

    orig_client = mod._client
    try:
        mod._client = None
        with patch.object(mod, "settings", fake_settings):
            with patch("cftc_pipeline.analysis.llm_analyzer.OpenAI", fake_openai):
                mod.get_client()
        assert captured["api_key"] == "codex-token"
    finally:
        mod._client = orig_client
