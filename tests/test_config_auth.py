from cftc_pipeline.config import Settings


def test_openai_auth_token_prefers_api_key():
    settings = Settings(openai_api_key="sk-api", codex_cli_auth_token="codex-token")
    assert settings.openai_auth_token() == "sk-api"


def test_openai_auth_token_uses_codex_token_when_api_key_missing():
    settings = Settings(openai_api_key="", codex_cli_auth_token="codex-token")
    assert settings.openai_auth_token() == "codex-token"
