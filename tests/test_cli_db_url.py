from cftc_pipeline import cli
from cftc_pipeline.config import settings


def test_is_likely_railway_private_host_true():
    assert cli._is_likely_railway_private_host(
        "postgresql://user:pass@postgres.railway.internal:5432/db"
    )


def test_is_likely_railway_private_host_false():
    assert not cli._is_likely_railway_private_host(
        "postgresql://user:pass@localhost:5432/db"
    )


def test_running_on_railway_detects_env(monkeypatch):
    monkeypatch.delenv("RAILWAY_ENVIRONMENT", raising=False)
    monkeypatch.delenv("RAILWAY_PROJECT_ID", raising=False)
    monkeypatch.delenv("RAILWAY_SERVICE_ID", raising=False)
    assert not cli._running_on_railway()

    monkeypatch.setenv("RAILWAY_SERVICE_ID", "svc_123")
    assert cli._running_on_railway()


def test_print_db_connection_help_for_railway_private_host(monkeypatch):
    monkeypatch.setattr(settings, "database_url", "postgresql://u:p@postgres.railway.internal:5432/db")
    monkeypatch.delenv("RAILWAY_ENVIRONMENT", raising=False)
    monkeypatch.delenv("RAILWAY_PROJECT_ID", raising=False)
    monkeypatch.delenv("RAILWAY_SERVICE_ID", raising=False)

    printed = []
    monkeypatch.setattr(cli.console, "print", lambda msg: printed.append(msg))

    cli._print_db_connection_help()

    assert any("railway.internal" in msg for msg in printed)
    assert any("public/external" in msg for msg in printed)


def test_print_db_connection_help_generic(monkeypatch):
    monkeypatch.setattr(settings, "database_url", "postgresql://u:p@localhost:5432/db")
    monkeypatch.delenv("RAILWAY_ENVIRONMENT", raising=False)
    monkeypatch.delenv("RAILWAY_PROJECT_ID", raising=False)
    monkeypatch.delenv("RAILWAY_SERVICE_ID", raising=False)

    printed = []
    monkeypatch.setattr(cli.console, "print", lambda msg: printed.append(msg))

    cli._print_db_connection_help()

    assert len(printed) == 1
    assert "Check that DATABASE_URL is set correctly" in printed[0]


def test_format_empty_export_guidance_when_pipeline_has_run():
    message = cli._format_empty_export_guidance("7654", pipeline_has_run=True)
    assert "status --docket 7654" in message
    assert "--stages crawl_docket --force" in message


def test_format_empty_export_guidance_when_pipeline_not_run():
    message = cli._format_empty_export_guidance("7654", pipeline_has_run=False)
    assert message.endswith("cftc run --docket 7654")
