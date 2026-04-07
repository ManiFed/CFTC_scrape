from cftc_pipeline import cli


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


def test_format_empty_export_guidance_when_pipeline_has_run():
    message = cli._format_empty_export_guidance("7654", pipeline_has_run=True)
    assert "status --docket 7654" in message
    assert "--stages crawl_docket --force" in message


def test_format_empty_export_guidance_when_pipeline_not_run():
    message = cli._format_empty_export_guidance("7654", pipeline_has_run=False)
    assert message.endswith("cftc run --docket 7654")
