from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # GCP
    gcp_project_id: str = "point-blank-ada"
    gcp_region: str = "northamerica-northeast1"
    bigquery_dataset: str = "cip"

    # Cloud SQL
    cloud_sql_connection_name: str = ""
    cloud_sql_database: str = "cip"
    cloud_sql_user: str = ""
    cloud_sql_password: str = ""

    # Firebase
    firebase_project_id: str = "point-blank-ada"

    # Google Sheets
    sheets_service_account_file: str = ""

    # Slack
    slack_bot_token: str = ""
    slack_default_channel: str = ""

    # App
    app_env: str = "development"
    log_level: str = "INFO"
    backend_url: str = "http://localhost:8000"
    frontend_url: str = "http://localhost:3000"

    # Engine version — git SHA injected by Cloud Build as ENGINE_VERSION env var.
    # Used to tag rows in fact_diagnostic_signals so Retrospective Mode can
    # differentiate outputs from different engine versions. Falls back to "dev"
    # in local development. Rows written before ADAC-51 shipped are tagged
    # "Pre-ADA" via a one-time backfill.
    engine_version: str = "dev"

    # CORS
    cors_origins: list[str] = ["http://localhost:3000"]

    # Alert charts — GCS bucket holding the spend PNGs embedded in pacing alerts
    # as Slack image blocks. Empty disables charts entirely: alerts still send,
    # just without images (graceful degradation).
    alert_charts_bucket: str = "point-blank-ada_resources"

    # Creative assets sync (Phase 19) — platform tokens for the creative
    # thumbnail + ad-set targeting sync. Both live in Secret Manager
    # (cip-meta-token / cip-stackadapt-key) and arrive as env vars via the
    # deploy's --update-secrets. Empty (the default) makes the corresponding
    # sync a no-op — local dev needs no platform access.
    meta_access_token: str = ""
    stackadapt_api_key: str = ""
    # Graph API version for the Meta calls above. Bump deliberately — Meta
    # retires versions roughly two years after release.
    meta_api_version: str = "v23.0"

    @property
    def is_production(self) -> bool:
        return self.app_env == "production"

    @property
    def bigquery_table(self) -> str:
        return f"{self.gcp_project_id}.{self.bigquery_dataset}"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
