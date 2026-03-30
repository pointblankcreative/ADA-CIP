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

    # CORS
    cors_origins: list[str] = ["http://localhost:3000"]

    @property
    def is_production(self) -> bool:
        return self.app_env == "production"

    @property
    def bigquery_table(self) -> str:
        return f"{self.gcp_project_id}.{self.bigquery_dataset}"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
