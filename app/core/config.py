from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict
import urllib.parse


class Settings(BaseSettings):
    # Environment file loading
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Core
    REDIS_URL: str = "redis://localhost:6379/0"
    DATA_ROOT: str = "/tmp/chunksmith-data"
    AUTH_TOKEN: str = "replace_with_real_secret_token"

    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    POSTGRES_HOST: str
    POSTGRES_PORT: int = 5432

    @computed_field
    @property
    def POSTGRES_URL(self) -> str:
        # Ensure the password is URL-encoded if it contains special characters
        password = urllib.parse.quote_plus(self.POSTGRES_PASSWORD)
        return f"postgresql://{self.POSTGRES_USER}:{password}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    # Worker
    REDIS_QUEUE_TIMEOUT_SECONDS: int = 1800  # 30 minutes default
    WEBHOOK_TIMEOUT_SECONDS: int = 5  # Default timeout for webhook POST
    MIN_CALLBACK_SECRET_BYTES: int = 32  # Minimum length for callback secrets

    # Limits
    MAX_FILE_SIZE_MB: int = 100
    MAX_PAGE_COUNT: int = 500
    CHUNK_DEFAULT_LIMIT: int = 50
    CHUNK_SIZE_BYTES: int = 2048
    CHUNK_OVERLAP_BYTES: int = 205

    # Logging
    LOG_LEVEL: str = "INFO"
    SERVICE_NAME: str = "chunksmith"


# Create a single instance for the application to import
settings = Settings()
