from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # KRX Open API - https://openapi.krx.co.kr
    krx_api_key: str

    # Open DART API - https://opendart.fss.or.kr
    dart_api_key: str

    # Bank of Korea ECOS API - https://ecos.bok.or.kr/api
    ecos_api_key: str

    # Server runtime (HTTP transport)
    server_host: str = "0.0.0.0"
    server_port: int = 8000


@lru_cache
def get_settings() -> Settings:
    return Settings()
